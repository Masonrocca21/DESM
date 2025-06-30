package ThermalPowerPlants;

import java.util.*;

// Importazioni delle classi generate da Protobuf
import PlantServiceGRPC.PlantServiceImpl;
import powerplants.ElectionMessage;
import powerplants.ElectedMessage;
import powerplants.PlantInfoMessage;
import Arbiter.ArbiterServiceGrpc;
import Arbiter.ArbiterProto.WorkRequest;
import Arbiter.ArbiterProto.Empty;
import Arbiter.ArbiterProto.QueueStatus;


import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.JSONObject;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.*;

import io.grpc.Server;
import io.grpc.ServerBuilder;

// Importazioni necessarie da aggiungere in cima alla classe NetManager
import org.eclipse.paho.client.mqttv3.*;

/**
 * NetManager gestisce tutta la logica di rete e la sincronizzazione per una ThermalPowerPlant.
 * Implementa l'inserimento "a caldo" e l'algoritmo di elezione usando il protocollo gRPC fornito.
 */
public class NetManager {

    // --- CAMPI (VARIABILI DI ISTANZA) ---

    private final ThermalPowerPlant plant;
    private final GrpcClient grpcClient;

    private final Object peerStateLock;
    private final Object electionStateLock;

    private final Map<Integer, PeerInfo> networkPeers;
    private PeerInfo nextInRing;


    private boolean isElectionRunning = false;
    private String currentElectionId = null;

    private final String adminServerUrl = "http://localhost:8080";
    private Server grpcServer;
    private MqttClient energyRequestMqttClient;
    private final String MQTT_BROKER = "tcp://localhost:1883";
    private final String ENERGY_REQUEST_TOPIC = "home/renewableEnergyProvider/power/new";

    // Lock per la comunicazione.
    private final Object grpcServerStartupLock = new Object();
    // Una flag volatile per comunicare lo stato del server tra i thread.
    private volatile boolean isGrpcServerReady = false;

    private Thread watchdogThread;
    private volatile boolean isRunning = true; // Per fermare il thread
    private static final long WATCHDOG_INTERVAL_MS = 15000; // 15 secondi

    private final Map<String, ElectionInfo> activeElections = new HashMap<>();
    private final ManagedChannel arbiterChannel;
    private final ArbiterServiceGrpc.ArbiterServiceBlockingStub arbiterClient;

    // Una classe interna per contenere lo stato di una singola elezione
    private static class ElectionInfo {
        final String requestId;
        boolean isParticipant; // Sono un partecipante attivo o solo un passante?

        ElectionInfo(String requestId) {
            this.requestId = requestId;
            this.isParticipant = false; // Di default non partecipo
        }
    }


    public NetManager(ThermalPowerPlant plant) {
        this.plant = plant;
        this.grpcClient = new GrpcClient();

        // Inizializza i lock per la gestione della concorrenza
        this.peerStateLock = new Object();
        this.electionStateLock = new Object();

        // Inizializza le strutture dati
        this.networkPeers = new HashMap<>();

        // Inizializza timer
        this.watchdogThread = new Thread(this::watchdogLoop);
        this.watchdogThread.setName("WatchdogTimer-" + plant.getId());
        this.watchdogThread.setDaemon(true); // Si chiude con l'applicazione

        // Inizializza la connessione con l'Arbitro
        String arbiterAddress = "localhost:9090";
        this.arbiterChannel = ManagedChannelBuilder.forTarget(arbiterAddress)
                .usePlaintext()
                .build();
        this.arbiterClient = ArbiterServiceGrpc.newBlockingStub(arbiterChannel);
        System.out.println("NetManager for Plant " + plant.getId() + ": Connected to Arbiter at " + arbiterAddress);
    }


    /**
     * Avvia la logica di rete. Il primo passo è registrarsi con l'Administrator Server
     * per ottenere la lista dei peer e costruire la topologia dell'anello.
     */
    public void start() {
        System.out.println("NetManager for Plant " + plant.getId() + ": Starting network services...");

        // 1. Avvia i servizi locali (gRPC server) e attendi che siano pronti.
        startGrpcServer();


        synchronized (grpcServerStartupLock) {
            while (!isGrpcServerReady) {
                try {
                    System.out.println("Plant " + plant.getId() + ": Waiting for gRPC server to be ready...");
                    grpcServerStartupLock.wait();
                } catch (InterruptedException e) {
                    System.err.println("Plant " + plant.getId() + ": Startup wait was interrupted.");
                    Thread.currentThread().interrupt();
                    return; // Se veniamo interrotti, usciamo.
                }
            }
        }
        System.out.println("Plant " + plant.getId() + ": gRPC server is ready. Proceeding with registration.");

        // 2. Registrati con l'admin server per ottenere la lista completa dei peer.
        List<PeerInfo> allPeersAfterMyRegistration = registerWithAdminServer();
        if (allPeersAfterMyRegistration == null) {
            System.err.println("FATAL: Plant " + plant.getId() + " could not register. Shutting down network services.");
            return;
        }

        // 3.  Estrae la lista dei peer a cui annunciarsi (tutti tranne me stesso).
        List<PeerInfo> peersToNotify = new ArrayList<>();
        for (PeerInfo peer : allPeersAfterMyRegistration) {
            if (peer.getId() != plant.getId()) {
                peersToNotify.add(peer);
            }
        }

        if (!peersToNotify.isEmpty()) {
            System.out.println("Plant " + plant.getId() + ": Announcing my presence to " + peersToNotify.size() + " existing peers in parallel...");
            broadcastMyPresence(peersToNotify);
        }

        // 4. Dopo aver notificato gli altri, imposto la mia visione della rete.
        synchronized (peerStateLock) {
            networkPeers.clear();
            for (PeerInfo peer : allPeersAfterMyRegistration) {
                networkPeers.put(peer.getId(), peer);
            }

            startMqttListenerForEnergyRequests();

            // Il mio successore
            recalculateRingUnsafe();

            plant.setAsIdle();
            System.out.println("Plant " + plant.getId() + ": Startup complete. Making initial check for work.");

            // Avvia il thread guardiano in background
            this.watchdogThread.start();

        }
    }

    /**
     * Tenta di avviare un lavoro. Controlla lo stato locale, poi chiede
     * all'arbitro qual è il prossimo lavoro e se la coda è "aperta" per nuove elezioni.
     */
    private void tryToStartWork() {
        boolean canIStartLocally = false;
        synchronized (electionStateLock) {
            if (plant.isIdle() && !isElectionRunning) {
                canIStartLocally = true;
            }
        }

        if (!canIStartLocally) {
            return; // Sono occupato, non posso fare nulla.
        }

        new Thread(() -> {
            // --- CONTROLLO CON L'ARBITRO/SEMAFORO ---
            // 1. Chiedo qual è il prossimo lavoro
            WorkRequest workToDo = peekNextRequest();
            boolean isQueueOpen = arbiterClient.isQueueOpen(Empty.getDefaultInstance()).getIsOpen();

            if ((workToDo != null && !workToDo.getRequestId().isEmpty() && isQueueOpen)) {
                // C'è lavoro da fare E la coda è aperta!
                synchronized (electionStateLock) {
                    if (isElectionRunning) return; // Doppio controllo di sicurezza

                    isElectionRunning = true;
                    currentElectionId = workToDo.getRequestId();

                    System.out.println("LOG (Plant " + plant.getId() + "): Queue is OPEN. Starting election for " + currentElectionId);
                    plant.onEnergyRequest(workToDo.getRequestId(), workToDo.getKWh());
                }
            } else {
                if (workToDo != null) {
                    System.out.println("LOG (Plant " + plant.getId() + "): Found pending work, but queue is LOCKED. Waiting for a result to propagate.");
                }
            }
        }).start();
    }

    /**
     * (Privato) Invia un annuncio di presenza in parallelo a una lista di peer.
     */
    private void broadcastMyPresence(List<PeerInfo> targets) {
        PlantInfoMessage myInfoMessage = plant.getPeerInfo().toProtobuf();
        for (PeerInfo target : targets) {
            new Thread(() -> grpcClient.announcePresence(target, myInfoMessage)).start();
        }
    }

    /**
     * Contatta l'endpoint REST /add dell'admin server per registrare questa centrale.
     *
     * @return Una lista di tutti i PeerInfo nella rete (incluso se stesso) se la
     *         registrazione ha successo, altrimenti null.
     */
    private List<PeerInfo> registerWithAdminServer() {
        String registrationUrl = adminServerUrl + "/Administrator/add";
        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        // Crea il corpo della richiesta con le informazioni di questa centrale.
        HttpEntity<PeerInfo> request = new HttpEntity<>(plant.getPeerInfo(), headers);

        try {
            System.out.println("Plant " + plant.getId() + ": Attempting registration with Admin Server at " + registrationUrl);

            // Esegue la chiamata POST e si aspetta un array di PeerInfo come risposta.
            ResponseEntity<PeerInfo[]> response = restTemplate.postForEntity(registrationUrl, request, PeerInfo[].class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                System.out.println("Plant " + plant.getId() + ": Registration successful.");
                return Arrays.asList(response.getBody());
            } else {
                System.err.println("Plant " + plant.getId() + ": Registration failed. Server responded with status: " + response.getStatusCode());
                return null;
            }
        } catch (Exception e) {
            System.err.println("Plant " + plant.getId() + ": Could not connect to Admin Server at " + registrationUrl + ". Error: " + e.getMessage());
            return null;
        }
    }


    /**
     * Avvia un'elezione inviando il primo messaggio di elezione nell'anello.
     * Questa chiamata è bloccante: attende la conferma di rete prima di restituire il controllo.
     *
     * @param requestId L'ID della richiesta di energia.
     * @param kwh La quantità di energia.
     * @param myId L'ID di questa centrale, il primo candidato.
     * @param myPrice Il prezzo offerto da questa centrale.
     */
    public void startElection(String requestId, double kwh, int myId, double myPrice) {
        synchronized (electionStateLock) {
            if (!isElectionRunning || !requestId.equals(this.currentElectionId)) {
                System.err.println("Plant " + myId + ": Tried to start an election but state is inconsistent.");
                return;
            }
        }

        System.out.println("Plant " + myId + ": Starting election by sending the first message to " + this.nextInRing.getId() + ".");

        ElectionMessage firstMessage = ElectionMessage.newBuilder()
                .setCurrentElectionRequestId(requestId)
                .setRequiredKwh(kwh)
                .setCandidateId(myId)
                .setCandidateValue(myPrice)
                .build();

        grpcClient.sendElection(this.nextInRing, firstMessage, new RpcCallback() {
            @Override
            public void onCompleted() {
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("FATAL: Failed to send initial election message to " + nextInRing.getId() + ". Error: " + t.getMessage());
            }
        });
    }

    /**
     * Gestisce la richiesta di un nuovo nodo di unirsi alla rete.
     * Questo metodo agisce da coordinatore temporaneo per l'inserimento:
     *
     * @param newPlantProtobuf Il messaggio gRPC con le info del nuovo nodo.
     */
    public void handleAnnouncePresence(PlantInfoMessage newPlantProtobuf) {
        PeerInfo newPeer = PeerInfo.fromProtobuf(newPlantProtobuf);
        synchronized (peerStateLock) {
            if (networkPeers.containsKey(newPeer.getId())) return;
            System.out.println("\n--- NETWORK UPDATE (Plant " + plant.getId() + ") ---");
            System.out.println("Received presence of new peer " + newPeer.getId());
            networkPeers.put(newPeer.getId(), newPeer);
            recalculateRingUnsafe();
            System.out.println("Ring updated. My knowledge: " + networkPeers.size() + " peers. My new next is " + (nextInRing != null ? nextInRing.getId() : "N/A"));
        }
    }

    /**
     * Gestisce un messaggio di elezione in arrivo, implementando l'algoritmo di Chang e Roberts.
     * Un nodo IDLE che riceve questo messaggio si unisce attivamente all'elezione.
     * Un nodo BUSY inoltra passivamente il messaggio.
     *
     * @param message Il messaggio di elezione ricevuto.
     */
    public void handleElectionMessage(ElectionMessage message) {
        String requestId = message.getCurrentElectionRequestId();

        // CONTROLLO CON L'ARBITRO
        if (!isRequestInQueue(requestId)) {
            System.out.println("LOG (Plant " + plant.getId() + "): Ignoring ZOMBIE election message for " +
                    "completed request '" + requestId + "'.");
            return;
        }

        PeerInfo currentNextInRing;
        synchronized (peerStateLock) {
            currentNextInRing = this.nextInRing;
        }

        if (currentNextInRing == null) {
            System.err.println("ERROR (Plant " + plant.getId() + "): Cannot process election message, my next peer is null.");
            return;
        }

        synchronized (electionStateLock) {
            ElectionInfo election = activeElections.get(requestId);

            if (election == null) {
                election = new ElectionInfo(requestId);
                activeElections.put(requestId, election);
                System.out.println("LOG (Plant " + plant.getId() + "): Learned of valid election " + requestId + " via ring message.");
            }

            if (!plant.isProducing() && !election.isParticipant) {
                System.out.println("LOG (Plant " + plant.getId() + "): Joining/confirming active participation for " + requestId);
                election.isParticipant = true;

                // Chiamiamo onJoinElection solo se non siamo già "IN_ELECTION" per un'altra istanza.
                if (plant.getCurrentState() == ThermalPowerPlant.PlantState.IDLE) {
                    plant.onJoinElection(requestId, message.getRequiredKwh());
                }
            }

            if (!election.isParticipant) {
                System.out.println("LOG (Plant " + plant.getId() + ", BUSY): Passively forwarding for " + requestId + " to Plant " + currentNextInRing.getId());
                new Thread(() -> {
                    grpcClient.sendElection(currentNextInRing, message, new RpcCallback() {
                        @Override public void onCompleted() {}
                        @Override public void onError(Throwable t) {
                            System.err.println("FATAL (Passive Forward): Failed to forward election message to " +
                                    currentNextInRing.getId() + ". Error: " + t.getMessage());
                        }
                    });
                }).start();
                return;
            }

            // --- Logica di Chang & Roberts (Partecipanti Attivi) ---

            System.out.println("LOG (Plant " + plant.getId() + "): Actively processing election message for " + requestId);

            double myPrice = plant.getCurrentOfferPrice();
            int myId = plant.getId();
            ElectionMessage.Builder messageBuilder = message.toBuilder();
            boolean iAmTheNewCandidate = false;

            if (myPrice < message.getCandidateValue() ||
                    (myPrice == message.getCandidateValue() && myId > message.getCandidateId())) {
                System.out.println("  -> My offer (" + myPrice + ") is better. Updating candidate to myself (ID " + myId + ").");
                messageBuilder.setCandidateId(myId).setCandidateValue(myPrice).build();
                iAmTheNewCandidate = true;
            }

            if (message.getCandidateId() == myId && !iAmTheNewCandidate) {
                // --- GESTIONE DELLA VITTORIA ---

                // HO VINTO! Il messaggio ha fatto il giro e sono ancora il migliore.
                System.out.println("LOG (Plant " + myId + ", Winner): I have won the election for " + requestId + "!");

                // Tenta di acquisire il lock dall'Arbitro
                WorkRequest lockRequest = WorkRequest.newBuilder().setRequestId(requestId).build();
                boolean lockAcquired = arbiterClient.lockForFinalization(lockRequest).getAcquired();

                if (lockAcquired) {
                    System.out.println("LOG (Plant " + myId + ", Winner): Central Queue confirmed removal. Starting the result ring-pass.");

                    // 2. Costruisco il messaggio di risultato.
                    ElectedMessage resultMessage = ElectedMessage.newBuilder()
                            .setWinnerId(myId)
                            .setWinnerValue(myPrice)
                            .setCurrentElectionRequestId(requestId)
                            .setRequiredKwh(message.getRequiredKwh())
                            .build();

                    // 3. AVVIO ELECTED: invio il token solo al mio successore.
                    new Thread(() -> {
                        grpcClient.sendElected(currentNextInRing, resultMessage, new RpcCallback() {
                            @Override public void onCompleted() {
                                System.out.println("LOG (Plant " + myId + ", Winner): Result token for " + requestId + " successfully sent to " + currentNextInRing.getId());
                            }
                            @Override public void onError(Throwable t) {
                                System.err.println("FATAL (Result Ring Start): Failed to send result token to " +
                                        currentNextInRing.getId() + ". The result might not propagate. Error: " + t.getMessage());
                            }
                        });
                    }).start();

                    // 4. Aggiorno il mio stato a BUSY e resetto lo stato di questa elezione.
                    resetElectionStateFor(requestId);

                } else {
                    // Questo significa che un'altra elezione concorrente ha già vinto e confermato.
                    System.out.println("WARN (Plant " + myId + "): My win was for an already-confirmed request. Resetting state.");
                    resetElectionStateFor(requestId);
                }
            } else {
                // NON HO ANCORA VINTO, inoltro il messaggio (originale o aggiornato).
                System.out.println("LOG (Plant " + plant.getId() + "): Forwarding updated election message for " + requestId + " to Plant " + currentNextInRing.getId());
                ElectionMessage messageToForward = messageBuilder.build();
                iAmTheNewCandidate = false;

                new Thread(() -> {
                    grpcClient.sendElection(currentNextInRing, messageToForward, new RpcCallback() {
                        @Override public void onCompleted() {}
                        @Override public void onError(Throwable t) {
                            System.err.println("FATAL (Active Forward): Failed to forward election message to " +
                                    currentNextInRing.getId() + ". Error: " + t.getMessage());
                        }
                    });
                }).start();
            }
        }
    }

    /**
     * Gestisce un messaggio 'ElectedMessage' ricevuto in broadcast.
     * Questo metodo finalizza un'elezione e, se necessario, avvia il processo
     * per la richiesta successiva in coda.
     *
     * @param message Il messaggio ElectedMessage con i dati del vincitore.
     */
    public void handleElectedMessage(ElectedMessage message) {
        String completedRequestId = message.getCurrentElectionRequestId();
        int winnerId = message.getWinnerId();
        int myId = plant.getId();

        System.out.println("LOG (Plant " + myId + "): Received result token for '" + completedRequestId +
                "'. Winner is Plant " + winnerId + ".");

        // --- 1. Controllo se sono il destinatario finale del token ---
        // Se sono il vincitore, significa che il token ha completato il suo giro.
        if (myId == winnerId) {
            System.out.println("LOG (Plant " + myId + ", Winner): Result token has completed the ring. Notifying Arbiter to unlock and remove.");

            // Notifica all'arbitro di sbloccare e rimuovere la richiesta
            WorkRequest finalRequest = WorkRequest.newBuilder().setRequestId(completedRequestId).build();
            arbiterClient.unlockAndRemove(finalRequest);

            System.out.println("LOG (Plant " + myId + ", Winner): Arbiter confirmed. System is ready for a new election.");
            plant.onWinning(completedRequestId, message.getRequiredKwh());
            return;
        }

        // Caso 2: Sono un perdente.
        // Aggiorno il mio stato e inoltro il token.
        plant.onElectionResult(completedRequestId, message.getRequiredKwh()); // Mi assicuro di essere IDLE
        resetElectionStateFor(completedRequestId); // Pulisco lo stato locale per questa elezione

        // --- 3. Inoltro il token al prossimo nodo nell'anello ---
        PeerInfo currentNextInRing;
        synchronized (peerStateLock) {
            currentNextInRing = this.nextInRing;
        }

        if (currentNextInRing != null) {
            System.out.println("LOG (Plant " + myId + ", Loser): Forwarding result token to Plant " + currentNextInRing.getId());

            new Thread(() -> {
                grpcClient.sendElected(currentNextInRing, message, new RpcCallback() {
                    @Override public void onCompleted() {}
                    @Override public void onError(Throwable t) {
                        System.err.println("WARN (Result Ring): Failed to forward result token to " +
                                currentNextInRing.getId() + ". Error: " + t.getMessage());
                    }
                });
            }).start();
        } else {
            System.err.println("ERROR (Result Ring): Cannot forward result token, my next peer is null!");
        }

        System.out.println("LOG (Plant " + myId + ", Loser): My state: " + plant.getCurrentState());
    }

    /**
     * Metodo helper che viene chiamato quando un nodo diventa IDLE e vuole
     * proattivamente cercare nuovo lavoro.
     * Contatta la CentralWorkQueue per scoprire la prossima richiesta e, se presente,
     * tenta di avviare un'elezione.
     */
    private void checkAndStartNextWork() {
        new Thread(() -> {
            // Chiedo all'arbitro qual è il prossimo lavoro in coda.
            WorkRequest nextWork = peekNextRequest();

            if (nextWork != null) {
                // C'è del lavoro!
                System.out.println("LOG (Plant " + plant.getId() + "): Found pending work in queue: " + nextWork.getRequestId());
                tryToStartWork();
            } else {
                // La coda è vuota, non c'è nulla da fare per ora.
                System.out.println("LOG (Plant " + plant.getId() + "): Central queue is empty. Awaiting new MQTT notifications.");
            }
        }).start();
    }


    /**
     * Gestore per i messaggi MQTT ricevuti sul topic delle richieste di energia.
     * Questo metodo agisce come un "trigger" o una "sveglia".
     * Il suo unico scopo è notificare alla centrale che potrebbe esserci del nuovo
     * lavoro da fare, spingendola a controllare la CentralWorkQueue.
     * Non accoda nulla localmente e non prende decisioni.
     *
     * @param requestId L'ID della richiesta notificata via MQTT.
     * @param kwh La quantità di energia richiesta.
     */
    public void onMqttEnergyRequest(String requestId, double kwh) {
        System.out.println("LOG (Plant " + plant.getId() + "): Received MQTT notification for '" + requestId + "'. Forwarding to Arbiter.");

        try {
            // Costruisci il messaggio gRPC per l'Arbitro
            WorkRequest workRequest = WorkRequest.newBuilder()
                    .setRequestId(requestId)
                    .setKWh(kwh)
                    .build();

            // Chiama il servizio remoto
            arbiterClient.addRequest(workRequest);

            // Dopo aver aggiunto la richiesta, prova a vedere se si può iniziare a lavorare
            tryToStartWork();

        } catch (Exception e) {
            System.err.println("FATAL: Could not forward request to Arbiter. Error: " + e.getMessage());
        }
    }

    /**
     * (Privato, Unsafe) Ricalcola e imposta il puntatore `nextInRing` per questa centrale.
     * Si basa su una lista di peer ordinata per ID per garantire la determinazione.
     */
    private void recalculateRingUnsafe() {
        // Se non ci sono peer, non c'è anello.
        if (networkPeers.isEmpty()) {
            this.nextInRing = null;
            return;
        }

        // Se c'è un solo peer (me stesso), punto a me stesso.
        if (networkPeers.size() == 1) {
            this.nextInRing = plant.getPeerInfo();
            return;
        }

        // 1. Ottiene la lista di peer ordinata per ID.
        List<PeerInfo> sortedPeers = getSortedPeersUnsafe();

        // 2. Trova la mia posizione nella lista ordinata.
        int myIndex = -1;
        for (int i = 0; i < sortedPeers.size(); i++) {
            if (sortedPeers.get(i).getId() == plant.getId()) {
                myIndex = i;
                break;
            }
        }

        // 3. Calcola l'indice del mio successore.
        if (myIndex != -1) {
            int nextIndex = (myIndex + 1) % sortedPeers.size();
            this.nextInRing = sortedPeers.get(nextIndex);
        } else {
            this.nextInRing = null;
        }
    }


    /**
     * (Privato, Unsafe) Restituisce una lista di tutti i PeerInfo conosciuti,
     * ordinata in modo crescente per ID.
     *
     * @return Una nuova lista ordinata di PeerInfo.
     */
    private List<PeerInfo> getSortedPeersUnsafe() {
        // Crea una nuova lista a partire dai valori della mappa.
        List<PeerInfo> sortedPeers = new ArrayList<>(networkPeers.values());

        // Ordina la lista usando l'ID come chiave.
        sortedPeers.sort(Comparator.comparingInt(PeerInfo::getId));

        return sortedPeers;
    }

    /**
     * Inizializza e avvia il server gRPC in un thread dedicato.
     * Notifica al thread principale quando l'avvio è completato.
     */
    private void startGrpcServer() {
        // L'avvio del server, che è potenzialmente bloccante o lento,
        // viene messo in un suo thread.
        new Thread(() -> {
            try {
                PlantServiceImpl serviceImpl = new PlantServiceImpl(plant);

                grpcServer = ServerBuilder.forPort(plant.getPeerInfo().getPort())
                        .addService(serviceImpl)
                        .build();

                grpcServer.start();

                // Log di successo AVVENUTO
                System.out.println("LOG (Plant " + plant.getId() + ", startGrpcServer): Server start() method completed successfully. Now awaiting termination.");

                // Il server è partito con successo. Ora notifichiamo il thread principale.
                synchronized (grpcServerStartupLock) {
                    isGrpcServerReady = true;
                    // Risveglia il thread che è in attesa su questo lock.
                    grpcServerStartupLock.notify();
                }

                System.out.println("Plant " + plant.getId() + ": gRPC server started, listening on port " + plant.getPeerInfo().getPort());

                // Il server ora blocca questo thread per rimanere in ascolto.
                grpcServer.awaitTermination();

            } catch (Exception e) {
                System.err.println("FATAL: Failed to start gRPC server for Plant " + plant.getId() + ". Error: " + e.getMessage());
                e.printStackTrace();
                // Notifichiamo anche in caso di errore per non lasciare il thread principale in attesa per sempre.
                synchronized (grpcServerStartupLock) {
                    isGrpcServerReady = false; // O un'altra flag di errore
                    grpcServerStartupLock.notify();
                }
            }
        }).start();
    }

    /**
     * Inizializza e avvia il listener MQTT per le richieste di energia.
     * Si sottoscrive al topic del fornitore di energia rinnovabile e, alla ricezione
     * di un messaggio, avvia il processo di elezione.
     */
    private void startMqttListenerForEnergyRequests() {
        try {
            String clientId = "Plant-" + plant.getId() + "-EnergyListener";
            energyRequestMqttClient = new MqttClient(MQTT_BROKER, clientId, new MemoryPersistence());

            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            connOpts.setAutomaticReconnect(true);

            energyRequestMqttClient.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    System.err.println("Plant " + plant.getId() + ": MQTT connection for energy requests lost! " + cause.getMessage());
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) {
                    String payload = new String(message.getPayload());
                    System.out.println("Plant " + plant.getId() + ": Received energy request on topic '" + topic + "'. Payload: " + payload);

                    try {
                        // Parsa il payload JSON per estrarre i dati della richiesta
                        JSONObject requestJson = new JSONObject(payload);
                        String requestId = requestJson.getString("requestId");
                        double kwh = requestJson.getDouble("kWh");

                        if (Double.isNaN(kwh)) {
                            // Se optDouble fallisce, proviamo come stringa
                            kwh = Double.parseDouble(requestJson.getString("kWh"));
                        }

                        onMqttEnergyRequest(requestId, kwh);

                    } catch (Exception e) {
                        System.err.println("Plant " + plant.getId() + ": Failed to parse energy request payload. Error: " + e.getMessage());
                    }
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                }
            });

            System.out.println("Plant " + plant.getId() + ": Connecting to MQTT for energy requests...");
            energyRequestMqttClient.connect(connOpts);

            // Sottoscrivi al topic
            energyRequestMqttClient.subscribe(ENERGY_REQUEST_TOPIC, 1);
            System.out.println("Plant " + plant.getId() + ": Subscribed to energy request topic '" + ENERGY_REQUEST_TOPIC + "'");

        } catch (MqttException e) {
            System.err.println("FATAL: Failed to set up MQTT listener for energy requests for Plant " + plant.getId() + ". Error: " + e.getMessage());
            e.printStackTrace();
        }
    }


    // CHIAMATO DALLA TPP QUANDO TORNA IDLE
    public void onProductionComplete() {
        plant.setAsIdle();
        System.out.println("LOG (Plant " + plant.getId() + "): TPP became IDLE.");
    }


    // Aggiungi un metodo di shutdown per pulizia
    public void shutdown() {
        this.isRunning = false;
        if (this.watchdogThread != null) {
            this.watchdogThread.interrupt();
        }
        grpcClient.shutdown(); // Per le comunicazioni P2P
        if (arbiterChannel != null) {
            arbiterChannel.shutdown(); // Per la comunicazione con l'arbitro
        }
    }

    /**
     * Loop del thread "watchdog". Controlla periodicamente se la centrale è
     * bloccata in uno stato IDLE e, in caso affermativo, la spinge a
     * cercare nuovo lavoro per sbloccare potenziali deadlock logici.
     */
    private void watchdogLoop() {
        System.out.println("LOG (Plant " + plant.getId() + "): Watchdog timer started with " + WATCHDOG_INTERVAL_MS + "ms interval.");
        while (isRunning) {
            try {
                Thread.sleep(WATCHDOG_INTERVAL_MS);
                System.out.println("Timer in execution");

                boolean isStuckAndIdle = false;
                synchronized (electionStateLock) {
                    // La condizione di "stallo" è: sono libero e non sto facendo nulla.
                    if (plant.isIdle() && !isElectionRunning) {
                        isStuckAndIdle = true;
                    }
                }

                if (isStuckAndIdle) {
                    System.out.println("WATCHDOG (Plant " + plant.getId() + "): Detected potential stall. Proactively checking for work.");
                    // Chiama il metodo di rilancio per sbloccare la situazione.
                    checkAndStartNextWork();
                }

            } catch (InterruptedException e) {
                isRunning = false;
                Thread.currentThread().interrupt();
            }
        }
        System.out.println("LOG (Plant " + plant.getId() + "): Watchdog timer stopped.");
    }

    /**
     * Resetta lo stato relativo a una singola e specifica elezione.
     * Rimuove l'elezione dalla mappa delle elezioni attive.
     *
     * @param requestId L'ID della richiesta la cui elezione è terminata.
     */
    private void resetElectionStateFor(String requestId) {

        ElectionInfo removedElection = activeElections.remove(requestId);

        if (removedElection != null) {
            System.out.println("LOG (Plant " + plant.getId() + "): State for election '" + requestId + "' has been reset.");
        }

        if (activeElections.isEmpty()) {
            this.isElectionRunning = false;
            this.currentElectionId = null;
            System.out.println("LOG (Plant " + plant.getId() + "): No more active elections. Global state flags cleared.");
        } else {
            System.out.println("LOG (Plant " + plant.getId() + "): " + activeElections.size() + " other elections still active.");
        }
    }

    public boolean isRequestInQueue(String requestId) {
        WorkRequest request = WorkRequest.newBuilder()
                .setRequestId(requestId)
                .build();

        QueueStatus status = arbiterClient.checkRequestInQueue(request);
        return status.getIsOpen();
    }

    public WorkRequest peekNextRequest() {
        Empty empty = Empty.newBuilder().build();
        WorkRequest request = arbiterClient.peekNextRequest(empty);

        if (request.getRequestId().isEmpty()) {
            System.out.println("Attentione! Nessuna richiesta in coda.");
            return null;
        }

        System.out.println("Prossima richiesta: " + request.getRequestId() + ", kWh: " + request.getKWh());
        return request;
    }
}