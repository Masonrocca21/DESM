package ThermalPowerPlants;

import java.util.*;

// Importazioni delle classi generate da Protobuf
import PlantServiceGRPC.PlantServiceImpl;
import com.example.powerplants.BatonMessage;
import com.example.powerplants.ElectionMessage;
import com.example.powerplants.ElectedMessage;
import com.example.powerplants.PlantInfoMessage;
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
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.JSONObject; // Per parsare il payload della richiesta di energia

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
    private final Object ringChangeLock;

    // @GuardedBy("peerStateLock")
    private final Map<Integer, PeerInfo> networkPeers;
    // @GuardedBy("peerStateLock")
    private PeerInfo nextInRing;

    // @GuardedBy("electionStateLock")
    private boolean isElectionRunning = false;
    private boolean isThisNodeAParticipant = false;
    private String currentElectionId = null;

    private final String adminServerUrl = "http://localhost:8080";

    // @GuardedBy("ringChangeLock")
    private volatile boolean isHandoverPending = false;
    private PeerInfo pendingNextNode = null;

    // Campo per mantenere un riferimento al server gRPC, per poterlo chiudere
    private Server grpcServer;

    private MqttClient energyRequestMqttClient;
    private final String MQTT_BROKER = "tcp://localhost:1883";
    private final String ENERGY_REQUEST_TOPIC = "home/renewableEnergyProvider/power/new"; // O il topic corretto

    // Questo oggetto servirà sia come lock sia per la comunicazione wait/notify.
    private final Object grpcServerStartupLock = new Object();
    // Una flag volatile per comunicare lo stato del server tra i thread.
    private volatile boolean isGrpcServerReady = false;

    // La coda vera e propria che conterrà le richieste non processate.
    // Dichiarata come LinkedList per essere espliciti sul non usare j.u.c.
    // L'accesso deve essere SEMPRE protetto da un blocco synchronized.
    private final LinkedList<PendingRequest> requestQueue = new LinkedList<>();

    // Un lock dedicato per proteggere l'accesso concorrente alla coda.
    private final Object queueLock = new Object();

    private final Object queueProcessorLock = new Object();
    private volatile boolean isProcessorRunning = false;
    private Thread queueProcessorThread;
    private Thread pollingThread;
    private volatile boolean isRunning = true; // Per fermare il thread gentilmente

    private final Map<String, ElectionInfo> activeElections = new HashMap<>();
    private final Object electionsLock = new Object(); // Lock per la mappa
    private boolean isSearchingForStarter = false;

    private final CentralWorkQueue workQueue;

    // Una classe interna per contenere lo stato di una singola elezione
    private static class ElectionInfo {
        final String requestId;
        boolean isParticipant; // Sono un partecipante attivo o solo un passante?
        // Aggiungi altri campi se necessario, es. il mio miglior prezzo per questa elezione

        ElectionInfo(String requestId) {
            this.requestId = requestId;
            this.isParticipant = false; // Di default non partecipo
        }
    }



    // --- COSTRUTTORE ---
    public NetManager(ThermalPowerPlant plant) {
        this.plant = plant;
        this.grpcClient = new GrpcClient();
        // Inizializza i lock per la gestione della concorrenza
        this.peerStateLock = new Object();
        this.electionStateLock = new Object();
        this.ringChangeLock = new Object();

        // Inizializza le strutture dati
        this.networkPeers = new HashMap<>();

        this.queueProcessorThread = new Thread(this::processQueueLoop);
        this.queueProcessorThread.setName("QueueProcessor-Plant-" + plant.getId());
        this.queueProcessorThread.setDaemon(true); // Così si chiude con l'app


        this.workQueue = CentralWorkQueue.getInstance();
    }

    // --- METODI PUBBLICI PRINCIPALI ---

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
                    // Rilascia il lock e attende che un altro thread chiami notify() su questo oggetto.
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

        // 2. ANNUNCIO ATTIVO
        // Estrae la lista dei peer a cui annunciarsi (tutti tranne me stesso).
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

        // 3. AGGIORNAMENTO DELLA PROPRIA CONOSCENZA
        // Dopo aver notificato gli altri, imposto la mia visione della rete.
        synchronized (peerStateLock) {
            networkPeers.clear();
            for (PeerInfo peer : allPeersAfterMyRegistration) {
                networkPeers.put(peer.getId(), peer);
            }

            // Riattiviamo il listener MQTT su ogni centrale!
            startMqttListenerForEnergyRequests();

            recalculateRingUnsafe();

            plant.setAsIdle();

            // All'avvio, la centrale è IDLE, quindi prova subito a vedere se c'è lavoro
            System.out.println("Plant " + plant.getId() + ": Startup complete. Making initial check for work.");

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
            PendingRequest workToDo = workQueue.peekNextRequest();

            // 2. Chiedo se la coda è aperta per nuove elezioni
            boolean isQueueOpen = workQueue.isQueueOpenForElections();

            if (workToDo != null && isQueueOpen) {
                // C'è lavoro da fare E la coda è aperta!
                synchronized (electionStateLock) {
                    if (isElectionRunning) return; // Doppio controllo di sicurezza

                    isElectionRunning = true;
                    currentElectionId = workToDo.getRequestId();

                    System.out.println("LOG (Plant " + plant.getId() + "): Queue is OPEN. Starting election for " + currentElectionId);
                    plant.onEnergyRequest(workToDo.getRequestId(), workToDo.getKwh());
                    // La logica di invio del primo ElectionMessage è in plant.onEnergyRequest -> netManager.startElection
                }
            } else {
                if (workToDo != null) {
                    System.out.println("LOG (Plant " + plant.getId() + "): Found pending work, but queue is LOCKED. Waiting for a result to propagate.");
                }
            }
        }).start();
    }

    private void processQueueLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                PendingRequest requestToProcess = null;
                synchronized (queueLock) {
                    if (!requestQueue.isEmpty()) {
                        requestToProcess = requestQueue.peekFirst();
                    }
                }

                // Se c'è una richiesta e non c'è già un'elezione in corso...
                if (requestToProcess != null) {
                    boolean shouldIAct = false;
                    synchronized(electionStateLock) {
                        if (!isElectionRunning) {
                            shouldIAct = true;
                        }
                    }

                    if (shouldIAct) {
                        // C'è del lavoro da fare!
                        handleRequest(requestToProcess);
                    }
                }

                // Attende prima del prossimo ciclo per non consumare CPU
                // o finché non viene notificato.
                synchronized(queueProcessorLock) {
                    queueProcessorLock.wait(500); // Attende 500ms o una notifica
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.err.println("FATAL ERROR in Queue Processor Thread: " + e.getMessage());
                e.printStackTrace();
            }
        }
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
            // Gestisce errori di connessione o altri problemi del client HTTP.
            System.err.println("Plant " + plant.getId() + ": Could not connect to Admin Server at " + registrationUrl + ". Error: " + e.getMessage());
            return null;
        }
    }

    // --- GESTORI DI EVENTI GRPC IN INGRESSO ---

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
            this.isThisNodeAParticipant = true;
        }

        System.out.println("Plant " + myId + ": Starting election by sending the first message to " + this.nextInRing.getId() + ".");

        ElectionMessage firstMessage = ElectionMessage.newBuilder()
                .setCurrentElectionRequestId(requestId)
                .setRequiredKwh(kwh)
                .setCandidateId(myId)
                .setCandidateValue(myPrice)
                .build();

        // Chiamata "lancia e dimentica". Non attendiamo la risposta.
        grpcClient.sendElection(this.nextInRing, firstMessage, new RpcCallback() {
            @Override
            public void onCompleted() {
                // Possiamo loggare il successo se vogliamo, ma non è necessario.
                // System.out.println("DEBUG: Initial election message sent successfully to " + nextInRing.getId());
            }

            @Override
            public void onError(Throwable t) {
                // Qui dovremmo gestire il fallimento. Per esempio, se il successore è morto,
                // potremmo tentare di inviare al successore del successore, o resettare l'elezione.
                System.err.println("FATAL: Failed to send initial election message to " + nextInRing.getId() + ". Error: " + t.getMessage());
                // Una gestione di errore robusta potrebbe resettare lo stato qui.
                // resetElectionState();
                // triggerProcessing(); // E provare a far ripartire la catena.
            }
        });
    }

    /**
     * Gestisce la richiesta di un nuovo nodo di unirsi alla rete.
     * Questo metodo agisce da coordinatore temporaneo per l'inserimento:
     * 1. Identifica il predecessore (P) e il successore (A) del nuovo nodo.
     * 2. Notifica a tutti i nodi l'esistenza del nuovo peer.
     * 3. Avvia il processo di handover controllato ordinando a (P) di iniziare.
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

        // CONTROLLO CON L'ARBITRO (Questo rimane, è fondamentale)
        if (!workQueue.isRequestStillPending(requestId)) {
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
                // Questo evita di ricalcolare il prezzo.
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
                messageBuilder.setCandidateId(myId).setCandidateValue(myPrice);
                iAmTheNewCandidate = true;
            }

            if (message.getCandidateId() == myId && !iAmTheNewCandidate) {
                // --- MODIFICA FONDAMENTALE: GESTIONE DELLA VITTORIA ---

                // HO VINTO! Il messaggio ha fatto il giro e sono ancora il migliore.
                System.out.println("LOG (Plant " + myId + ", Winner): I have won the election for " + requestId + "!");

                boolean lockAcquired = workQueue.lockForFinalization(requestId);
                if (lockAcquired) {
                    System.out.println("LOG (Plant " + myId + ", Winner): Central Queue confirmed removal. Starting the result ring-pass.");

                    // 2. Costruisco il messaggio di risultato.
                    ElectedMessage resultMessage = ElectedMessage.newBuilder()
                            .setWinnerId(myId)
                            .setWinnerValue(myPrice)
                            .setCurrentElectionRequestId(requestId)
                            .setRequiredKwh(message.getRequiredKwh())
                            .build();

                    // 3. AVVIO L'ANELLO DEL RISULTATO: invio il token solo al mio successore.
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
                    plant.onElectionResult(requestId, myId);
                    resetElectionStateFor(requestId);

                } else {
                    // Questo significa che un'altra elezione concorrente ha già vinto e confermato.
                    // La mia vittoria è "obsoleta". Resetto e basta.
                    System.out.println("WARN (Plant " + myId + "): My win was for an already-confirmed request. Resetting state.");
                    resetElectionStateFor(requestId);
                }
            } else {
                // NON HO ANCORA VINTO, inoltro il messaggio (originale o aggiornato).
                System.out.println("LOG (Plant " + plant.getId() + "): Forwarding updated election message for " + requestId + " to Plant " + currentNextInRing.getId());
                ElectionMessage messageToForward = messageBuilder.build();

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
        // Il mio unico compito è "consumarlo" e fermare la catena.
        if (myId == winnerId) {
            System.out.println("LOG (Plant " + myId + ", Winner): Result token has completed the ring tour. Consuming token.");
            // Notifico all'arbitro che la propagazione è finita.
            // Questo rimuoverà la richiesta dalla coda e riaprirà il semaforo.
            workQueue.unlockQueue(completedRequestId);
            System.out.println("LOG (Plant " + myId + ", Winner): Notified queue to unlock. System is ready for a new election.");
            return;
        }

        // Caso 2: Sono un perdente.
        // Aggiorno il mio stato e inoltro il token.
        plant.onElectionResult(completedRequestId, winnerId); // Mi assicuro di essere IDLE
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

        // --- 4. Azione del Perdente: Cerco nuovo lavoro ---
        // Ora che ho processato il risultato e inoltrato il token,
        // sono ufficialmente libero e so che un'elezione è finita.
        // È il momento perfetto per cercare la prossima richiesta in coda.
        System.out.println("LOG (Plant " + myId + ", Loser)");
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
            PendingRequest nextWork = workQueue.peekNextRequest();

            if (nextWork != null) {
                // C'è del lavoro! Passo i dettagli al metodo che tenta di avviarlo.
                System.out.println("LOG (Plant " + plant.getId() + "): Found pending work in queue: " + nextWork.getRequestId());
                tryToStartWork();
            } else {
                // La coda è vuota, non c'è nulla da fare per ora.
                System.out.println("LOG (Plant " + plant.getId() + "): Central queue is empty. Awaiting new MQTT notifications.");
            }
        }).start();
    }

    /**
     * Invia in broadcast un messaggio ElectedMessage a tutti i peer conosciuti nella rete.
     * @param resultMessage Il messaggio con i dati del vincitore da inviare.
     */
    private void broadcastElectedMessage(ElectedMessage resultMessage) {
        List<PeerInfo> allPeers;
        synchronized(peerStateLock) {
            allPeers = new ArrayList<>(networkPeers.values());
        }

        System.out.println("LOG (Plant " + plant.getId() + ", Winner): Broadcasting election result to " + allPeers.size() + " peers.");

        for (PeerInfo peer : allPeers) {
            // Esegui ogni invio in un thread separato per non bloccare
            final PeerInfo target = peer;
            new Thread(() -> {
                System.out.println("  -> Broadcasting result to Plant " + target.getId());
                grpcClient.sendElected(target, resultMessage, new RpcCallback() {
                    @Override
                    public void onCompleted() {
                        // Log di debug opzionale
                    }

                    @Override
                    public void onError(Throwable t) {
                        // La notifica del risultato non è critica come l'elezione stessa.
                        // Se un nodo non la riceve, potrebbe avere uno stato leggermente obsoleto,
                        // ma il sistema si riprenderà. Logghiamo come warning.
                        System.err.println("WARN: Failed to send election result to Plant " + target.getId() + ". Error: " + t.getMessage());
                    }
                });
            }).start();
        }
    }


    // --- GESTORE DI EVENTI MQTT ---
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
        System.out.println("LOG (Plant " + plant.getId() + "): Received MQTT notification for '" + requestId + "'.");

        // Delego tutta la logica a tryToStartWork, passandogli i dati.
        tryToStartWork();
    }


    /**
     * (Gestore gRPC) Chiamato quando si riceve il testimone dal predecessore.
     */
    public void handlePassBaton(BatonMessage request) {
        System.out.println("LOG (Plant " + plant.getId() + "): Received election baton for request " + request.getRequestId());
        handleBaton(request);
    }

    /**
     * Riceve il testimone e decide se avviare l'elezione (se IDLE)
     * o passare il testimone al prossimo candidato valido (se BUSY).
     */
    public void handleBaton(BatonMessage request) {
        synchronized(electionStateLock) {
            // Controllo di sicurezza: se non siamo in un circuito di ricerca,
            // o se il testimone è per una richiesta diversa, è un messaggio obsoleto.
            if (!isSearchingForStarter || !request.getRequestId().equals(currentElectionId)) {
                System.out.println("WARN (Plant " + plant.getId() + "): Ignoring obsolete or unexpected BatonMessage for " + request.getRequestId());
                return;
            }

            if (plant.isIdle()) {
                // SONO IDLE, DIVENTO LO STARTER.
                isSearchingForStarter = false; // Spengo il circuito di ricerca
                isElectionRunning = true;
                currentElectionId = request.getRequestId();

                synchronized(queueLock) {
                    requestQueue.removeIf(req -> req.getRequestId().equals(request.getRequestId()));
                }

                System.out.println("LOG (Plant " + plant.getId() + "): I am IDLE. Accepting baton, will start election for " + request.getRequestId() + " in a new context.");

                // --- MODIFICA CRUCIALE ---
                // Avviamo l'elezione in un nuovo thread per disaccoppiarla dal contesto
                // della chiamata gRPC "passElectionBaton" che ha attivato questo metodo.
                new Thread(() -> {
                    plant.onEnergyRequest(request.getRequestId(), request.getKwh());
                }).start();

            } else {
                // SONO BUSY, PASSO IL TESTIMONE.
                System.out.println("LOG (Plant " + plant.getId() + "): I am BUSY, passing baton for " + request.getRequestId());

                List<Integer> visitedIds = new ArrayList<>(request.getVisitedNodesIdsList());
                visitedIds.add(plant.getId());

                PeerInfo nextCandidate = findNextCandidate(visitedIds);

                if (nextCandidate != null) {
                    BatonMessage nextBaton = request.toBuilder()
                            .clearVisitedNodesIds()
                            .addAllVisitedNodesIds(visitedIds)
                            .build();
                    grpcClient.passElectionBaton(nextCandidate, nextBaton);
                } else {
                    System.out.println("WARN (Plant " + plant.getId() + "): Baton tour for " + request.getRequestId() + " completed but no IDLE node was found.");
                    // In questo caso la richiesta rimane in coda. Il prossimo coordinatore riproverà.
                }
            }
        }
    }

    // Metodo helper per la logica di ricerca
    private PeerInfo findNextCandidate(List<Integer> visitedIds) {
        PeerInfo nextCandidate = null;
        synchronized(peerStateLock) {
            int lowestIdFound = Integer.MAX_VALUE;
            for(PeerInfo peer : networkPeers.values()) {
                if (peer.getId() < lowestIdFound && !visitedIds.contains(peer.getId())) {
                    lowestIdFound = peer.getId();
                    nextCandidate = peer;
                }
            }
        }
        return nextCandidate;
    }

    // Aggiungi un metodo helper per resettare lo stato
    private void resetElectionState() {
        synchronized(electionStateLock) {
            isElectionRunning = false;
            currentElectionId = null;
            isThisNodeAParticipant = false;
            System.out.println("LOG (Plant " + plant.getId() + "): Election state has been reset.");
        }
    }

    // --- METODI PRIVATI ---


    /**
     * (Privato, Unsafe) Ricalcola e imposta il puntatore `nextInRing` per questa centrale.
     * Si basa su una lista di peer ordinata per ID per garantire la determinazione.
     * DEVE essere chiamato all'interno di un blocco `synchronized(peerStateLock)`.
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
        // L'operatore modulo (%) gestisce il "wrap-around" per l'ultimo nodo.
        if (myIndex != -1) {
            int nextIndex = (myIndex + 1) % sortedPeers.size();
            this.nextInRing = sortedPeers.get(nextIndex);
        } else {
            // Questo non dovrebbe accadere se la centrale è nella sua stessa mappa.
            this.nextInRing = null;
        }
    }


    /**
     * (Privato, Unsafe) Restituisce una lista di tutti i PeerInfo conosciuti,
     * ordinata in modo crescente per ID.
     *
     * "Unsafe" significa che questo metodo DEVE essere chiamato solo da un
     * blocco di codice che già detiene il `peerStateLock` per evitare
     * race condition sulla mappa `networkPeers`.
     *
     * @return Una nuova lista ordinata di PeerInfo.
     */
    private List<PeerInfo> getSortedPeersUnsafe() {
        // Crea una nuova lista a partire dai valori della mappa.
        List<PeerInfo> sortedPeers = new ArrayList<>(networkPeers.values());

        // Ordina la lista usando l'ID come chiave.
        // Usare Comparator.comparingInt è un modo moderno ed efficiente per farlo.
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

                // --- PUNTO CHIAVE DELLA NOTIFICA ---
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
            connOpts.setAutomaticReconnect(true); // Utile per la robustezza

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
            energyRequestMqttClient.subscribe(ENERGY_REQUEST_TOPIC, 1); // QoS 1
            System.out.println("Plant " + plant.getId() + ": Subscribed to energy request topic '" + ENERGY_REQUEST_TOPIC + "'");

        } catch (MqttException e) {
            System.err.println("FATAL: Failed to set up MQTT listener for energy requests for Plant " + plant.getId() + ". Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Restituisce una copia della lista di tutti i peer attualmente conosciuti nella rete.
     * La copia previene modifiche accidentali alla mappa interna.
     * Questo metodo è thread-safe.
     *
     * @return Una nuova List<PeerInfo> contenente tutti i peer.
     */
    public List<PeerInfo> getAllPeers() {
        synchronized (peerStateLock) {
            // Restituiamo una NUOVA ArrayList, non un riferimento alla mappa interna.
            return new ArrayList<>(networkPeers.values());
        }
    }

    /**
     * Restituisce il numero di peer attualmente conosciuti nella rete.
     * @return Il numero di peer.
     */
    public int getNetworkSize() {
        synchronized (peerStateLock) {
            return networkPeers.size();
        }
    }

    /**
     * Restituisce le informazioni del peer successore nell'anello.
     * @return Il PeerInfo del prossimo nodo, o null se non esiste.
     */
    public PeerInfo getNextPeerInRing() {
        synchronized (peerStateLock) {
            return this.nextInRing;
        }
    }

    // CHIAMATO DALLA TPP QUANDO TORNA IDLE
    public void onProductionComplete() {
        plant.setAsIdle();
        System.out.println("LOG (Plant " + plant.getId() + "): TPP became IDLE.");
    }

    private void kickQueueProcessor() {
        synchronized (queueProcessorLock) {
            queueProcessorLock.notify();
        }
    }

    // Eseguito SOLO dal nostro thread singolo. Nessuna race condition.
    /**
     * Questo metodo viene eseguito ESCLUSIVAMENTE dal thread processore della coda.
     * Si assume la responsabilità di gestire una richiesta specifica.
     * Determina se il nodo attuale è il responsabile (ID più basso) e agisce di conseguenza.
     *
     * @param request La richiesta da gestire.
     */
    private void handleRequest(PendingRequest request) {
        // 1. DETERMINA IL RESPONSABILE
        int myId = plant.getId();
        int lowestId;
        String requestId = request.getRequestId();
        synchronized(peerStateLock) {
            if (networkPeers.isEmpty()) {
                return; // Non c'è una rete, non si può fare nulla.
            }
            lowestId = Collections.min(networkPeers.keySet());
        }

        // Se non sono io il responsabile, il mio lavoro finisce qui.
        // Il thread processore tornerà a dormire e attenderà nuovi eventi.
        if (myId != lowestId) {
            // È importante rimettere il flag a false se non sono io il responsabile,
            // altrimenti nessun altro thread trigger potrà "prenotare" questa richiesta.
            synchronized (queueLock) {
                // Cerca la richiesta nella coda per resettare il suo stato
                for (PendingRequest req : requestQueue) {
                    if (requestId.equals(requestId)) {
                        break;
                    }
                }
            }
            return;
        }

        // --- DA QUI IN POI, SONO IO IL RESPONSABILE ---

        // 2. PRENDI IN CARICO L'ELEZIONE
        // Prendo possesso dell'elezione. Da questo momento, nessun altro potrà avviare nulla.
        synchronized (electionStateLock) {
            // Se uno dei due circuiti è già attivo, non faccio nulla.
            // Il sistema si riprenderà al prossimo ciclo del processore.
            if (isElectionRunning || isSearchingForStarter) {
                return;
            }
            isElectionRunning = true;
            currentElectionId = request.getRequestId();
        }

        // 3. RIMUOVI LA RICHIESTA DALLA CODA
        // Ora che è ufficialmente mia responsabilità, la rimuovo dalla coda per evitare
        // che venga processata di nuovo.
        synchronized(queueLock) {
            requestQueue.remove(request);
        }

        System.out.println("LOG (Processor-" + myId + "): I am responsible for and handling request " + request.getRequestId());

        // 4. DECIDI L'AZIONE: AVVIA O PASSA IL TESTIMONE
        if (plant.isIdle()) {
            isElectionRunning = true; // Attivo il circuito di elezione
            currentElectionId = request.getRequestId();
            // SONO IDLE: avvio l'elezione direttamente.
            // Poiché non sono in un thread gRPC, non c'è rischio di 'CANCELLED'.
            System.out.println("LOG (Processor-" + myId + "): State is IDLE. Starting election directly.");
            plant.onEnergyRequest(request.getRequestId(), request.getKwh());
        } else {
            // SONO BUSY: passo il testimone al prossimo candidato valido.
            System.out.println("LOG (Processor-" + myId + "): State is BUSY. Passing the baton.");
            isSearchingForStarter = true; // Attivo il circuito di ricerca
            currentElectionId = request.getRequestId(); // Tengo traccia per quale richiesta sto cercando

            List<Integer> visitedIds = new ArrayList<>();
            visitedIds.add(myId); // Aggiungo me stesso come primo nodo visitato

            PeerInfo nextCandidate = findNextCandidate(visitedIds);

            if (nextCandidate != null) {
                System.out.println("LOG (Processor-" + myId + "): Passing baton to " + nextCandidate.getId());

                BatonMessage baton = BatonMessage.newBuilder()
                        .setRequestId(request.getRequestId())
                        .setKwh(request.getKwh())
                        .addAllVisitedNodesIds(visitedIds)
                        .build();

                // La chiamata è sicura perché viene da un thread dedicato e non da un worker gRPC.
                grpcClient.passElectionBaton(nextCandidate, baton);
            } else {
                // Nessun altro candidato, devo resettare per non rimanere bloccato
                System.err.println("WARN: Responsible node " + myId + " is BUSY and no other candidates found.");
                resetSearchState(); // Metodo helper per resettare isSearchingForStarter
            }
        }
    }

    /**
     * (Privato) Resetta solo lo stato relativo alla ricerca di uno starter.
     * Viene chiamato quando un coordinatore BUSY non riesce a trovare
     * nessun altro nodo a cui passare il testimone, evitando così un deadlock.
     * DEVE essere chiamato all'interno di un blocco synchronized(electionStateLock).
     */
    private void resetSearchState() {
        // Il lock è già stato acquisito dal chiamante (es. handleRequest)
        isSearchingForStarter = false;
        currentElectionId = null;
        // NON tocchiamo isThisNodeAParticipant, perché non è rilevante per la ricerca.

        System.out.println("LOG (Plant " + plant.getId() + "): 'Search for Starter' circuit has been reset.");

        // NOTA: In questo caso specifico, la richiesta che ha fallito il "baton pass"
        // NON è stata rimossa dalla coda. Quindi, al prossimo ciclo,
        // il processore del coordinatore ci riproverà. Questo è il comportamento desiderato.
    }


    // Aggiungi un metodo di shutdown per pulizia
    public void shutdown() {
        isRunning = false;
        if (pollingThread != null) {
            pollingThread.interrupt();
        }
        grpcClient.shutdown();
    }

    /**
     * Resetta lo stato relativo a una singola e specifica elezione.
     * Rimuove l'elezione dalla mappa delle elezioni attive.
     * Questo metodo DEVE essere chiamato all'interno di un blocco synchronized(electionStateLock).
     *
     * @param requestId L'ID della richiesta la cui elezione è terminata.
     */
    private void resetElectionStateFor(String requestId) {
        // Il lock è già stato acquisito dal metodo chiamante (es. handleElectionMessage)

        ElectionInfo removedElection = activeElections.remove(requestId);

        if (removedElection != null) {
            System.out.println("LOG (Plant " + plant.getId() + "): State for election '" + requestId + "' has been reset.");
        }
        // Se è null, potrebbe essere già stato resettato da un altro thread, il che è ok.

        // Controlliamo se ci sono altre elezioni attive. Se no, possiamo resettare
        // anche i flag globali per sicurezza.
        if (activeElections.isEmpty()) {
            this.isElectionRunning = false; // Non è più strettamente necessario, ma pulisce lo stato
            this.currentElectionId = null;  // Anche questo per pulizia
            System.out.println("LOG (Plant " + plant.getId() + "): No more active elections. Global state flags cleared.");
        } else {
            // Ci sono ancora altre elezioni in corso. Dobbiamo decidere chi è il "current"
            // Potremmo prendere il primo dalla mappa, ma per ora non è necessario
            // fare nulla. La logica si basa sulla mappa, non più sul singolo `currentElectionId`.
            System.out.println("LOG (Plant " + plant.getId() + "): " + activeElections.size() + " other elections still active.");
        }
    }
}