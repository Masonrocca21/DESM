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
import java.io.IOException;

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
            recalculateRingUnsafe();

            startMqttListenerForEnergyRequests();

            plant.setAsIdle();

            System.out.println("Plant " + plant.getId() + ": Initial ring configured with " + networkPeers.size() + " peers. My next is Plant " + (nextInRing != null ? nextInRing.getId() : "N/A"));
        }
    }

    /**
     * (Privato) Invia un annuncio di presenza in parallelo a una lista di peer.
     */
    private void broadcastMyPresence(List<PeerInfo> targets) {
        PlantInfoMessage myInfoMessage = plant.getPeerInfo().toProtobuf();
        for (PeerInfo target : targets) {
            // Lancia un thread per ogni chiamata per eseguirle in parallelo
            System.out.println("LOG (Plant " + plant.getId() + ", broadcastMyPresence): Preparing to announce to " + target.toString());
            new Thread(() -> {
                System.out.println("  -> (Thread " + Thread.currentThread().getId() + ") Announcing to Plant " + target.getId());
                grpcClient.announcePresence(target, myInfoMessage);
            }).start();
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

    public void shutdown() {
        grpcClient.shutdown();
        // shutdownGrpcServer();
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

        System.out.println("Plant " + myId + ": Starting election by sending the first message to " + this.nextInRing.getId() + ". Awaiting network confirmation...");

        ElectionMessage firstMessage = ElectionMessage.newBuilder()
                .setCurrentElectionRequestId(requestId)
                .setRequiredKwh(kwh)
                .setCandidateId(myId)
                .setCandidateValue(myPrice)
                .build();

        // --- Stessa logica wait/notify usata in handleElectionMessage ---

        final Object lock = new Object();

        grpcClient.sendElection(this.nextInRing, firstMessage, new RpcCallback() {
            @Override
            public void onCompleted() {
                synchronized (lock) {
                    lock.notify();
                }
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("Failed to send initial election message: " + t.getMessage());
                synchronized (lock) {
                    lock.notify();
                }
            }
        });

        // Ora, il thread del NetManager si mette in attesa indefinita,
        // fidandosi che gRPC chiamerà sempre onCompleted o onError.
        synchronized (lock) {
            try {
                lock.wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("ERROR: Wait for gRPC call completion was interrupted.");
            }
        }

        System.out.println("LOG (Plant " + plant.getId() + "): Forwarding confirmed. Resuming operations.");
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

        // Controlla se il peer è già noto per evitare di processare annunci duplicati.
        synchronized (peerStateLock) {
            if (networkPeers.containsKey(newPeer.getId())) {
                System.out.println("LOG (Plant " + plant.getId() + "): Received duplicate presence announcement from Plant " + newPeer.getId() + ". Ignoring.");
                return; // Già conosciuto, nessuna azione richiesta.
            }
        }

        System.out.println("\n--- NETWORK UPDATE (Plant " + plant.getId() + ") ---");
        System.out.println("Received presence of new peer " + newPeer.getId() + ". Updating my network view.");

        // 1. AGGIORNA la conoscenza locale della rete.
        networkPeers.put(newPeer.getId(), newPeer);

        // 2. RICALCOLA la topologia dell'anello.
        recalculateRingUnsafe();

        System.out.println("Ring updated. My knowledge: " + networkPeers.size() + " peers. My new next is Plant " + (nextInRing != null ? nextInRing.getId() : "N/A"));
        System.out.println("--------------------------------\n");
    }

    /**
     * Gestisce un messaggio di elezione in arrivo, implementando l'algoritmo di Chang e Roberts.
     * Un nodo IDLE che riceve questo messaggio si unisce attivamente all'elezione.
     * Un nodo BUSY inoltra passivamente il messaggio.
     *
     * @param message Il messaggio di elezione ricevuto.
     */
    public void handleElectionMessage(ElectionMessage message) {
        String receivedRequestId = message.getCurrentElectionRequestId();

        // --- 1. Controlli preliminari e gestione della partecipazione ---
        synchronized (electionStateLock) {
            // CASO 1: Nessuna elezione in corso per me. Questo messaggio la AVVIA.
            if (!isElectionRunning) {
                System.out.println("LOG (Plant " + plant.getId() + "): Received first election message for " + receivedRequestId + ". Entering election mode.");
                // Sincronizzo il mio stato con l'elezione appena scoperta.
                isElectionRunning = true;
                currentElectionId = receivedRequestId;
            }
            // CASO 2: Un'elezione è già in corso. Controllo che sia la stessa.
            else if (!receivedRequestId.equals(this.currentElectionId)) {
                System.out.println("LOG (Plant " + plant.getId() + "): Ignoring election message for " + receivedRequestId + " because I'm in election for " + this.currentElectionId);
                return; // Messaggio obsoleto o di un'elezione concorrente. Ignoro.
            }

            // Se sono qui, l'elezione è quella giusta. Devo decidere se partecipare.
            // Se non sono già un partecipante E sono IDLE, allora entro in gioco.
            if (!isThisNodeAParticipant && plant.isIdle()) {
                System.out.println("LOG (Plant " + plant.getId() + "): Joining an ongoing election for " + receivedRequestId);
                isThisNodeAParticipant = true;
                // Dico alla TPP di prepararsi (impostare stato e generare prezzo).
                plant.onJoinElection(receivedRequestId, message.getRequiredKwh());
            }
        }

        // --- 2. Inoltro passivo se non si è partecipanti (perché BUSY) ---
        // Leggo di nuovo la variabile (potrebbe essere cambiata nel blocco sopra).
        boolean amIParticipatingNow;
        synchronized(electionStateLock) {
            amIParticipatingNow = this.isThisNodeAParticipant;
        }

        if (!amIParticipatingNow) {
            System.out.println("LOG (Plant " + plant.getId() + "): Passively forwarding election message for " + receivedRequestId + " because I am BUSY. Awaiting network confirmation...");

            // --- Inserisci la logica wait/notify anche qui ---
            final Object lock = new Object();
            final boolean[] completed = {false};

            grpcClient.sendElection(this.nextInRing, message, new RpcCallback() {
                @Override
                public void onCompleted() {
                    synchronized (lock) { completed[0] = true; lock.notify(); }
                }
                @Override
                public void onError(Throwable t) {
                    System.err.println("Passive forwarding call failed: " + t.getMessage());
                    synchronized (lock) { completed[0] = true; lock.notify(); }
                }
            });

            // Ora, il thread del NetManager si mette in attesa indefinita,
            // fidandosi che gRPC chiamerà sempre onCompleted o onError.
            synchronized (lock) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("ERROR: Wait for gRPC call completion was interrupted.");
                }
            }

            System.out.println("LOG (Plant " + plant.getId() + "): Forwarding confirmed. Resuming operations.");
            return; // Fine dell'esecuzione per questo nodo
        }

        // --- 3. Logica di Chang & Roberts (solo per i partecipanti attivi) ---

        System.out.println("LOG (Plant " + plant.getId() + "): Actively processing election message for request " + receivedRequestId);

        // Ottiene i dati della propria offerta dalla TPP.
        double myPrice = plant.getCurrentOfferPrice();
        int myId = plant.getId();

        // Dati del candidato nel messaggio in arrivo.
        double candidatePrice = message.getCandidateValue();
        int candidateId = message.getCandidateId();

        ElectionMessage.Builder messageBuilder = message.toBuilder();
        boolean iAmTheNewBestCandidate = false;

        // Logica di confronto
        if (myPrice < candidatePrice) {
            iAmTheNewBestCandidate = true;
        }  else if (myPrice == candidatePrice && myId > candidateId) {
            // I prezzi sono uguali, ma il mio ID è più alto.
            // Come da requisito, vinco io il confronto (tie-break).
            System.out.println("LOG (Plant " + myId + "): Tie-break! My price (" + myPrice + ") is the same as Plant " + candidateId + "'s, but my ID is higher.");
            iAmTheNewBestCandidate = true;
        }

        if (iAmTheNewBestCandidate) {
            System.out.println("LOG (Plant " + myId + "): I am a better candidate. Updating message with my offer.");
            messageBuilder.setCandidateId(myId);
            messageBuilder.setCandidateValue(myPrice);
        }

        // Controllo della terminazione: se l'ID nel messaggio è il mio, ho vinto.
        if (messageBuilder.getCandidateId() == myId) {
            System.out.println("LOG (Plant " + myId + "): I have won the election for request " + receivedRequestId + "!");

            ElectedMessage winnerMessage = ElectedMessage.newBuilder()
                    .setWinnerId(myId)
                    .setWinnerValue(myPrice)
                    .setCurrentElectionRequestId(receivedRequestId)
                    .setRequiredKwh(message.getRequiredKwh())
                    .build();

            broadcastElectionResult(winnerMessage);
            handleElectedMessage(winnerMessage); // Gestisco il mio stesso annuncio per resettare lo stato.

        }else {
            // --- CASO INOLTRO (Questa parte va sostituita interamente) ---
            System.out.println("LOG (Plant " + plant.getId() + "): Forwarding election message. Awaiting network confirmation...");

            final Object forwardingLock = new Object(); // Lock locale per questa specifica operazione

            // Chiama il client gRPC, passando il nostro callback custom
            grpcClient.sendElection(this.nextInRing, messageBuilder.build(), new RpcCallback() {
                @Override
                public void onCompleted() {
                    synchronized (forwardingLock) {
                        forwardingLock.notify(); // Risveglia il thread del NetManager
                    }
                }

                @Override
                public void onError(Throwable t) {
                    System.err.println("Forwarding call failed: " + t.getMessage());
                    synchronized (forwardingLock) {
                        forwardingLock.notify(); // Risveglia il thread anche in caso di errore
                    }
                }
            });

            // Ora, il thread del NetManager si mette in attesa indefinita,
            // fidandosi che gRPC chiamerà sempre onCompleted o onError.
            synchronized (forwardingLock) {
                try {
                    forwardingLock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("ERROR: Wait for gRPC call completion was interrupted.");
                }
            }

            System.out.println("LOG (Plant " + plant.getId() + "): Forwarding confirmed. Resuming operations.");
        }
    }

    /**
     * (Privato) Invia il risultato di un'elezione a tutti i peer in parallelo.
     */
    private void broadcastElectionResult(ElectedMessage resultMessage) {
        List<PeerInfo> allPeers;
        synchronized(peerStateLock) {
            allPeers = new ArrayList<>(networkPeers.values());
        }

        for (PeerInfo peer : allPeers) {
            if (peer.getId() != plant.getId()) {
                final PeerInfo target = peer;
                new Thread(() -> {
                    // Chiamata "lancia e dimentica"
                    grpcClient.sendElected(target, resultMessage, new RpcCallback() {
                        @Override public void onCompleted() {}
                        @Override public void onError(Throwable t) {}
                    });
                }).start();
            }
        }
    }

    /**
     * Gestisce un messaggio 'ElectedMessage' in arrivo. Questo metodo ha una doppia funzione:
     * 1. Se è un normale risultato di elezione, notifica la centrale e resetta lo stato.
     * 2. Se è un comando di management (identificato da un 'winnerId' speciale),
     *    avvia le procedure interne corrispondenti (es. handover).
     *
     * @param message Il messaggio ElectedMessage ricevuto.
     */
    public void handleElectedMessage(ElectedMessage message) {

        // --- CASO 1: È un comando di management per l'handover ---
        if (message.getWinnerId() == -1 && "CMD:HANDOVER".equals(message.getCurrentElectionRequestId())) {

            // Estrae l'ID del nuovo nodo dal campo 'requiredKwh' (il nostro "hack").
            int newNodeId = (int) message.getRequiredKwh();
            PeerInfo newNodeInfo;

            // Recupera le informazioni del nuovo nodo dalla mappa dei peer in modo thread-safe.
            synchronized (peerStateLock) {
                newNodeInfo = networkPeers.get(newNodeId);
            }

            if (newNodeInfo != null) {
                System.out.println("LOG (Plant " + plant.getId() + ", as P): Received HANDOVER command for new node " + newNodeInfo.getId());
                // Avvia il processo di handover controllato.
                prepareForHandover(newNodeInfo);
            } else {
                System.err.println("Plant " + plant.getId() + ": Received handover command for an unknown new node ID: " + newNodeId);
            }
            return; // Il processo è terminato per questo comando.
        }

        // --- CASO 2: È un comando di management per lo SWITCH ---
        if (message.getWinnerId() == -1 && "CMD:SWITCH".equals(message.getCurrentElectionRequestId())) {
            System.out.println("LOG (Plant " + plant.getId() + ", as A): Received SWITCH message. The old channel from my predecessor is now clean.");
            return; // Nessuna ulteriore azione richiesta.
        }

        // --- CASO 3: È un normale risultato di elezione ---

        // Blocco sincronizzato per garantire che la modifica dello stato dell'elezione sia atomica.
        synchronized (electionStateLock) {
            // Se non c'è un'elezione in corso o se il risultato è per un'altra elezione, lo ignoriamo.
            if (!isElectionRunning || !message.getCurrentElectionRequestId().equals(this.currentElectionId)) {
                return; // Risultato obsoleto o non pertinente.
            }

            System.out.println("Plant " + plant.getId() + ": Election result received for request '" + message.getCurrentElectionRequestId() + "'. Winner is Plant " + message.getWinnerId());

            // 1. Notifica la centrale "cervello" dell'esito, che gestirà il proprio stato (IDLE/BUSY).
            plant.onElectionResult(message.getCurrentElectionRequestId(), message.getWinnerId());

            // 2. Resetta lo stato dell'elezione per essere pronti per la prossima.
            isElectionRunning = false;
            isThisNodeAParticipant = false;
            currentElectionId = null;

            System.out.println("Plant " + plant.getId() + ": Election state has been reset.");

            // 3. (Opzionale ma robusto) Ricalcola l'anello per sincronizzarsi
            //    con eventuali modifiche avvenute durante l'elezione.
            synchronized (peerStateLock) {
                recalculateRingUnsafe();
            }
        }
    }

    // --- GESTORE DI EVENTI MQTT ---
    /**
     * Gestisce una nuova richiesta di energia ricevuta via MQTT.
     * Avvia il processo di elezione se e solo se la rete non è già impegnata
     * in un'altra elezione.
     *
     * @param requestId L'ID univoco della richiesta di energia.
     * @param kwh La quantità di energia richiesta.
     */
    /**
     * Chiamato da MQTT. Solo la pianta con l'ID più basso agisce.
     */
    public void onMqttEnergyRequest(String requestId, double kwh) {
        int myId = plant.getId();
        int lowestIdInRing;

        synchronized (peerStateLock) {
            // Se non conosco nessun altro, sono io l'ID più basso.
            if (networkPeers.isEmpty() || networkPeers.size() == 1) {
                lowestIdInRing = myId;
            } else {
                lowestIdInRing = Collections.min(networkPeers.keySet());
            }
        }

        // Solo la pianta con l'ID più basso agisce per non iniziare N elezioni.
        if (myId != lowestIdInRing) {
            return;
        }

        System.out.println("LOG (Plant " + myId + "): I am the designated starter for request " + requestId);

        // --- LOGICA DI CONTROLLO RIVISTA ---

        // 1. PRIMA controlla se la nostra TPP è disponibile.
        if (!plant.isIdle()) {
            System.out.println("LOG (Plant " + myId + "): I am the starter, but I am BUSY. Aborting election for " + requestId + ". Network remains open.");
            // NON impostiamo isElectionRunning. L'elezione per questa richiesta fallisce,
            // ma la rete è pronta per la prossima. Questo è il comportamento corretto.
            return;
        }

        // 2. SOLO SE la TPP è IDLE, tentiamo di acquisire il "lock" dell'elezione.
        boolean canIStart = false;
        synchronized (electionStateLock) {
            if (!isElectionRunning) {
                isElectionRunning = true;
                currentElectionId = requestId;
                canIStart = true;
            }
        }

        // 3. Se abbiamo acquisito il lock con successo, procediamo.
        if (canIStart) {
            System.out.println("LOG (Plant " + myId + "): I am IDLE and have acquired the election lock. Starting process...");
            // Ora passiamo la palla alla TPP, che è garantito essere IDLE.
            plant.onEnergyRequest(requestId, kwh);
        } else {
            // Questo caso può accadere in una race condition molto rara se due messaggi MQTT
            // vengono processati quasi simultaneamente. È una protezione extra.
            System.out.println("LOG (Plant " + myId + "): I was ready, but another election started just before I could. Ignoring request " + requestId);
        }
    }

    /**
     * (Gestore gRPC) Chiamato quando si riceve il testimone dal predecessore.
     */
    public void handlePassBaton(BatonMessage request) {
        System.out.println("LOG (Plant " + plant.getId() + "): Received election baton for request " + request.getRequestId());
        handleBaton(request.getRequestId(), request.getKwh(), request.getInitiatorId());
    }

    /**
     * (Logica Centrale) Prova a gestire il testimone: o avvia l'elezione o passa avanti.
     */
    private void handleBaton(String requestId, double kwh, int initiatorId) {
        // Prevenzione del loop infinito: se il testimone è tornato da me,
        // significa che nessuno era disponibile.
        if (plant.getId() == initiatorId && isThisNodeAParticipant) { // `isThisNodeAParticipant` indica se ho già visto questo testimone
            System.out.println("LOG (Plant " + plant.getId() + "): Baton for " + requestId + " completed a full circle. No plant available. Election aborted.");
            // Resetto lo stato per permettere nuove elezioni.
            resetElectionState();
            return;
        }

        // Marco me stesso come partecipante a questo "passaggio di testimone".
        isThisNodeAParticipant = true;

        if (plant.isIdle()) {
            System.out.println("LOG (Plant " + plant.getId() + "): I am IDLE. I will start the election for " + requestId);
            // Avvio l'elezione vera e propria.
            plant.onEnergyRequest(requestId, kwh); // Questo chiamerà startElection etc.
        } else {
            System.out.println("LOG (Plant " + plant.getId() + "): I am BUSY. Passing the baton to my successor: " + nextInRing.getId());
            BatonMessage batonMessage = BatonMessage.newBuilder()
                    .setRequestId(requestId)
                    .setKwh(kwh)
                    .setInitiatorId(initiatorId) // Propago l'ID di chi ha iniziato il giro.
                    .build();
            grpcClient.passElectionBaton(this.nextInRing, batonMessage);
        }
    }

    // Aggiungi un metodo helper per resettare lo stato
    private void resetElectionState() {
        synchronized(electionStateLock) {
            isElectionRunning = false;
            currentElectionId = null;
            isThisNodeAParticipant = false;
        }
    }

    // --- METODI PRIVATI ---

    /**
     * (Privato) Prepara questo nodo (P) a inserire un nuovo nodo (NC) dopo di sé.
     * Imposta lo stato di "handover pending" e avvia l'invio del messaggio SWITCH.
     *
     * @param newNodeToInsert Il PeerInfo del nuovo nodo da inserire.
     */
    private void prepareForHandover(PeerInfo newNodeToInsert) {
        // Blocco sincronizzato per proteggere lo stato di handover.
        synchronized (ringChangeLock) {
            // Evita di avviare un nuovo handover se uno è già in corso.
            if (isHandoverPending) {
                System.err.println("LOG (Plant " + plant.getId() + ", as P): Cannot start a new handover, one is already pending.");
                return;
            }

            System.out.println("LOG (Plant " + plant.getId() + ", as P): Preparing for handover. My next pointer will change to " + newNodeToInsert.getId() + ". State is now PENDING.");
            this.isHandoverPending = true;
            this.pendingNextNode = newNodeToInsert;
        }

        // Una volta impostato lo stato, avvia l'invio del messaggio che "pulisce" il canale.
        sendSwitchMessage();
    }

    /**
     * (Privato) Invia un messaggio "SWITCH" all'attuale successore (A) e,
     * immediatamente dopo, aggiorna il proprio puntatore `nextInRing` per
     * puntare al nuovo nodo (NC). Questo garantisce la priorità dei messaggi in transito.
     */
    private void sendSwitchMessage() {
        PeerInfo oldNext;
        synchronized (peerStateLock) {
            oldNext = this.nextInRing;
        }
        if (oldNext == null) return;

        System.out.println("LOG (Plant " + plant.getId() + ", as P): Sending SWITCH message to " + oldNext.getId() + ". Awaiting confirmation...");

        ElectedMessage switchMsg = ElectedMessage.newBuilder()
                .setWinnerId(-1)
                .setCurrentElectionRequestId("CMD:SWITCH")
                .build();

        // --- Logica wait/notify per la chiamata critica ---
        final Object lock = new Object();
        final boolean[] completed = {false};

        grpcClient.sendElected(oldNext, switchMsg, new RpcCallback() {
            @Override
            public void onCompleted() {
                synchronized(lock) { completed[0] = true; lock.notify(); }
            }
            @Override
            public void onError(Throwable t) {
                System.err.println("ERROR sending SWITCH message to " + oldNext.getId() + ": " + t.getMessage());
                synchronized(lock) { completed[0] = true; lock.notify(); }
            }
        });

        // Attendi la conferma
        synchronized(lock) {
            try {
                lock.wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (!completed[0]) {
            // Questo non dovrebbe accadere senza timeout, ma è una sicurezza in caso di interruzione
            System.err.println("WARN: SWITCH message sending was interrupted.");
        }

        // --- Il "flip" del puntatore avviene solo DOPO la conferma ---
        synchronized (peerStateLock) {
            synchronized (ringChangeLock) {
                this.nextInRing = this.pendingNextNode;
                this.isHandoverPending = false;
                this.pendingNextNode = null;
                System.out.println("LOG (Plant " + plant.getId() + ", as P): SWITCH confirmed. POINTER FLIPPED. My next is now " + this.nextInRing.getId());
            }
        }
    }

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
                public void messageArrived(String topic, MqttMessage message) throws Exception {
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

                        // Notifica il NetManager per iniziare il processo di elezione
                        onMqttEnergyRequest(requestId, kwh);

                    } catch (Exception e) {
                        System.err.println("Plant " + plant.getId() + ": Failed to parse energy request payload. Error: " + e.getMessage());
                    }
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    // Non usato da un subscriber
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
}