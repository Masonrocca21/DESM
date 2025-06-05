package ThermalPowerPlants;

import com.example.powerplants.Ack;
import org.eclipse.paho.client.mqttv3.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import com.example.powerplants.PlantServiceGrpc;
import com.example.powerplants.PlantInfoMessage;
import PlantServiceGRPC.PlantServiceImpl;

import Simulators.PollutionSensor;
import PollutionManagement.SlidingWindowBuffer;

import org.json.JSONObject;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

public class ThermalPowerPlants {
    private int id;
    private String address;
    private int portNumber;
    private String serverAddress = "http://localhost:8080";
    private Server grpcServer;
    private volatile boolean isShutdownInitiated = false; // Per evitare chiamate multiple a shutdown
    volatile String currentProductionRequestId;

    private Set<String> concludedElectionRequestIds = ConcurrentHashMap.newKeySet(); // Thread-safe


    private boolean isActive;

    private volatile boolean participantFlagForNM = false; //Per capire se può partecipare alle elezioni

    private List<ThermalPowerPlants> AllPlants = new ArrayList<>();
    private List<ThermalPowerPlantInfo> allPlantsInfoInNetwork; // ThermalPowerPlantInfo è una classe/record semplice con id, address, port
    private final Map<Integer, PlantServiceGrpc.PlantServiceBlockingStub> connections = new HashMap<>();
    private Map<Integer, ManagedChannel> channels = new HashMap<>();
    public ManagedChannel successorChannel;
    private PlantServiceGrpc.PlantServiceBlockingStub successorStub;
    private Map<Integer, String> topology = new HashMap<>();
    private MqttClient mqttClient;
    private static String broker = "tcp://localhost:1883";
    private final String energyRequestTopic = "home/renewableEnergyProvider/power/new";

    private static final RestTemplate restTemplate = new RestTemplate();
    private static final double NO_VALID_PRICE = -1.0; // Valore sentinella
    private double myEnegyValue = NO_VALID_PRICE; //Prezzo in $/Kwh per l'elezione
    private volatile String requestIdForCurrentPrice = null;
    private double energyRequest;
    private NetworkManager networkManager; //Per gestire l'elezione
    private volatile boolean isCurrentlyBusy = false; // volatile per visibilità tra thread
    private volatile long productionEndsAtMillis = 0L;  // volatile per visibilità
    private volatile String activelyParticipatingInElectionForRequestId = null; //per vedere se sta partecipando a una elezione
    private volatile boolean isNewlyJoinedAndMustForward = true; //Per vedere se la pianta è nuova

    private SlidingWindowBuffer pollutionBuffer; // Usa la nostra implementazione
    private PollutionSensor sensorSimulator;     // Istanza del TUO simulatore

    private ScheduledExecutorService dataSenderScheduler;
    private final String pollutionDataTopic = "DESM/pollution_stats"; // O il topic che preferisci
    private int availableEnergy;

    public ThermalPowerPlants() {
        this.allPlantsInfoInNetwork = new ArrayList<>();
        this.pollutionBuffer = new SlidingWindowBuffer(); // Inizializza il buffer
    }

    public ThermalPowerPlants(int id, String address, int port, String adminAddress) {
        this.id = id;
        this.address = address;
        this.portNumber = port;
        this.serverAddress = adminAddress;

        this.allPlantsInfoInNetwork = new ArrayList<>();
        this.pollutionBuffer = new SlidingWindowBuffer(); // Inizializza il buffer

    }

    public static void main(String[] args) {
        ThermalPowerPlants plant = new ThermalPowerPlants();

        // Registra uno shutdown hook per chiamare il nostro metodo shutdown()
        // Questo gestirà Ctrl+C o terminazioni normali della JVM
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("JVM Shutdown Hook triggered for Plant: " + (plant.getId() != 0 ? plant.getId() : "UNKNOWN_ID_YET"));
            plant.shutdown();
        }, "PlantShutdownThread-" + plant.id)); // Dai un nome al thread dello hook

        try {
            plant.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    public void start() throws IOException, InterruptedException {
        // 1. Registrati all'amministratore
        List<ThermalPowerPlantInfo> existingPlants = registerToAdminServer();

        if (this.isActive) {

            // ----- MOMENTO CRUCIALE 1: Inizializzazione NM -----
            // Necessario per evitare NPE
            if (this.networkManager == null) {
                this.networkManager = new NetworkManager(this, this.id, 0.5, null, null);
                System.out.println("P3 (" + this.id + "): NetworkManager PRE-INITIALIZED. Hash: " + System.identityHashCode(this.networkManager));
            }
            // ---------------------------------------------------------

            // 2. Avvia server gRPC
            startGrpcServer();

            System.out.println("Server started");
            // 3. Presentati alle piante esistenti
            if (existingPlants != null && !existingPlants.isEmpty()) {
                announceMyselfToExistingPlants(existingPlants);
            } else {
                System.out.println("TPP " + this.id + ": No existing plants found to announce myself to.");
            }

            // 4. Configura il tuo anello (ora this.allPlantsInfoInNetwork dovrebbe contenere tutti, inclusi quelli a cui ci siamo presentati
            //    e noi stessi, che aggiungiamo in announceMyselfToExistingPlants o dopo)
            //    Aggiungiamo noi stessi alla lista *prima* di stabilire l'anello
            synchronized (this) {
                // Assicurati di non aggiungerti due volte se handleNewPlantAnnouncement lo fa per te
                boolean selfFound = false;
                for (ThermalPowerPlantInfo p : this.allPlantsInfoInNetwork) {
                    if (p.getId() == this.id) {
                        selfFound = true;
                        break;
                    }
                }
                if (!selfFound) {
                    this.allPlantsInfoInNetwork.add(new ThermalPowerPlantInfo(this.id, this.address, this.portNumber));
                }
                this.allPlantsInfoInNetwork.sort(Comparator.comparingInt(ThermalPowerPlantInfo::getId));

                System.out.println("TPP " + this.id + ": Final plant list before establishing ring: " + this.allPlantsInfoInNetwork);
                if (!this.allPlantsInfoInNetwork.isEmpty()) { // Controlla prima di passare a connectToOtherPlants
                    connectToOtherPlants(this.allPlantsInfoInNetwork);
                } else {
                    System.out.println("TPP " + this.id + ": Plant list is empty, cannot connect to other plants.");
                    // Se la lista è vuota, significa che sono il primo e unico.
                    // connectToOtherPlants (o establishRingConnection) dovrebbe gestire questo.
                    // Potremmo chiamarlo con una lista contenente solo noi stessi.
                    List<ThermalPowerPlantInfo> selfList = new ArrayList<>();
                    selfList.add(new ThermalPowerPlantInfo(this.id, this.address, this.portNumber));
                    connectToOtherPlants(selfList);
                }
            }

            // AVVIA SIMULATORE SENSORE DOPO CHE L'ID DELLA PIANTA È NOTO
            // Il costruttore di PollutionSensor(Buffer) genera un ID interno per il sensore.
            // L'ID della pianta (this.id) è per identificare da quale pianta provengono i dati.

            this.sensorSimulator = new PollutionSensor(this.pollutionBuffer);
            this.sensorSimulator.setName("PollutionSensorThread-Plant-" + this.id); // Dagli un nome descrittivo
            this.sensorSimulator.start(); // Avvia il thread del sensore
            System.out.println("TPP " + this.id + ": Pollution Sensor (" + this.sensorSimulator.getIdentifier() + ") started.");

            // AVVIA SCHEDULER PER INVIO DATI
            startPeriodicDataSender();

            // 4. Inizializza MQTT
            setupMqtt();

            // 5. Subscrivi al topic per ricevere richieste energetiche
            subscribeToEnergyRequests();
        }
    }

    private List<ThermalPowerPlantInfo> registerToAdminServer() throws IOException {
        // HTTP POST verso l'amministratore con ID, address e port
        BufferedReader inputStream =
                new BufferedReader(new InputStreamReader(System.in));

        String postPath = "/Administrator/add";

        //Faccio settare i parametri allutente
        System.out.println("Adding a thermal plant");
        System.out.println("Enter ID: ");
        int id = Integer.parseInt(inputStream.readLine());
        System.out.println("Enter address: ");
        String address = inputStream.readLine();
        System.out.println("Enter portNumber: ");
        int portNumber = Integer.parseInt(inputStream.readLine());

        //Creo la pianta termale
        //ThermalPowerPlants newPlant = new ThermalPowerPlants(id, address, portNumber, serverAddress);
        this.address = address;
        this.portNumber = portNumber;
        this.id = id;

        this.isActive = false; // Inizia come non attivo
        List<ThermalPowerPlantInfo> existingPlantsList = new ArrayList<>();

        //eseguo la chiamata a post
        ResponseEntity<ThermalPowerPlants[]> postResponse = postRequest(serverAddress + postPath, this);

        //Se tutto funziona bene, salvo la lista di tutte le piante termali nella nuova pianta termale
        if (postResponse.getStatusCode() == HttpStatus.OK && postResponse.getBody() != null) {
            this.isActive = true;
            ThermalPowerPlants[] plantsArrayFromServer = postResponse.getBody();

            //Rimuovo la mia pianta dalla lista
            for (ThermalPowerPlants plantData : plantsArrayFromServer) {
                if (plantData.getId() != this.id) { // Se l'admin la include, la escludiamo qui per la fase di annuncio
                    existingPlantsList.add(new ThermalPowerPlantInfo(plantData.getId(), plantData.getAddress(), plantData.getPortNumber()));
                }
            }

            synchronized (this) {
                this.allPlantsInfoInNetwork.clear();
                for (ThermalPowerPlants plantData : plantsArrayFromServer) {
                    this.allPlantsInfoInNetwork.add(new ThermalPowerPlantInfo(plantData.getId(), plantData.getAddress(), plantData.getPortNumber()));
                }
                // Assicurati che la pianta corrente sia nella sua lista, se l'admin non l'ha inclusa
                boolean selfInList = false;
                for (ThermalPowerPlantInfo p : this.allPlantsInfoInNetwork) {
                    if (p.getId() == this.id) {
                        selfInList = true;
                        break;
                    }
                }
                if (!selfInList) {
                    this.allPlantsInfoInNetwork.add(new ThermalPowerPlantInfo(this.id, this.address, this.portNumber));
                }
                Collections.sort(this.allPlantsInfoInNetwork, Comparator.comparingInt(ThermalPowerPlantInfo::getId));
            }


            System.out.println("TPP " + this.id + ": Registered. Admin returned " + plantsArrayFromServer.length +
                    " plants. Existing for announcement: " + existingPlantsList.size());
            return existingPlantsList; // Lista delle altre piante a cui presentarsi
        } else {
            System.err.println("TPP " + this.id + ": Registration failed. Status: " + postResponse.getStatusCode());
            return null; // o lancia eccezione
        }
    } //FUNZIONA

    private void announceMyselfToExistingPlants(List<ThermalPowerPlantInfo> existingPlants) {
        System.out.println("TPP " + this.id + ": Announcing my presence to " + existingPlants.size() + " existing plants (SEQUENTIALLY).");
        PlantInfoMessage myInfoMessage = PlantInfoMessage.newBuilder()
                .setId(this.id)
                .setAddress(this.address)
                .setPortNumber(this.portNumber)
                .build();

        for (ThermalPowerPlantInfo existingPlant : existingPlants) {
            ManagedChannel channel = null; // Inizializza a null per il blocco finally
            try {
                System.out.println("TPP " + this.id + ": Sending AnnouncePresence to Plant " + existingPlant.getId() +
                        " at " + existingPlant.getAddress() + ":" + existingPlant.getPortNumber());
                channel = ManagedChannelBuilder
                        .forAddress(existingPlant.getAddress(), existingPlant.getPortNumber())
                        .usePlaintext()
                        .build();
                PlantServiceGrpc.PlantServiceBlockingStub stub = PlantServiceGrpc.newBlockingStub(channel);

                // Effettua la chiamata gRPC con un timeout
                Ack ack = stub.announcePresence(myInfoMessage);
                System.out.println("TPP " + this.id + ": Received Ack from Plant " + existingPlant.getId() + ": " + ack.getMessage());

            } catch (Exception e) {
                System.err.println("TPP " + this.id + ": Failed to announce presence to Plant " + existingPlant.getId() +
                        " (" + existingPlant.getAddress() + ":" + existingPlant.getPortNumber() + "): " + e.getMessage());
                // Continua con la prossima pianta anche se una fallisce
            } finally {
                // Assicurati di chiudere il canale dopo ogni tentativo
                if (channel != null) {
                    try {
                        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        System.err.println("TPP " + this.id + ": Interrupted while shutting down channel to Plant " + existingPlant.getId());
                        Thread.currentThread().interrupt(); // Ripristina lo stato di interruzione
                    }
                }
            }
        }
        System.out.println("TPP " + this.id + ": Finished announcing presence sequentially to all known existing plants.");
    }

    private void startGrpcServer() throws IOException {
        Server server = ServerBuilder.forPort(portNumber)
                .addService(new PlantServiceImpl(this))
                .build()
                .start();

        System.out.println("Server gRPC avviato sulla porta " + portNumber);
    } //FUNZIONA

    // Metodo chiamato dal servicer gRPC quando un'altra pianta si annuncia
    // Sincronizzato per evitare chiamate concorrenti al servizio di gRPC e quindi calcoli dell'anello contemporanei che poossono portare a situazioni ibride
    public synchronized boolean handleNewPlantAnnouncement(ThermalPowerPlantInfo newPlantInfo) {

        boolean alreadyExists = false;
        for (ThermalPowerPlantInfo p : this.allPlantsInfoInNetwork) {
            if (p.getId() == newPlantInfo.getId()) {
                alreadyExists = true;
                break;
            }
        }

        System.out.println("SuccStub a inizio handleNewPlantAnnouncement: " + this.successorStub);

        if (!alreadyExists) {
            this.allPlantsInfoInNetwork.add(newPlantInfo);
            Collections.sort(this.allPlantsInfoInNetwork, Comparator.comparingInt(ThermalPowerPlantInfo::getId));
            System.out.println("TPP " + this.id + ": Added new Plant " + newPlantInfo.getId() +
                    " to my list. New list size: " + this.allPlantsInfoInNetwork.size());
            System.out.println("TPP " + this.id + ": My current plant list: " + this.allPlantsInfoInNetwork);

            System.out.println("TPP " + this.id + ": Re-establishing ring due to new plant announcement.");
            // Passa una copia della lista per evitare problemi se establishRingConnection
            // itera su di essa mentre un altro thread potrebbe (teoricamente, anche se improbabile con synchronized) modificarla.
            // Tuttavia, dato che il metodo è synchronized, this.allPlantsInfoInNetwork non cambierà
            // finché questo thread non ha finito.
            connectToOtherPlants(new ArrayList<>(this.allPlantsInfoInNetwork));
            return true;
        } else {
            System.out.println("TPP " + this.id + ": Plant " + newPlantInfo.getId() +
                    " (announcement) already known. List size: " + this.allPlantsInfoInNetwork.size());

            System.out.println("SuccStub a fine handleNewPlantAnnouncement: " + this.successorStub);
            return false;
        }
    }

    public void connectToOtherPlants(List<ThermalPowerPlantInfo> allPlantsInfoSortedById) {
        System.out.println("TPP " + this.id + " is establishing its ring connection based on " + allPlantsInfoSortedById.size() + " plants.");
        establishRingConnection(allPlantsInfoSortedById);

        System.out.println("TPP " + this.id + ": Finished establishing ring connection. My successor stub is: " + this.successorStub);
        networkManager = new NetworkManager(this, id, this.myEnegyValue, this.successorStub, this.successorChannel);
        System.out.println("TPP " + this.id + ": NetworkManager instance: " + System.identityHashCode(this.networkManager));
    }

    // Metodo chiamato dopo che la pianta si è registrata e ha ricevuto la lista
    // di TUTTE le piante attualmente nella rete (inclusa sé stessa).
    // La lista è già ordinata per ID.
    public synchronized void establishRingConnection(List<ThermalPowerPlantInfo> allPlantsSortedById) {
        this.allPlantsInfoInNetwork = new ArrayList<>(allPlantsSortedById); // Salva una copia
        // Chiudi il vecchio canale (SE ESISTE E SE È DIVERSO DAL NUOVO POTENZIALE)
        ManagedChannel oldChannel = this.successorChannel; // Salva riferimento al vecchio

        if (this.allPlantsInfoInNetwork == null || this.allPlantsInfoInNetwork.isEmpty()) {
            System.err.println("TPP " + this.id + ": Plant list is empty, cannot establish ring.");
            return;
        }

        if (this.allPlantsInfoInNetwork.size() == 1 && this.allPlantsInfoInNetwork.get(0).getId() == this.id) {
            System.out.println("TPP " + this.id + ": I am the only plant in the network. No successor.");
            this.successorChannel = null;
            this.successorStub = null;

            return;
        }

        int myIndex = -1;
        for (int i = 0; i < this.allPlantsInfoInNetwork.size(); i++) {
            if (this.allPlantsInfoInNetwork.get(i).getId() == this.id) {
                myIndex = i;
                break;
            }
        }

        if (myIndex == -1) {
            System.err.println("TPP " + this.id + ": Could not find myself in the plant list. This should not happen.");
            return; // O gestisci l'errore
        }

        // Determina l'indice del successore
        int successorIndex = (myIndex + 1) % this.allPlantsInfoInNetwork.size();
        ThermalPowerPlantInfo successorInfo = this.allPlantsInfoInNetwork.get(successorIndex);

        // Assicurati che il successore non sia te stesso (a meno che non sia l'unica altra pianta)
        // Questa condizione è già implicitamente gestita dal size == 1 sopra
        // se ci sono solo due piante, il successore dell'altra sono io.
        if (successorInfo.getId() == this.id) {
            if (this.allPlantsInfoInNetwork.size() > 1) {
                System.err.println("TPP " + this.id + ": Calculated successor is myself, but network size is > 1. Error in logic.");
                // Potrebbe succedere se la lista non è aggiornata correttamente o se c'è un bug
                this.successorChannel = null;
                this.successorStub = null;
                successorInfo = null;
                return;
            }
            // Se network size è 1, è già gestito sopra (nessun successore)
        }


        System.out.println("TPP " + this.id + ": My successor is " + successorInfo.getId() +
                " at " + successorInfo.getAddress() + ":" + successorInfo.getPortNumber());

        // Ottieni le informazioni del nuovo successore
        ThermalPowerPlantInfo newSuccessorInfo = this.allPlantsInfoInNetwork.get(successorIndex);

        // Chiudi il canale precedente se esisteva, per evitare resource leak
        if (this.successorChannel != null && !this.successorChannel.isShutdown()) {
            this.successorChannel.shutdown();
            try {
                if (!this.successorChannel.awaitTermination(5, TimeUnit.SECONDS)) {
                    this.successorChannel.shutdownNow();
                }
            } catch (InterruptedException e) {
                this.successorChannel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        this.successorChannel = null; // Resetta esplicitamente
        this.successorStub = null;    // Resetta esplicitamente

        // Crea il nuovo canale e stub SE C'È un successore effettivo e non sono io stesso (se > 1 pianta)
        ManagedChannel newManagedChannelForSuccessor = null;
        PlantServiceGrpc.PlantServiceBlockingStub newStubForSuccessor = null;

        if (newSuccessorInfo != null && !(newSuccessorInfo.getId() == this.id)) { // Non connetterti a te stesso
            System.out.println("TPP " + this.id + ": My new successor is " + newSuccessorInfo.getId() +
                    " at " + newSuccessorInfo.getAddress() + ":" + newSuccessorInfo.getPortNumber());
            newManagedChannelForSuccessor = ManagedChannelBuilder
                    .forAddress(newSuccessorInfo.getAddress(), newSuccessorInfo.getPortNumber())
                    .usePlaintext()
                    .build();
            newStubForSuccessor = PlantServiceGrpc.newBlockingStub(newManagedChannelForSuccessor);
            System.out.println("TPP " + this.id + ": Successfully created new channel and stub for successor " + newSuccessorInfo.getId());
        } else if (newSuccessorInfo != null && (newSuccessorInfo.getId() == this.id) && this.allPlantsInfoInNetwork.size() == 1) {
            System.out.println("TPP " + this.id + ": I am the only plant. No successor.");
            // newManagedChannelForSuccessor e newStubForSuccessor rimangono null
        } else if (newSuccessorInfo == null) {
            System.out.println("TPP " + this.id + ": No successor found (e.g., empty list or error).");
        }


        // Aggiorna i membri di ThermalPowerPlant
        this.successorChannel = newManagedChannelForSuccessor;
        this.successorStub = newStubForSuccessor;

        System.out.println("TPP " + this.id + ": Successor updated. New stub is " + (this.successorStub != null) +
                ". Old channel to shutdown: " + (oldChannel != null));

        // Aggiorna NetworkManager
        if (this.networkManager == null) {
            // Prima creazione di NetworkManager
            double initialPrice = 0.5; // o un valore di default o calcolato
            this.networkManager = new NetworkManager(
                    this,
                    this.id,
                    initialPrice,
                    this.successorStub,
                    this.successorChannel
            );
            System.out.println("TPP " + this.id + ": NetworkManager initialized.");
        } else {
            System.out.println("TPP " + this.id + ": Successor identity may have changed. NM will fetch current stub as needed.");
        }

        // Chiudi il vecchio canale DOPO aver aggiornato i riferimenti e informato NM (se necessario)
        if (oldChannel != null && oldChannel != this.successorChannel && !oldChannel.isShutdown()) {
            System.out.println("TPP " + this.id + ": Shutting down PREVIOUS successor channel.");
            oldChannel.shutdown();
            try {
                if (!oldChannel.awaitTermination(5, TimeUnit.SECONDS)) {
                    oldChannel.shutdownNow();
                }
            } catch (InterruptedException e) {
                oldChannel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        this.participantFlagForNM = true; // La pianta è pronta per la logica NM base dopo la configurazione dell'anello
        System.out.println("TPP " + this.id + ": Ring connection setup/update complete. My successor stub for NetworkManager: " + (this.successorStub != null));
    }

    /**
     * Controlla se la pianta è attualmente occupata a produrre energia.
     * Se la produzione è terminata, aggiorna lo stato a non occupato.
     * Questo metodo è thread-safe usando la sincronizzazione sul metodo stesso.
     *
     * @return true se la pianta è occupata, false altrimenti.
     */
    public synchronized boolean isBusyProducing() { // Metodo Sincronizzato
        if (isCurrentlyBusy) {
            if (System.currentTimeMillis() >= productionEndsAtMillis) {
                System.out.println("TPP " + this.id + ": Production time ended. Releasing busy state.");
                isCurrentlyBusy = false;
                productionEndsAtMillis = 0L;
                currentProductionRequestId = null; // Assicurati che venga resettato
                return false;
            }
            return true;
        }
        return false;
    }

    // Metodo per controllare se la pianta può avviare/partecipare a una NUOVA elezione
    public synchronized boolean canParticipateInNewElection() {
        if (isCurrentlyBusy) {
            System.out.println("TPP " + this.id + ": Cannot participate in new election, currently busy producing for request '" + currentProductionRequestId + "'.");
            return false;
        }
        if (activelyParticipatingInElectionForRequestId != null) {
            System.out.println("TPP " + this.id + ": Cannot participate in new election, already actively participating in election for request '" + activelyParticipatingInElectionForRequestId + "'.");
            return false;
        }
        // Se isNewlyJoinedAndMustForward è true, questa pianta non dovrebbe INIZIARE una nuova elezione
        // (perché potrebbe non avere ancora un successore stabile o una visione completa della rete).
        // Ma il controllo principale per le nuove piante è in NetworkManager.onElectionMessage.
        // Qui, per coerenza, potremmo anche aggiungerlo, ma è più critico per la *ricezione* di messaggi.
        return true;
    }

    // Metodo chiamato quando la pianta inizia a partecipare attivamente a un'elezione
    public synchronized void startedActiveParticipationInElection(String requestId) {
        this.activelyParticipatingInElectionForRequestId = requestId;
        System.out.println("TPP " + this.id + ": Actively participating in election for request '" + requestId + "'.");
    }

    // Metodo da chiamare PRIMA di avviare una nuova elezione (es. da callback MQTT)
    public boolean prepareForNewElection(String requestId, double kwh) { // kwh solo per logging forse
        synchronized (this) {
            if (isElectionConcluded(requestId)) { // <<< CONTROLLO QUI
                System.out.println("TPP " + this.id + ": Election for request '" + requestId + "' ALREADY CONCLUDED. Cannot prepare/participate.");
                return false;
            }

            if (isBusyProducing()) { // Usa la nuova isBusyProducing()
                System.out.println("TPP " + this.id + ": Cannot prepare for new election for request '" + requestId +
                        "', currently busy producing for request '" + currentProductionRequestId + "'.");
                return false;
            }
            if (activelyParticipatingInElectionForRequestId != null && !this.activelyParticipatingInElectionForRequestId.equals(requestId)) {
                System.out.println("TPP " + this.id + ": Cannot prepare for new election for request '" + requestId +
                        "', already actively participating in election for request '" +
                        activelyParticipatingInElectionForRequestId + "'.");
                return false;
            }

            if (requestId.equals(this.requestIdForCurrentPrice) && this.myEnegyValue != NO_VALID_PRICE) {
                System.out.println("TPP " + this.id + ": Already prepared for election '" + requestId + "' with price " + this.myEnegyValue);
                startedActiveParticipationInElection(requestId); // Riafferma la partecipazione
                return true;
            }

            // Se tutti i controlli passano, la pianta si prepara
            this.myEnegyValue = 0.1 + (0.8 * new java.util.Random().nextDouble());
            this.requestIdForCurrentPrice = requestId;

            /* if (networkManager != null) { // Assicurati che NM esista
                networkManager.setValue(this.myEnegyValue);
            } else {
                System.err.println("TPP " + this.id + ": NetworkManager is null during prepareForNewElection for request '" + requestId + "'! Cannot set value.");
                return false; // Non possiamo procedere
            }
             */
            startedActiveParticipationInElection(requestId); // Segna che ora è impegnata per questa richiesta
            System.out.println("TPP " + this.id + ": Prepared for new election. Request ID: '" + requestId + "', Price: " + this.myEnegyValue);
            return true;
        }
    }

    // Metodo chiamato quando un'elezione (per un dato requestId) si conclude per questa pianta
    // (o quando smette di essere partecipante attivo)
    public synchronized void electionProcessConcludedForRequest(String requestId) {
        boolean wasParticipating = false;

        if (requestId != null && requestId.equals(this.activelyParticipatingInElectionForRequestId)) {
            this.activelyParticipatingInElectionForRequestId = null;
            wasParticipating = true;
            System.out.println("TPP " + this.id + ": Concluded active participation in election for request '" + requestId + "'. Now available for other elections.");
        }
        if (requestId != null && requestId.equals(this.requestIdForCurrentPrice)) {
            // Pulisce il prezzo solo se l'elezione conclusa è quella per cui avevamo questo prezzo.
            clearCurrentPriceInfo();
        } else if (wasParticipating) {
            System.out.println("TPP " + this.id + ": Election for request '" + requestId + "' concluded, but current price was for a different request ('" + this.requestIdForCurrentPrice + "'). Price not cleared by this event.");
        }
    }


    /**
     * Metodo chiamato quando QUESTA pianta vince un'elezione e deve iniziare la produzione.
     * Imposta lo stato della pianta a "occupato" per la durata calcolata.
     * Questo metodo è thread-safe usando la sincronizzazione sul metodo stesso.
     *
     * @param kWhToProduce La quantità di kWh da produrre.
     */
    public synchronized void handleElectionWinAndStartProduction(String requestId, double kWhToProduce) { // Metodo Sincronizzato
        long productionDurationMs = (long) kWhToProduce * 1; // 1 millisecondo per kWh

        // Anche se isBusyProducing() è sincronizzato, un controllo qui dentro un blocco sincronizzato
        // previene una potenziale (anche se piccola) race condition tra il controllo esterno
        // e l'impostazione dello stato busy qui.
        if (isCurrentlyBusy && System.currentTimeMillis() < productionEndsAtMillis) {
            System.err.println("TPP " + this.id + ": CRITICAL ERROR - Won election for request " +
                    " but ALREADY BUSY producing until " + new Date(productionEndsAtMillis) +
                    ". Ignoring new production task.");
            return;
        }

        markElectionAsConcluded(requestId); // <<< MARCA COME CONCLUSA
        electionProcessConcludedForRequest(requestId); // Pulisce prezzo e partecipazione attiva
        isCurrentlyBusy = true;
        productionEndsAtMillis = System.currentTimeMillis() + productionDurationMs;
        this.currentProductionRequestId = requestId; // Salva l'ID della richiesta per cui stiamo producendo

        System.out.println("TPP " + this.id + ": WON ELECTION for request '" + requestId + "'. " +
                "Starting production of " + kWhToProduce + " kWh. " +
                "Will be busy for " + productionDurationMs + " ms (until " +
                new Date(productionEndsAtMillis) + ").");

        // Avvia un thread separato per "sbloccare" lo stato busy dopo la durata.
        Thread productionTimerThread = new Thread(() -> {
            try {
                Thread.sleep(productionDurationMs);
            } catch (InterruptedException e) {
                System.err.println("TPP " + this.id + ": Production timer for request " + " interrupted.");
                Thread.currentThread().interrupt();
            } finally {
                // Reset proattivo dello stato. Deve essere sincronizzato se accede
                // alle stesse variabili di isBusyProducing.
                synchronized (this) {
                    final String productionRequestId = requestId; // Cattura il requestId del metodo esterno
                    // Sincronizza sul lock intrinseco dell'oggetto ThermalPowerPlants
                    // Controlla se questa era ancora la produzione che doveva finire ora
                    // E se il productionEndsAtMillis non è stato sovrascritto
                    if (isCurrentlyBusy && System.currentTimeMillis() >= productionEndsAtMillis &&
                            productionEndsAtMillis != 0L &&
                            productionRequestId.equals(this.currentProductionRequestId)) { // Aggiunto controllo per productionEndsAtMillis != 0L
                        // Per essere sicuri, potremmo voler controllare se productionEndsAtMillis è esattamente
                        // quello che ci aspettavamo da QUESTA produzione, ma diventa più complesso.
                        // Il controllo temporale è di solito sufficiente.
                        System.out.println("TPP " + this.id + ": Production timer thread for request " + " finished. Releasing busy state.");
                        isCurrentlyBusy = false;
                        productionEndsAtMillis = 0L;
                        this.currentProductionRequestId = null; // Resetta l'ID della richiesta in produzione
                        sendAckToRep(productionRequestId); // Invia ACK per la richiesta corretta
                    } else if (isCurrentlyBusy && !requestId.equals(this.currentProductionRequestId)) {
                        // Questo caso si verifica se, nel frattempo, la TPP ha vinto un'altra elezione
                        // e this.currentProductionRequestId è stato sovrascritto.
                        // Il timer per la vecchia richiesta 'requestId' non dovrebbe resettare lo stato
                        // che appartiene ora a una produzione più recente.
                        System.out.println("TPP " + this.id + ": Production timer for OLD request '" + requestId +
                                "' (kWh: " + kWhToProduce + ") finished, but plant is currently busy with NEWER request '" +
                                this.currentProductionRequestId + "'. State NOT changed by this (old) timer.");
                    } else if (!isCurrentlyBusy && requestId.equals(this.currentProductionRequestId)) {
                        // Stato già rilasciato, forse da un altro meccanismo o un precedente timer.
                        System.out.println("TPP " + this.id + ": Production timer for request '" + requestId +
                                "' (kWh: " + kWhToProduce + ") finished, but plant was no longer busy for this request (or busy state already cleared). Current request ID was: " + this.currentProductionRequestId);
                        sendAckToRep(requestId);
                        // Assicura che currentProductionRequestId sia null se corrisponde e non siamo busy
                        if (this.currentProductionRequestId != null && this.currentProductionRequestId.equals(requestId)) {
                            this.currentProductionRequestId = null;
                        }
                    }
                }
            }
        });
        productionTimerThread.setName("ProductionTimer-" + this.id);
        productionTimerThread.setDaemon(true);
        productionTimerThread.start();
    }

    private void sendAckToRep(String acknowledgedRequestId) {
        if (mqttClient != null && mqttClient.isConnected()) {
            JSONObject ackPayload = new JSONObject();
            ackPayload.put("requestId", acknowledgedRequestId);
            ackPayload.put("plantId", this.id); // Informazione aggiuntiva utile per il REP
            ackPayload.put("message", "TPP " + this.id + " will handle request " + acknowledgedRequestId);
            ackPayload.put("timestamp", System.currentTimeMillis());

            String payloadStr = ackPayload.toString();
            MqttMessage ackMessage = new MqttMessage(payloadStr.getBytes(StandardCharsets.UTF_8));
            ackMessage.setQos(1); // QoS 1 o 2 per l'ACK è una buona idea

            try {
                // Assicurati che ENERGY_ACK_TOPIC sia definito, es. come costante statica
                // private static final String ENERGY_ACK_TOPIC_FROM_TPP = "home/renewableEnergyProvider/power/ack";
                mqttClient.publish("home/renewableEnergyProvider/power/ack", ackMessage); // Usa il topic corretto
                System.out.println("TPP " + this.id + ": Sent ACK to REP for requestId: " + acknowledgedRequestId);
            } catch (MqttException e) {
                System.err.println("TPP " + this.id + ": Failed to send ACK for requestId " + acknowledgedRequestId + " to REP: " + e.getMessage());
                // Qui potresti considerare una logica di retry per l'ACK se è critico
            }
        } else {
            System.err.println("TPP " + this.id + ": Cannot send ACK for requestId " + acknowledgedRequestId + ", MQTT client not connected.");
        }
    }

    private void setupMqtt() {
        // Inizializza MQTT client, connect, setCallback, ecc.
        try {
            String clientId = "ThermalPlant-" + id;
            mqttClient = new MqttClient(broker, clientId);
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);

            mqttClient.connect(options);
            System.out.println("Connesso al broker MQTT");

        } catch (MqttException e) {
            System.err.println("Errore durante la connessione MQTT: " + e.getMessage());
        }
    }

    private void subscribeToEnergyRequests() {
        try {
            mqttClient.subscribe(energyRequestTopic, 1, (topic, message) -> {
                String payload = new String(message.getPayload());
                System.out.println("TPP " + this.id + ": Energy request received on MQTT: " + payload);

                JSONObject requestJson;
                String parsedRequestId;
                double kwhDouble;
                try {
                    requestJson = new JSONObject(payload);
                    parsedRequestId = requestJson.getString("requestId");
                    kwhDouble = requestJson.getDouble("kWh");
                } catch (Exception e) {
                    System.err.println("TPP " + this.id + ": Error parsing MQTT message: " + payload + ". Error: " + e.getMessage());
                    return; // Non possiamo procedere
                }

                // Controlli veloci da fare nel thread del callback
                if (isBusyProducing()) {
                    System.out.println("TPP " + this.id + ": IGNORING new energy request '" + parsedRequestId +
                            "' because currently busy producing for . (Callback Thread)");
                    return;
                }

                if (this.activelyParticipatingInElectionForRequestId != null) {
                    System.out.println("TPP " + this.id + ": IGNORING new energy request '" + parsedRequestId +
                            "' because already actively participating in election for request '" +
                            this.activelyParticipatingInElectionForRequestId + "'. (Callback Thread)");
                    return;
                }

                if (networkManager == null || !this.participantFlagForNM) { // participantFlagForNM verifica se TPP è pronta
                    System.err.println("TPP " + this.id + ": NetworkManager not ready or TPP not fully initialized for elections! Cannot process request " +
                            parsedRequestId + ". (Callback Thread)");
                    return;
                }

                // Prepara i dati per il nuovo thread
                final String finalRequestId = parsedRequestId;
                final double finalKwh = kwhDouble;

                if (!prepareForNewElection(finalRequestId, finalKwh)) {
                    System.out.println("TPP " + this.id + ": Did not start election for request '" + finalRequestId + "' due to current state.");
                    // Qui potresti mettere la richiesta in una coda
                    return; // Esce dal gestore della richiesta specifica
                }

                // Crea e avvia un nuovo thread per gestire l'avvio dell'elezione
                Thread electionHandlerThread = new Thread(() -> {
                    try {
                        System.out.println("TPP " + this.id + ": Election Handler Thread started for request '" + finalRequestId + "'.");
                        // Il prezzo è già stato generato e associato in prepareForNewElection.
                        // NetworkManager lo prenderà dalla TPP.
                        handleIncomingEnergyRequest(finalRequestId, finalKwh);
                    } catch (Exception e) {
                        System.err.println("TPP " + this.id + ": Error processing energy request '" + finalRequestId + "' in dedicated thread: " + e.getMessage());
                        e.printStackTrace();
                    } finally {
                        System.out.println("TPP " + this.id + ": Election Handler Thread finished for request '" + finalRequestId + "'.");
                    }
                });
                electionHandlerThread.setName("ElectionHandler-" + this.id + "-" + finalRequestId);
                electionHandlerThread.start();

            });
            System.out.println("TPP " + this.id + ": Subscribed to topic " + energyRequestTopic);

        } catch (MqttException e) {
            System.err.println("TPP " + this.id + ": Error in MQTT subscription: " + e.getMessage());
        }
    }

    public void handleIncomingEnergyRequest(String requestId, double kWh) {
        System.out.println("TPP " + this.id + ": Attempting to start election for request '" + requestId + "'.");

        if (networkManager != null && this.participantFlagForNM) {
            // Verifica che la TPP abbia effettivamente un prezzo per QUESTA richiesta.
            // prepareForNewElection dovrebbe essere già stato chiamato nel callback MQTT.
            if (!hasPreparedPriceForRequest(requestId)) {
                System.err.println("TPP " + this.id + ": CRITICAL - Trying to start election for '" + requestId +
                        "' but no price prepared for it. Aborting.");
                // Potrebbe essere necessario chiamare di nuovo prepareForNewElection se questo metodo
                // può essere chiamato da percorsi diversi da quello MQTT.
                // In alternativa, la chiamata a prepareForNewElection nel callback MQTT è l'unica fonte.
                return;
            }
            // NetworkManager prenderà il prezzo dalla TPP.
            networkManager.startElection(requestId, kWh);
        } else {
            System.err.println("TPP " + this.id + ": NetworkManager not initialized or TPP not ready! " +
                    "Cannot start election for request '" + requestId + "'.");
        }
    }


    private void startPeriodicDataSender() {
        this.dataSenderScheduler = Executors.newSingleThreadScheduledExecutor();
        System.out.println("TPP " + this.id + ": Starting periodic sender for pollution data.");

        this.dataSenderScheduler.scheduleAtFixedRate(() -> {
            try {
                List<Double> averagesToSend = pollutionBuffer.getComputedAveragesAndClear();

                if (!averagesToSend.isEmpty()) {
                    long currentTimestamp = System.currentTimeMillis();

                    // ----- COSTRUZIONE MANUALE DELLA STRINGA JSON -----
                    StringBuilder sb = new StringBuilder();
                    sb.append("{");
                    sb.append("\"plantId\":").append(this.id).append(","); // plantId è un numero
                    sb.append("\"timestamp\":").append(currentTimestamp).append(",");

                    sb.append("\"averagesCO2\":[");
                    for (int i = 0; i < averagesToSend.size(); i++) {
                        // Formatta il double con il punto come separatore decimale, indipendentemente dal Locale
                        sb.append(String.format(Locale.US, "%.2f", averagesToSend.get(i)));
                        if (i < averagesToSend.size() - 1) {
                            sb.append(",");
                        }
                    }
                    sb.append("]");
                    sb.append("}");
                    String payload = sb.toString();
                    // ----------------------------------------------------

                    if (mqttClient != null && mqttClient.isConnected()) {
                        MqttMessage mqttMsg = new MqttMessage(payload.getBytes());
                        mqttMsg.setQos(0);
                        mqttClient.publish(pollutionDataTopic, mqttMsg);
                        System.out.println("TPP " + this.id + ": Sent " + averagesToSend.size() +
                                " CO2 averages to admin via MQTT. Payload: " + payload.substring(0, Math.min(payload.length(), 100)) + "...");
                    } else {
                        System.err.println("TPP " + this.id + ": MQTT client not connected. Cannot send pollution data.");
                    }
                }
            } catch (Exception e) {
                System.err.println("TPP " + this.id + ": Error in periodic data sender task: " + e.getMessage());
                // e.printStackTrace(); // Considera di stamparlo per debug più approfondito
            }
        }, 10, 10, TimeUnit.SECONDS);
    }


    // Nel metodo di shutdown della pianta:
    public void shutdown() { // Se hai un metodo di shutdown, o implementalo

        if (isShutdownInitiated) {
            System.out.println("TPP " + this.id + ": Shutdown already in progress.");
            return;
        }
        isShutdownInitiated = true;
        System.out.println("TPP " + this.id + ": Shutting down...");

        System.out.println("TPP " + this.id + ": Shutting down...");
        if (sensorSimulator != null) {
            sensorSimulator.stopMeGently(); // Usa il metodo fornito da Simulator
            try {
                sensorSimulator.join(5000); // Attendi che il thread del sensore termini
                if (sensorSimulator.isAlive()) {
                    System.err.println("TPP " + this.id + ": Pollution sensor thread did not terminate cleanly.");
                    sensorSimulator.interrupt(); // Forza l'interruzione se non si ferma
                }
            } catch (InterruptedException e) {
                System.err.println("TPP " + this.id + ": Interrupted while waiting for sensor thread to stop.");
                sensorSimulator.interrupt();
                Thread.currentThread().interrupt();
            }
        }
        if (dataSenderScheduler != null && !dataSenderScheduler.isShutdown()) {
            dataSenderScheduler.shutdownNow();
            // ... (attendi terminazione come prima) ...
        }

        System.out.println("TPP " + this.id + ": Shutdown complete.");
    }


    private void shutdownGrpcServer() {
        if (this.grpcServer != null && !this.grpcServer.isShutdown()) {
            System.out.println("TPP " + this.id + ": Shutting down gRPC server...");
            this.grpcServer.shutdown(); // Inizia la chiusura ordinata
            try {
                // Attendi che il server termini o scada il timeout
                if (!this.grpcServer.awaitTermination(5, TimeUnit.SECONDS)) {
                    System.err.println("TPP " + this.id + ": gRPC server did not terminate in time, forcing shutdown...");
                    this.grpcServer.shutdownNow(); // Forza la chiusura
                    // Attendi di nuovo dopo shutdownNow
                    if (!this.grpcServer.awaitTermination(5, TimeUnit.SECONDS)) {
                        System.err.println("TPP " + this.id + ": gRPC server did not terminate even after shutdownNow.");
                    }
                }
            } catch (InterruptedException e) {
                System.err.println("TPP " + this.id + ": Interrupted while shutting down gRPC server.");
                this.grpcServer.shutdownNow();
                Thread.currentThread().interrupt();
            }
            System.out.println("TPP " + this.id + ": gRPC server shut down.");
        } else if (this.grpcServer != null && this.grpcServer.isShutdown()) {
            System.out.println("TPP " + this.id + ": gRPC server was already shut down.");
        } else {
            System.out.println("TPP " + this.id + ": No gRPC server instance to shut down.");
        }
    }


    public Integer getId() {
        return this.id;
    }

    public String getAddress() {
        return this.address;
    }

    public Integer getPortNumber() {
        return this.portNumber;
    }

    public List<ThermalPowerPlants> getAllPlants() {
        return this.AllPlants;
    }

    public NetworkManager getNetworkManager() {
        return this.networkManager;
    }

    public synchronized PlantServiceGrpc.PlantServiceBlockingStub getCurrentSuccessorStub() {
        // Potresti aggiungere qui logica se lo stub non è valido / il canale è chiuso,
        // ma establishRingConnection dovrebbe mantenerlo consistente.
        return this.successorStub;
    }

    public String toString() {
        return "ID = " + this.getId() + "\nAddress = " + this.getAddress() + "\nPortNumber = " + this.getPortNumber();
    }

    public static ResponseEntity<ThermalPowerPlants[]> postRequest(String url, ThermalPowerPlants dummyPlants) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<ThermalPowerPlants> request = new HttpEntity<>(dummyPlants, headers);

            try {
                return restTemplate.postForEntity(url, request, ThermalPowerPlants[].class);
            } catch (Exception e) {
                System.out.println("Errore nella POST: " + e.getMessage());
                return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isReadyForElection() {
        return this.participantFlagForNM;
    }

    public synchronized String getActivelyParticipatingInElectionForRequestId() {
        return this.activelyParticipatingInElectionForRequestId;
    }

    // Metodo per "pulire" il prezzo quando un'elezione finisce
    public synchronized void clearCurrentPriceInfo() {
        if (this.requestIdForCurrentPrice != null || this.myEnegyValue != NO_VALID_PRICE) {
            System.out.println("TPP " + this.id + ": Clearing price info (was for request '" + this.requestIdForCurrentPrice +
                    "', price " + String.format("%.3f", this.myEnegyValue) + ").");
        }
        this.myEnegyValue = NO_VALID_PRICE;
        this.requestIdForCurrentPrice = null;
    }

    public synchronized boolean hasPreparedPriceForRequest(String requestId) {
        boolean hasPrice = requestId != null &&
                requestId.equals(this.requestIdForCurrentPrice) &&
                this.myEnegyValue != NO_VALID_PRICE;
        return hasPrice;
    }

    public synchronized double getCurrentElectionPrice() {
        return this.myEnegyValue;
    }

    public String getRequestIdForCurrentPrice() {
        return this.requestIdForCurrentPrice;
    }

    public synchronized void markElectionAsConcluded(String requestId) {
        if (requestId == null) return;
        boolean added = this.concludedElectionRequestIds.add(requestId);
        if (added) {
            System.out.println("TPP " + this.id + ": Marked election for request '" + requestId + "' AS CONCLUDED.");
        }
    }

    public synchronized boolean isElectionConcluded(String requestId) {
        if (requestId == null) return false;
        return this.concludedElectionRequestIds.contains(requestId);
    }
}