package ThermalPowerPlants;

import com.example.powerplants.Ack;
import org.eclipse.paho.client.mqttv3.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
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

import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

public class ThermalPowerPlants {
    private int id;
    private String address;
    private int portNumber;
    private String serverAddress = "http://localhost:8080";
    private Server grpcServer;
    private volatile boolean isShutdownInitiated = false; // Per evitare chiamate multiple a shutdown


    private boolean isActive;

    private volatile boolean participantFlagForNM = false; //Per capire se può partecipare alle elezioni

    private List<ThermalPowerPlants> AllPlants = new ArrayList<>();
    private List<ThermalPowerPlantInfo> allPlantsInfoInNetwork; // ThermalPowerPlantInfo è una classe/record semplice con id, address, port
    private final Map<Integer, PlantServiceGrpc.PlantServiceBlockingStub> connections = new HashMap<>();
    private Map<Integer, ManagedChannel> channels = new HashMap<>();
    public ManagedChannel successorChannel;
    private  PlantServiceGrpc.PlantServiceBlockingStub successorStub;
    private Map<Integer, String> topology = new HashMap<>();
    private MqttClient mqttClient;
    private static String broker = "tcp://localhost:1883";
    private final String energyRequestTopic = "home/renewableEnergyProvider/power";

    private static final RestTemplate restTemplate = new RestTemplate();

    private double myEnegyValue; //Prezzo in $/Kwh per l'elezione
    private double energyRequest;
    private NetworkManager networkManager; //Per gestire l'elezione
    private volatile boolean isCurrentlyBusy = false; // volatile per visibilità tra thread
    private volatile long productionEndsAtMillis = 0L;  // volatile per visibilità

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

            synchronized(this) {
                this.allPlantsInfoInNetwork.clear();
                for (ThermalPowerPlants plantData : plantsArrayFromServer) {
                    this.allPlantsInfoInNetwork.add(new ThermalPowerPlantInfo(plantData.getId(), plantData.getAddress(), plantData.getPortNumber()));
                }
                // Assicurati che la pianta corrente sia nella sua lista, se l'admin non l'ha inclusa
                boolean selfInList = false;
                for(ThermalPowerPlantInfo p : this.allPlantsInfoInNetwork) {
                    if(p.getId() == this.id) {
                        selfInList = true;
                        break;
                    }
                }
                if(!selfInList) {
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
                Ack ack = stub.withDeadlineAfter(5, TimeUnit.SECONDS).announcePresence(myInfoMessage);
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
        this.participantFlagForNM = true;
        System.out.println("SuccStub dopo estabilisehRingConnection: " + this.successorStub);
        System.out.println("TPP " + this.id + ": Finished establishing ring connection. My successor stub is: " + this.successorStub);
        networkManager = new NetworkManager(this, id, this.myEnegyValue, this.successorStub, this.successorChannel);
        System.out.println("TPP " + this.id + ": NetworkManager instance: " + System.identityHashCode(this.networkManager));
    }

    // Metodo chiamato dopo che la pianta si è registrata e ha ricevuto la lista
    // di TUTTE le piante attualmente nella rete (inclusa sé stessa).
    // La lista è già ordinata per ID.
    public synchronized void establishRingConnection(List<ThermalPowerPlantInfo> allPlantsSortedById) {
        this.allPlantsInfoInNetwork = new ArrayList<>(allPlantsSortedById); // Salva una copia

        if (this.allPlantsInfoInNetwork == null || this.allPlantsInfoInNetwork.isEmpty()) {
            System.err.println("TPP " + this.id + ": Plant list is empty, cannot establish ring.");
            return;
        }

        if (this.allPlantsInfoInNetwork.size() == 1 && this.allPlantsInfoInNetwork.get(0).getId() == this.id) {
            System.out.println("TPP " + this.id + ": I am the only plant in the network. No successor.");
            this.successorChannel = null;
            this.successorStub = null;
            // Inizializza NetworkManager, se necessario, sapendo che non c'è successore
            // networkManager = new NetworkManager(this.id, this.myEnergyValue, null, null);
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
                return;
            }
            // Se network size è 1, è già gestito sopra (nessun successore)
        }


        System.out.println("TPP " + this.id + ": My successor is " + successorInfo.getId() +
                " at " + successorInfo.getAddress() + ":" + successorInfo.getPortNumber());

        // Ottieni le informazioni del nuovo successore
        ThermalPowerPlantInfo newSuccessorInfo = this.allPlantsInfoInNetwork.get(successorIndex);
        int newSuccessorId = (newSuccessorInfo != null) ? newSuccessorInfo.getId() : -1; // o un valore sentinella se null

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
        } else if (newSuccessorInfo != null && (newSuccessorInfo.getId() == this.id) && this.allPlantsInfoInNetwork.size() == 1){
            System.out.println("TPP " + this.id + ": I am the only plant. No successor.");
            // newManagedChannelForSuccessor e newStubForSuccessor rimangono null
        } else if (newSuccessorInfo == null) {
            System.out.println("TPP " + this.id + ": No successor found (e.g., empty list or error).");
        }


        // Aggiorna i membri di ThermalPowerPlant
        this.successorChannel = newManagedChannelForSuccessor;
        this.successorStub = newStubForSuccessor;

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
            // NetworkManager esiste già, aggiorna il suo successore
            this.networkManager.updateSuccessor(
                    this.successorStub,
                    this.successorChannel
            );
            System.out.println("TPP " + this.id + ": NetworkManager successor updated.");
            System.out.println("TPP " + this.id + ": NetworkManager instance: " + System.identityHashCode(this.networkManager));
        }
        System.out.println("TPP " + this.id + ": Ring connection setup/update complete. My successor stub for NetworkManager: " + (this.successorStub != null));
    }

    /**
     * Controlla se la pianta è attualmente occupata a produrre energia.
     * Se la produzione è terminata, aggiorna lo stato a non occupato.
     * Questo metodo è thread-safe usando la sincronizzazione sul metodo stesso.
     * @return true se la pianta è occupata, false altrimenti.
     */
    public synchronized boolean isBusyProducing() { // Metodo Sincronizzato
        if (isCurrentlyBusy) {
            if (System.currentTimeMillis() >= productionEndsAtMillis) {
                System.out.println("TPP " + this.id + ": Production time ended. Releasing busy state.");
                isCurrentlyBusy = false;
                productionEndsAtMillis = 0L;
                return false;
            }
            return true;
        }
        return false;
    }

    /**
     * Metodo chiamato quando QUESTA pianta vince un'elezione e deve iniziare la produzione.
     * Imposta lo stato della pianta a "occupato" per la durata calcolata.
     * Questo metodo è thread-safe usando la sincronizzazione sul metodo stesso.
     * @param kWhToProduce La quantità di kWh da produrre.
     */
    public synchronized void handleElectionWinAndStartProduction(double kWhToProduce) { // Metodo Sincronizzato
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

        isCurrentlyBusy = true;
        productionEndsAtMillis = System.currentTimeMillis() + productionDurationMs;

        System.out.println("TPP " + this.id + ": WON ELECTION for request '" +  "'. " +
                "Starting production of " + kWhToProduce + " kWh. " +
                "Will be busy for " + productionDurationMs + " ms (until " +
                new Date(productionEndsAtMillis) + ").");

        // Avvia un thread separato per "sbloccare" lo stato busy dopo la durata.
        Thread productionTimerThread = new Thread(() -> {
            try {
                Thread.sleep(productionDurationMs);
            } catch (InterruptedException e) {
                System.err.println("TPP " + this.id + ": Production timer for request " +  " interrupted.");
                Thread.currentThread().interrupt();
            } finally {
                // Reset proattivo dello stato. Deve essere sincronizzato se accede
                // alle stesse variabili di isBusyProducing.
                synchronized (this) { // Sincronizza sul lock intrinseco dell'oggetto ThermalPowerPlants
                    // Controlla se questa era ancora la produzione che doveva finire ora
                    // E se il productionEndsAtMillis non è stato sovrascritto
                    if (isCurrentlyBusy && System.currentTimeMillis() >= productionEndsAtMillis &&
                            productionEndsAtMillis != 0L ) { // Aggiunto controllo per productionEndsAtMillis != 0L
                        // Per essere sicuri, potremmo voler controllare se productionEndsAtMillis è esattamente
                        // quello che ci aspettavamo da QUESTA produzione, ma diventa più complesso.
                        // Il controllo temporale è di solito sufficiente.
                        System.out.println("TPP " + this.id + ": Production timer thread for request " + " finished. Releasing busy state.");
                        isCurrentlyBusy = false;
                        productionEndsAtMillis = 0L;
                    }
                }
            }
        });
        productionTimerThread.setName("ProductionTimer-" + this.id );
        productionTimerThread.setDaemon(true);
        productionTimerThread.start();
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
        // Subscrivi a un topic tipo "energy/requests"
        try {
            mqttClient.subscribe(energyRequestTopic, (topic, message) -> {
                String payload = new String(message.getPayload());
                System.out.println("Richiesta energia ricevuta: " + payload);

                // Parsing semplificato (JSON → oggetto). Usa una libreria come Jackson o manualmente
                String[] parts = payload.replace("{", "").replace("}", "").replace("\"", "").split(",");
                System.out.println(Arrays.toString(parts));

                this.energyRequest = Double.parseDouble(parts[0]);
                this.networkManager.setCurrentElectionKWh(this.energyRequest);

                System.out.println(Arrays.toString(parts));

                if (isBusyProducing()) { // isBusyProducing() è già synchronized
                    System.out.println("TPP " + this.id + ": IGNORANDO nuova richiesta energia '" + this.energyRequest +
                            "' perché attualmente occupato a produrre.");
                    return; // Esci dal callback, non partecipare all'elezione
                }

                this.myEnegyValue = 0.1 + (0.8 * new Random().nextDouble());
                networkManager.setValue(this.myEnegyValue);  //Genero il prezzo random
                System.out.println("Prezzo assegnato alla pianta di id " + this.id + ": " + this.myEnegyValue);
                handleIncomingEnergyRequest();
            });
            System.out.println("Sottoscritto al topic " + energyRequestTopic);

        } catch (MqttException e) {
            System.err.println("Errore nella sottoscrizione MQTT: " + e.getMessage());
        }
    }

    public void handleIncomingEnergyRequest() {
        System.out.println("Ricevuta richiesta energia MQTT (o gRPC) → inizio elezione");

        if (networkManager != null) {
            networkManager.startElection(this.energyRequest);
        } else {
            System.err.println("NetworkManager non inizializzato!");
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
        // ... (chiudi server gRPC, client MQTT come prima) ...
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
        } else if (this.grpcServer != null && this.grpcServer.isShutdown()){
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

    public int[] getPollution() {
        return new int[]{1, 2};
    }

    public List<ThermalPowerPlants> getAllPlants() {
        return this.AllPlants;
    }

    public NetworkManager getNetworkManager() {
        return this.networkManager;
    }

    public void setAllPlants(List<ThermalPowerPlants> allPlants) {
        this.AllPlants = allPlants;
    }

    public void setSuccessorStub(PlantServiceGrpc.PlantServiceBlockingStub successorStub) {
        this.successorStub = successorStub;
    }

    public void addLocalPlantSorted(ThermalPowerPlants plants) {
        this.AllPlants.add(plants);
        this.AllPlants.sort(Comparator.comparing(ThermalPowerPlants::getId));
    }

    public String toString() {
        return "ID = " + this.getId() + "\nAddress = " + this.getAddress() + "\nPortNumber = " + this.getPortNumber();
    }

    public ManagedChannel getSuccessorChannel() {
        return this.successorChannel;
    }

    public PlantServiceGrpc.PlantServiceBlockingStub getSuccessorStub() {
        return this.successorStub;
    }
    public Map<Integer, String> getTopology(){
        return this.topology;
    }

    public double getMyValue() {
        return myEnegyValue;
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

    public int getAvailableEnergy() {
        return availableEnergy;
    }

    public boolean canHandle(int requiredEnergy) {
        return availableEnergy >= requiredEnergy;
    }

    public boolean isReadyForElection() {
        return this.participantFlagForNM;
    }
}