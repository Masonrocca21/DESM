package ThermalPowerPlants;

import org.eclipse.paho.client.mqttv3.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.TimeUnit;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import com.example.powerplants.PlantServiceGrpc;
import PlantServiceGRPC.PlantServiceImpl;

import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

public class ThermalPowerPlants {
    private int id;
    private String address;
    private int portNumber;
    private String serverAddress = "http://localhost:8080";

    private boolean isActive;

    private Server server;

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

    private int SuccessorID; //ID del sucessore nella topologia ad anello
    boolean participant = false; //Partecipante all'elezione del leader
    double myEnegyValue; //Prezzo in $/Kwh per l'elezione
    private NetworkManager networkManager; //Per gestire l'elezione

    private int pollutionLevel;
    private int availableEnergy;

    public ThermalPowerPlants() {
        this.allPlantsInfoInNetwork = new ArrayList<>();
    }

    public ThermalPowerPlants(int id, String address, int port, String adminAddress) {
        this.id = id;
        this.address = address;
        this.portNumber = port;
        this.serverAddress = adminAddress;

        this.allPlantsInfoInNetwork = new ArrayList<>();
    }

    public static void main(String[] args) {
        ThermalPowerPlants plant = new ThermalPowerPlants();
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
        isActive = registerToAdminServer();

        if (isActive) {
            // 2. Avvia server gRPC
            startGrpcServer();

            System.out.println("Server started");
            // 3. Connettiti a tutti gli altri impianti via gRPC

            // this.allPlantsInfoInNetwork dovrebbe essere popolato da registerToAdminServer()
            if (this.allPlantsInfoInNetwork != null && !this.allPlantsInfoInNetwork.isEmpty()) {
                connectToOtherPlants(this.allPlantsInfoInNetwork);
                System.out.println("Connessione alle altre piante (simulata) con la lista: " + this.allPlantsInfoInNetwork);
            } else {
                System.out.println("Nessuna altra pianta a cui connettersi o lista non popolata.");
            }

            // 4. Inizializza MQTT
            setupMqtt();

            // 5. Subscrivi al topic per ricevere richieste energetiche
            subscribeToEnergyRequests();
        }
    }

    private boolean registerToAdminServer() throws IOException {
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

        //eseguo la chiamata a post
        ResponseEntity<ThermalPowerPlants[]> postResponse = postRequest(serverAddress + postPath, this);

        //Se tutto funziona bene, salvo la lista di tutte le piante termali nella nuova pianta termale
        if (postResponse.getStatusCode() == HttpStatus.OK && postResponse.getBody() != null) {
            ThermalPowerPlants[] plantsArrayFromServer = postResponse.getBody();

            // Trasforma ThermalPowerPlants[] in List<ThermalPowerPlantInfo>
            // e assegnala a this.allPlantsInfoInNetwork
            this.allPlantsInfoInNetwork.clear(); // Pulisci la lista precedente se presente
            for (ThermalPowerPlants plantData : plantsArrayFromServer) {
                this.allPlantsInfoInNetwork.add(new ThermalPowerPlantInfo(plantData.id, plantData.address, plantData.portNumber));
            }

            // Assicurati che la lista sia ordinata per ID, se la logica di anello lo richiede
            // e se l'admin server non garantisce l'ordine.
            // Se l'admin server restituisce già la lista ordinata per ID, questo passaggio
            // potrebbe non essere necessario, ma è più sicuro farlo.
            Collections.sort(this.allPlantsInfoInNetwork, Comparator.comparingInt(ThermalPowerPlantInfo::getId));

            System.out.println("Lista piante memorizzata e ordinata (this.allPlantsInfoInNetwork): " + this.allPlantsInfoInNetwork);
            return true;
        } else {
            System.err.println("Registrazione fallita o risposta non valida dall'admin server. Status: " + postResponse.getStatusCode());
            // Lascia this.allPlantsInfoInNetwork vuota o gestisci l'errore
            this.allPlantsInfoInNetwork.clear();
            return false;
        }
    } //FUNZIONA

    private void startGrpcServer() throws IOException, InterruptedException {
        server = ServerBuilder.forPort(portNumber)
                .addService(new PlantServiceImpl(this))
                .build()
                .start();

        System.out.println("Server gRPC avviato sulla porta " + portNumber);
    } //FUNZIONA

    public void connectToOtherPlants(List<ThermalPowerPlantInfo> allPlantsInfoSortedById) {
        System.out.println("TPP " + this.id + " is establishing its ring connection based on " + allPlantsInfoSortedById.size() + " plants.");
        establishRingConnection(allPlantsInfoSortedById);
        System.out.println("TPP " + this.id + ": Finished establishing ring connection. My successor stub is: " + this.successorStub);
        networkManager = new NetworkManager(id, this.myEnegyValue, this.successorStub, this.successorChannel);
    }

    // Metodo chiamato dopo che la pianta si è registrata e ha ricevuto la lista
    // di TUTTE le piante attualmente nella rete (inclusa sé stessa).
    // La lista è già ordinata per ID.
    public void establishRingConnection(List<ThermalPowerPlantInfo> allPlantsSortedById) {
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
        }
        System.out.println("TPP " + this.id + ": Ring connection setup/update complete. My successor stub for NetworkManager: " + (this.successorStub != null));
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

                double requiredEnergy = Double.parseDouble(parts[0]);
                System.out.println(Arrays.toString(parts));

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
            networkManager.startElection();
        } else {
            System.err.println("NetworkManager non inizializzato!");
        }
    }

    private void publishPollutionData() {
        // Pubblica ogni tot secondi su topic "pollution/plant{ID}"
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

    public int getPollutionLevel() {
        return pollutionLevel;
    }

    public int getAvailableEnergy() {
        return availableEnergy;
    }

    public boolean canHandle(int requiredEnergy) {
        return availableEnergy >= requiredEnergy;
    }
}