package ThermalPowerPlants;

import org.eclipse.paho.client.mqttv3.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import com.example.powerplants.PlantServiceGrpc;
import com.example.powerplants.EnergyRequest;
import com.example.powerplants.EnergyResponse;
import PlantServiceGRPC.PlantServiceImpl;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

public class ThermalPowerPlants {
    private int id;
    private String address;
    private int portNumber;
    private String serverAddress = "http://localhost:8080";

    private Server server;

    private List<ThermalPowerPlants> otherPlants = new ArrayList<>();
    private final Map<Integer, PlantServiceGrpc.PlantServiceBlockingStub> connections = new HashMap<>();
    private Map<Integer, String> topology = new HashMap<>();
    private MqttClient mqttClient;

    private static final RestTemplate restTemplate = new RestTemplate();

    private int pollutionLevel;
    private int availableEnergy;

    public ThermalPowerPlants() {
    }

    public ThermalPowerPlants(int id, String address, int port, String adminAddress) {
        this.id = id;
        this.address = address;
        this.portNumber = port;
        this.serverAddress = adminAddress;
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
        registerToAdminServer();

        // 2. Avvia server gRPC
        startGrpcServer();

        // 3. Connettiti a tutti gli altri impianti via gRPC
        connectToOtherPlants(topology);

        // 4. Inizializza MQTT
        setupMqtt();

        // 5. Subscrivi al topic per ricevere richieste energetiche
        subscribeToEnergyRequests();
    }

    private void registerToAdminServer() throws IOException {
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
        ThermalPowerPlants newPlant = new ThermalPowerPlants(id, address, portNumber, serverAddress);

        //eseguo la chiamata a post
        ResponseEntity<Map<Integer, String>> postResponse = postRequest(serverAddress + postPath, newPlant);

        //Se tutto funziona bene, salvo la lista delle altre piante termali nella nuova pianta termale
        if (postResponse.getStatusCode() == HttpStatus.OK && postResponse.getBody() != null) {
            topology = postResponse.getBody();
        }
    }

    private void startGrpcServer() throws IOException, InterruptedException {
        server = ServerBuilder.forPort(portNumber)
                .addService(new PlantServiceImpl(this))
                .build()
                .start();

        System.out.println("Server gRPC avviato sulla porta " + portNumber);
        server.awaitTermination();
    }

    public void connectToOtherPlants(Map<Integer, String> topology) {
        for (Map.Entry<Integer, String> entry : topology.entrySet()) {
            int otherId = entry.getKey();
            String address = entry.getValue();

            if (otherId == this.id) continue; // non serve connettersi a sé stessi

            String[] hostPort = address.split(":");
            String host = hostPort[0];
            int port = Integer.parseInt(hostPort[1]);

            try {
                ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                        .usePlaintext()
                        .build();

                PlantServiceGrpc.PlantServiceBlockingStub stub = PlantServiceGrpc.newBlockingStub(channel);
                connections.put(otherId, stub);

                System.out.println("Connesso a pianta " + otherId + " (" + address + ")");
            } catch (Exception e) {
                System.err.println("Errore nel connettersi a pianta " + otherId + ": " + e.getMessage());
            }
        }
    }

    private void setupMqtt() {
        // Inizializza MQTT client, connect, setCallback, ecc.
    }

    private void subscribeToEnergyRequests() {
        // Subscrivi a un topic tipo "energy/requests"
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

    public List<ThermalPowerPlants> getOtherPlants() {
        return this.otherPlants;
    }

    public String toString() {
        return "ID = " + this.getId() + "\nAddress = " + this.getAddress() + "\nPortNumber = " + this.getPortNumber();
    }

    public void setPlantsList(List<ThermalPowerPlants> plants) {
        this.otherPlants = plants;
    }

    public static ResponseEntity<Map<Integer, String>> postRequest(String url, ThermalPowerPlants dummyPlants) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<ThermalPowerPlants> request = new HttpEntity<>(dummyPlants, headers);

            // **Uilizzata LLM per risolvere il problema di ricevere una Map<Integer, String> come risposta**
            // Usiamo ParameterizedTypeReference per specificare il tipo generico di Map
            ParameterizedTypeReference<Map<Integer, String>> responseType =
                    new ParameterizedTypeReference<Map<Integer, String>>() {};

            // Usiamo restTemplate.exchange() invece di postForEntity()
            // perché supporta ParameterizedTypeReference per i tipi generici.
            return restTemplate.exchange(url, HttpMethod.POST, request, responseType);
        } catch (Exception e) {
            System.out.println("Server not available: " + e.getMessage());
            return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
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