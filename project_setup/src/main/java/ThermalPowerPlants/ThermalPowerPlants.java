package ThermalPowerPlants;

import org.eclipse.paho.client.mqttv3.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.grpc.ServerBuilder;
import org.springframework.http.*;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

public class ThermalPowerPlants {
    private int id;
    private String address;
    private int portNumber;
    private String serverAddress = "http://localhost:8080";

    private List<ThermalPowerPlants> otherPlants = new ArrayList<>();
    private MqttClient mqttClient;

    private static final RestTemplate restTemplate = new RestTemplate();

    public ThermalPowerPlants() {
    }

    public ThermalPowerPlants(int id, String address, int port, String adminAddress) {
        this.id = id;
        this.address = address;
        this.portNumber = port;
        this.serverAddress = adminAddress;
    }

    public static void main(String[] args) throws IOException {
        ThermalPowerPlants plant = new ThermalPowerPlants();
        plant.start();
    }


    public void start() throws IOException {
        // 1. Registrati all'amministratore
        registerToAdminServer();

        // 2. Avvia server gRPC
        startGrpcServer();

        // 3. Connettiti a tutti gli altri impianti via gRPC
        connectToOtherPlants();

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
        ResponseEntity<ThermalPowerPlants[]> postResponse = postRequest(serverAddress + postPath, newPlant);

        //Se tutto funziona bene, salvo la lista delle altre piante termali nella nuova pianta termale
        if (postResponse.getStatusCode() == HttpStatus.OK && postResponse.getBody() != null) {
            ThermalPowerPlants[] otherPlants = postResponse.getBody();
            newPlant.setPlantsList(Arrays.asList(otherPlants));
        }
    }

    /*  private void startGrpcServer() {
          // Avvia server gRPC con il tuo handler (implementazione dei metodi)
          try {
              io.grpc.Server server = ServerBuilder.forPort(portNumber)
                      .addService(new PlantServiceImpl(this))
                      .build()
                      .start();
              System.out.println("gRPC server started at " + portNumber);
              server.awaitTermination();
          } catch (Exception e) {
              e.printStackTrace();
          }
      }
  */
    private void startGrpcServer() {
    }

    private void connectToOtherPlants() {
        // Per ogni impianto nella lista, crea un gRPC stub client
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

    public static ResponseEntity<ThermalPowerPlants[]> postRequest(String url, ThermalPowerPlants dummyPlants) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<ThermalPowerPlants> request = new HttpEntity<>(dummyPlants, headers);
            return restTemplate.postForEntity(url, request, ThermalPowerPlants[].class);
        } catch (Exception e) {
            System.out.println("Server not available: " + e.getMessage());
            return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
        }
    }
}