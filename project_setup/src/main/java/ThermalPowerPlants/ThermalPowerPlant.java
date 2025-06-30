package ThermalPowerPlants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

// Importazioni per il sensore e il buffer
import Simulators.Buffer;
import Simulators.PollutionSensor;
import PollutionManagement.SlidingWindowBuffer;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 * La classe ThermalPowerPlant rappresenta l'entità logica di una centrale termica.
 * Gestisce il proprio stato operativo, la logica di business, il sensore di inquinamento
 * e delega le operazioni di rete al NetManager.
 */
public class ThermalPowerPlant {

    private final int id;
    private final String grpcAddress;
    private final int grpcPort;
    private final PeerInfo selfInfo;

    private final NetManager netManager;
    private final Random priceGenerator;

    private final SlidingWindowBuffer  pollutionBuffer;
    private final PollutionSensor pollutionSensor;
    private final Timer dataSenderTimer;
    private MqttClient mqttAdminClient;
    private final String MQTT_BROKER = "tcp://localhost:1883";
    private final String ADMIN_POLLUTION_TOPIC = "DESM/pollution_stats";

    enum PlantState { STARTING, IDLE, IN_ELECTION, BUSY }
    private volatile PlantState currentState;
    private final Object stateLock = new Object();

    private String currentElectionRequestId;
    private double currentOfferPrice;
    private double currentKwhRequest;


    public ThermalPowerPlant(int id, String grpcAddress, int grpcPort) {
        this.id = id;
        this.grpcAddress = grpcAddress;
        this.grpcPort = grpcPort;

        this.selfInfo = new PeerInfo(id, grpcAddress, grpcPort);

        this.netManager = new NetManager(this);
        this.priceGenerator = new Random();

        this.pollutionBuffer = new SlidingWindowBuffer();
        this.pollutionSensor = new PollutionSensor(this.pollutionBuffer);
        this.dataSenderTimer = new Timer("DataSenderTimer-Plant" + id, true);

        this.currentState = PlantState.STARTING;
    }

    public void start() {
        System.out.println("Plant " + id + ": Initializing...");
        try {
            String mqttClientId = "Plant-" + id + "-AdminClient";
            mqttAdminClient = new MqttClient(MQTT_BROKER, mqttClientId, new MemoryPersistence());
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            mqttAdminClient.connect(connOpts);
            System.out.println("Plant " + id + ": Connected to MQTT Broker for admin communication.");
        } catch (MqttException e) {
            System.err.println("Plant " + id + ": Failed to connect to MQTT broker. Admin data will not be sent.");
            e.printStackTrace();
        }

        this.pollutionSensor.start();
        System.out.println("Plant " + id + ": Pollution sensor started.");

        netManager.start(); // Il NetManager ora gestisce la registrazione con l'admin server

        startScheduledDataSending();
        System.out.println("Plant " + id + ": Scheduled data sending to admin server started.");

        synchronized (stateLock) {
            this.currentState = PlantState.IDLE;
        }
        System.out.println("Plant " + id + ": Startup complete. Current state is IDLE.");
    }

    public void shutdown() {
        System.out.println("Plant " + id + ": Shutting down...");
        this.pollutionSensor.stopMeGently();
        this.dataSenderTimer.cancel();
        this.netManager.shutdown();

        try {
            if (this.mqttAdminClient != null && this.mqttAdminClient.isConnected()) {
                this.mqttAdminClient.disconnect();
            }
        } catch (MqttException e) {
            System.err.println("Plant " + id + ": Error disconnecting MQTT client.");
        }
        System.out.println("Plant " + id + ": Shutdown complete.");
    }

    public void onEnergyRequest(String requestId, double kwh) {
        onJoinElection(requestId, kwh);
        netManager.startElection(requestId, kwh, this.id, this.currentOfferPrice);
    }

    public void onElectionResult(String requestId,  double kwh) {
        synchronized (stateLock) {
            if (currentState == PlantState.IN_ELECTION) {
                System.out.println("INFO (Plant " + id + "): finished partecipation to  " + requestId + ", becomes IDLE");
                setAsIdle();
                return;
            }

            System.out.println("Plant " + id + ": Joining election for request '" + requestId + "'.");
            this.currentKwhRequest = kwh;
            this.currentElectionRequestId = requestId;
            this.currentOfferPrice = generatePrice();
            System.out.println("Plant " + id + ": My offer price is " + String.format("%.2f", currentOfferPrice));
        }
    }

    public void onWinning(String requestId, double kwh) {
        synchronized (stateLock) {
            if (currentState != PlantState.IN_ELECTION) {
                System.out.println("Warning: problem in winning condition " + requestId);
                return;
            }
        }

        currentState = PlantState.BUSY;
        System.out.println("Plant " + id + ": producing for request: " + requestId + "'.");
        this.currentKwhRequest = kwh;
        this.currentElectionRequestId = requestId;

        simulateEnergyProduction(kwh);
    }

    private void simulateEnergyProduction(double kwhToProduce) {

        final long productionTimeMs = Math.round(kwhToProduce * 1.0); // 1 ms per kWh
        this.currentState = PlantState.BUSY;

        new Thread(() -> {
            try {
                System.out.println("Plant " + id + ": Starting energy production for " + String.format("%.0f", kwhToProduce) + " kWh. "
                        + "This will take " + productionTimeMs + " ms (" + String.format("%.2f", productionTimeMs / 1000.0) + " seconds).");
                Thread.sleep(productionTimeMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Plant " + id + ": Energy production was interrupted.");
            } finally {
                synchronized (stateLock) {
                    System.out.println("Plant " + id + ": Production complete. Returning to IDLE state.");
                    this.currentState = PlantState.IDLE;
                    this.currentKwhRequest = 0;
                }
                // Notifica il NetManager che una risorsa si è liberata,
                netManager.onProductionComplete();
            }
        }).start();
    }

    private double generatePrice() {
        return 0.1 + (priceGenerator.nextDouble() * 0.8);
    }

    private void startScheduledDataSending() {
        TimerTask sendingTask = new TimerTask() {
            @Override
            public void run() {
                List<Double> averagesToSend = pollutionBuffer.getComputedAveragesAndClear();

                    if (!averagesToSend.isEmpty()) {
                        System.out.println("\n--- SENDING POLLUTION DATA (Plant " + id + ") ---");

                        String payload = formatDataForMqtt(averagesToSend);
                        System.out.println("LOG (TPP " + id + "): Publishing to topic '" + ADMIN_POLLUTION_TOPIC + "'. Payload: " + payload);

                        try {
                            if (mqttAdminClient != null && mqttAdminClient.isConnected()) {
                                MqttMessage message = new MqttMessage(payload.getBytes());
                                message.setQos(1);
                                mqttAdminClient.publish(ADMIN_POLLUTION_TOPIC, message);
                                System.out.println("LOG (TPP " + id + "): Payload published successfully.");
                            }
                        } catch (MqttException e) {
                            System.err.println("Plant " + id + ": Failed to publish pollution data.");
                        }
                }
            }
        };
        dataSenderTimer.scheduleAtFixedRate(sendingTask, 10000, 10000);
    }

    private String formatDataForMqtt(List<Double> averages) {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"plantId\":").append(this.id).append(",");
        json.append("\"timestamp\":").append(System.currentTimeMillis()).append(",");
        json.append("\"averagesCO2\": [");
        for (int i = 0; i < averages.size(); i++) {
            json.append(String.format("%.2f", averages.get(i)).replace(",", "."));
            if (i < averages.size() - 1) json.append(",");
        }
        json.append("]");
        json.append("}");
        return json.toString();
    }

    // --- GETTERS ---
    public PeerInfo getPeerInfo() { return this.selfInfo; }
    public int getId() { return this.id; }
    public double getCurrentOfferPrice() {
        return this.currentOfferPrice;
    }

    public NetManager getNetManager() { return this.netManager; }

    public boolean isIdle() {
        return this.currentState == PlantState.IDLE;
    }

    public PlantState getCurrentState() {
        return this.currentState;
    }

    public void setAsIdle() {
        synchronized (stateLock) {
            this.currentState = PlantState.IDLE;
        }
        System.out.println("--- Plant " + id + ": Status changed to IDLE. Ready for elections. ---");
    }

    public void onJoinElection(String requestId, double kwh) {
        synchronized(stateLock) {
            if (currentState == PlantState.IDLE) {

                currentState = PlantState.IN_ELECTION;
                this.currentElectionRequestId = requestId;
                this.currentOfferPrice = generatePrice();
                this.currentKwhRequest = kwh; // Salva i kWh
                System.out.println("Plant " + id + ": Generated my offer for ongoing election: " + String.format("%.2f", currentOfferPrice));
            }
        }
    }

    public boolean isProducing(){
        synchronized(stateLock) {
            return this.currentState == PlantState.BUSY;
        }
    }


    // --- MAIN ---
    public static void main(String[] args) {
        BufferedReader inputStream = new BufferedReader(new InputStreamReader(System.in));
        int id;
        String address;
        int port;


        System.out.println("--- Configure New Thermal Power Plant ---");

        try {
            System.out.print("Enter Plant ID: ");
            id = Integer.parseInt(inputStream.readLine());

            System.out.print("Enter gRPC Address: ");
            address = inputStream.readLine();

            System.out.print("Enter gRPC Port: ");
            port = Integer.parseInt(inputStream.readLine());


        } catch (NumberFormatException | IOException e) {
            System.err.println("ERROR: Invalid input. Please enter valid data.");
            e.printStackTrace();
            System.exit(1);
            return;
        }

        System.out.println("\n--- Starting Thermal Power Plant Instance ---");
        System.out.println("  ID: " + id);
        System.out.println("  gRPC Address: " + address + ":" + port);
        System.out.println("-------------------------------------------");

        try {
            ThermalPowerPlant plant = new ThermalPowerPlant(id, address, port);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\nShutdown signal received. Cleaning up resources for Plant " + id + "...");
                plant.shutdown();
                System.out.println("Cleanup for Plant " + id + " complete. Exiting.");
            }));

            // Avvia la logica principale della centrale.
            plant.start();

            System.out.println("\nPlant " + id + " is running. Press Ctrl+C to exit.");
            Thread.currentThread().join();

        } catch (Exception e) {
            System.err.println("An unexpected error occurred during plant startup or operation.");
            e.printStackTrace();
            System.exit(1);
        }
    }


}