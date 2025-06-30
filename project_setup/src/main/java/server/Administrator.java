package server;

import java.util.*;

import ThermalPowerPlants.PendingRequest;
import ThermalPowerPlants.PeerInfo;

import org.springframework.stereotype.Service;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import javax.annotation.PostConstruct;

import org.json.JSONArray;
import org.json.JSONObject;



@Service //Spring automatically makes this class a singleton bean
public class Administrator {

    private final List<PeerInfo> registeredPlantsInfo = new ArrayList<>();

    private final Map<Integer, List<AdminPollutionEntry>> pollutionDataByPlantId = new HashMap<>();
    private final Object pollutionDataLock = new Object();
    private MqttClient mqttClient;
    private final String MQTT_BROKER = "tcp://localhost:1883"; // Configura secondo necessità
    private final String POLLUTION_TOPIC = "DESM/pollution_stats"; // Deve corrispondere a quello usato dalle piante
    private final String ENERGY_REQUEST_TOPIC = "home/renewableEnergyProvider/power/new";

    private final LinkedList<PendingRequest> requestQueue = new LinkedList<>();
    private final Object queueLock = new Object(); // Lock dedicato per la coda

    // Struttura per le statistiche di inquinamento
    static class AdminPollutionEntry {
        final long submissionTimestamp;
        final List<Double> co2Averages;
        public AdminPollutionEntry(long submissionTimestamp, List<Double> co2Averages) {
            this.submissionTimestamp = submissionTimestamp;
            this.co2Averages = Collections.unmodifiableList(new ArrayList<>(co2Averages));
        }
        public long getSubmissionTimestamp() { return submissionTimestamp; }
        public List<Double> getCo2Averages() { return co2Averages; }
    }


    public Administrator() {
    }

    // Spring chiamerà questo metodo dopo l'inizializzazione del bean
    @PostConstruct
    public void initializeMqttListener() {
        setupMqttListener();
    }

    private void setupMqttListener() {
        try {
            String clientId = "AdminServer-PollutionMonitor-" + UUID.randomUUID();
            mqttClient = new MqttClient(MQTT_BROKER, clientId, new MemoryPersistence());
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);

            System.out.println("AdminServer: Connecting to MQTT broker: " + MQTT_BROKER);
            mqttClient.connect(connOpts);
            System.out.println("AdminServer: Connected to MQTT broker.");

            mqttClient.setCallback(new MqttCallbackExtended() {
                @Override
                public void connectComplete(boolean reconnect, String serverURI) {
                    System.out.println("AdminServer: MQTT connectComplete (reconnect=" + reconnect + "), subscribing to " + POLLUTION_TOPIC);
                    subscribeToPollutionTopic();
                    System.out.println("AdminServer: MQTT connectComplete (reconnect=" + reconnect + "), subscribing to " + ENERGY_REQUEST_TOPIC);
                    subscribeToQueueTopic();
                }

                @Override
                public void connectionLost(Throwable cause) {
                    System.err.println("AdminServer: MQTT connection lost! Cause: " + (cause != null ? cause.getMessage() : "Unknown"));
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) {
                    System.out.println("AdminServer: MQTT received message: " + new String(message.getPayload()));
                    String payloadStr = new String(message.getPayload()); // Ottieni la stringa JSON completa
                    System.out.println("AdminServer (Callback): Received MQTT on '" + topic + "': " + payloadStr.substring(0, Math.min(150, payloadStr.length())) + "...");

                    try {
                        JSONObject payloadJson = new JSONObject(payloadStr);

                        int plantId = -1;
                        long submissionTimestamp = -1L;
                        List<Double> averages = new ArrayList<>();

                        if (payloadJson.has("plantId")) {
                            plantId = payloadJson.getInt("plantId");
                        } else {
                            System.err.println("AdminServer: Payload missing 'plantId'");
                            return;
                        }


                        if (payloadJson.has("timestamp")) {
                            submissionTimestamp = payloadJson.getLong("timestamp");
                        } else {
                            System.err.println("AdminServer: Payload missing 'timestamp'");
                            return;
                        }


                        if (payloadJson.has("averagesCO2")) {
                            JSONArray averagesJsonArray = payloadJson.getJSONArray("averagesCO2");

                            for (int i = 0; i < averagesJsonArray.length(); i++) {
                                try {
                                    averages.add(averagesJsonArray.getDouble(i));
                                } catch (org.json.JSONException e_num) {
                                    try {
                                        averages.add(Double.parseDouble(averagesJsonArray.getString(i)));
                                    } catch (NumberFormatException e_str) {
                                        System.err.println("AdminServer: Could not parse average value from JSONArray: '" + averagesJsonArray.get(i) + "' - " + e_str.getMessage());
                                    }
                                }
                            }
                        } else {
                            System.err.println("AdminServer: Payload missing 'averagesCO2' array.");
                        }
                        // --------------------------------------------------------------------------

                        if (plantId != -1 && submissionTimestamp != -1L) {
                            recordPollutionData(plantId, submissionTimestamp, averages);
                        } else {
                            System.err.println("AdminServer: Critical data (plantId or timestamp) missing after parsing. Payload: " + payloadStr);
                        }

                        recordPollutionData(plantId, submissionTimestamp, averages); // Questa chiamata è ora thread-safe

                    } catch (org.json.JSONException e_json) {
                        System.err.println("AdminServer: Error parsing MAIN JSON payload: '" + payloadStr + "' - " + e_json.getMessage());
                    } catch (Exception e) {
                        System.err.println("AdminServer: Unexpected error processing MQTT message: '" + payloadStr + "'");
                        e.printStackTrace();
                    }
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                }
            });
            // La sottoscrizione avviene in connectComplete per gestire le riconnessioni
            // ma se siamo già connessi, sottoscriviamo subito.
            if (mqttClient.isConnected()) {
                subscribeToPollutionTopic();
            }

        } catch (MqttException me) {
            System.err.println("AdminServer: MQTT Setup Error - Reason: " + me.getReasonCode() +
                    ", Message: " + me.getMessage() +
                    ", LocalizedMsg: " + me.getLocalizedMessage() +
                    ", Cause: " + (me.getCause() != null ? me.getCause().toString() : "null"));
            me.printStackTrace();
        }
    }

    private void subscribeToPollutionTopic() {
        try {
            if (mqttClient != null && mqttClient.isConnected()) {
                mqttClient.subscribe(POLLUTION_TOPIC, 1);
                System.out.println("AdminServer: Successfully subscribed to topic '" + POLLUTION_TOPIC + "'");
            } else {
                System.err.println("AdminServer: Cannot subscribe to topic '" + POLLUTION_TOPIC + "', MQTT client not connected.");
            }
        } catch (MqttException e) {
            System.err.println("AdminServer: Error subscribing to topic '" + POLLUTION_TOPIC + "': " + e.getMessage());
        }
    }

    private void subscribeToQueueTopic(){
        try {
            if (mqttClient != null && mqttClient.isConnected()) {
                // Sottoscrizione al topic esistente dell'inquinamento
                mqttClient.subscribe(POLLUTION_TOPIC, 1);
                System.out.println("AdminServer: Successfully subscribed to topic '" + POLLUTION_TOPIC + "'");

                // *** NUOVA SOTTOSCRIZIONE ***
                mqttClient.subscribe(ENERGY_REQUEST_TOPIC, 1, (topic, message) -> {
                    // Gestore per le richieste di energia
                    String payload = new String(message.getPayload());
                    System.out.println("AdminServer: Received ENERGY REQUEST on '" + topic + "': " + payload);
                    try {
                        JSONObject requestJson = new JSONObject(payload);
                        String requestId = requestJson.getString("requestId");
                        double kwh = requestJson.getDouble("kWh");

                        // Chiama il nuovo metodo per accodare la richiesta
                        enqueueNewRequest(requestId, kwh);

                    } catch (Exception e) {
                        System.err.println("AdminServer: Failed to parse energy request payload. Error: " + e.getMessage());
                    }
                });
                System.out.println("AdminServer: Successfully subscribed to topic '" + ENERGY_REQUEST_TOPIC + "'");

            }
        } catch (MqttException e) {
            throw new RuntimeException(e);
        }
    }


    private void enqueueNewRequest(String requestId, double kwh) {
        synchronized (queueLock) {
            // Potresti controllare se la richiesta è già in coda per evitare duplicati
            boolean alreadyExists = requestQueue.stream().anyMatch(req -> req.getRequestId().equals(requestId));
            if (!alreadyExists) {
                requestQueue.add(new PendingRequest(requestId, kwh));
                System.out.println("AdminServer: Enqueued request " + requestId + ". Queue size is now: " + requestQueue.size());
            } else {
                System.out.println("AdminServer: Ignoring duplicate request " + requestId);
            }
        }
    }

    public PendingRequest getNextWork() {
        synchronized (queueLock) {
            if (requestQueue.isEmpty()) {
                return null; // Niente lavoro da fare
            }
            // Rimuove e restituisce il primo elemento della coda
            PendingRequest work = requestQueue.poll();
            System.out.println("AdminServer: Assigning work for request " + work.getRequestId() + ". Queue size remaining: " + requestQueue.size());
            return work;
        }
    }


    // Metodo per il listener MQTT per aggiungere dati di inquinamento
    public void recordPollutionData(int plantId, long submissionTimestamp, List<Double> averages) {
        if (averages == null || averages.isEmpty()) {
            System.out.println("AdminServer: No averages to record for plant " + plantId);
            return;
        }
        synchronized (pollutionDataLock) {
            pollutionDataByPlantId
                    .computeIfAbsent(plantId, k -> new ArrayList<>())
                    .add(new AdminPollutionEntry(submissionTimestamp, averages));
        }
            System.out.println("AdminServer: Recorded " + averages.size() + " pollution averages for plant " + plantId +
                    " (submitted at " + submissionTimestamp + ")");
    }

    public List<PeerInfo> addPlant(PeerInfo newPlantInfo) {
        synchronized (this) {
            // Controlla se esiste già una pianta con lo stesso ID
            for (PeerInfo pInfo : registeredPlantsInfo) {
                if (pInfo.getId() == newPlantInfo.getId()) {
                    System.out.println("AdminServer: Plant with ID " + newPlantInfo.getId() + " already registered.");
                    return null; // Indica conflitto o pianta già presente
                }
            }

            registeredPlantsInfo.add(newPlantInfo);
            // Ordina se la logica di anello della TPP si basa su una lista ordinata ricevuta
            registeredPlantsInfo.sort(Comparator.comparingInt(PeerInfo::getId));
            System.out.println("AdminServer: Added plant info: " + newPlantInfo + ". Total plants: " + registeredPlantsInfo.size());

            return new ArrayList<>(registeredPlantsInfo); // Restituisce la lista completa
        }
    }

    public List<PeerInfo> getAllRegisteredPlants() { // Per l'endpoint /getList
        synchronized (this) {
            if (registeredPlantsInfo.isEmpty()) {
                return Collections.emptyList();
            }
            return new ArrayList<>(registeredPlantsInfo);
        }
    }

    public Map<String, Object> getAveragePollutionBetweenAsMap(long t1_ms, long t2_ms) {
        double totalCo2Sum = 0;
        int individualAveragesCount = 0;
        Map<String, Object> responseMap = new HashMap<>();

        synchronized (pollutionDataLock) { // Usa il lock dedicato
            for (List<AdminPollutionEntry> entriesForOnePlant : pollutionDataByPlantId.values()) {
                for (AdminPollutionEntry entry : entriesForOnePlant) {
                    if (entry.getSubmissionTimestamp() >= t1_ms && entry.getSubmissionTimestamp() <= t2_ms) {
                        for (Double singleAvgCo2 : entry.getCo2Averages()) {
                            totalCo2Sum += singleAvgCo2;
                            individualAveragesCount++;
                        }
                    }
                }
            }
        }
        // --------------------

        if (individualAveragesCount == 0) {
            responseMap.put("averageCo2", 0.0);
            responseMap.put("readingsCount", 0);
        } else {
            double overallAverage = totalCo2Sum / individualAveragesCount;
            responseMap.put("averageCo2", overallAverage);
            responseMap.put("readingsCount", individualAveragesCount);
        }
        System.out.println("AdminServer: Calculated overall CO2 average for range [" + t1_ms + "," + t2_ms + "]: " + responseMap);
        return responseMap;
    }
}
