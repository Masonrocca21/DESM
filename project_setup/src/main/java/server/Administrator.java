package server;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import ThermalPowerPlants.ThermalPowerPlants;
import client.PollutionStatsResponse;


import org.springframework.stereotype.Service;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence; // Aggiunto

import javax.annotation.PostConstruct;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;


@Service //Spring automatically makes this class a singleton bean
public class Administrator {

    private final List<ThermalPowerPlants> ThermalPlants = new ArrayList<ThermalPowerPlants>();
    private HashMap<Integer, String> informations = new HashMap<>();

    // Chiave: Plant ID, Valore: Lista di entry, dove ogni entry è un batch di medie inviate
    private final Map<Integer, List<AdminPollutionEntry>> pollutionDataByPlantId = new ConcurrentHashMap<>();

    private MqttClient mqttClient;
    private final String MQTT_BROKER = "tcp://localhost:1883"; // Configura secondo necessità
    private final String POLLUTION_TOPIC = "DESM/pollution_stats"; // Deve corrispondere a quello usato dalle piante

    public Administrator() {
        // L'inizializzazione del listener MQTT avverrà tramite @PostConstruct
    }

    // Spring chiamerà questo metodo dopo l'inizializzazione del bean
    @PostConstruct
    public void initializeMqttListener() {
        setupMqttListener();
    }

    private void setupMqttListener() {
        try {
            String clientId = "AdminServer-PollutionMonitor-" + UUID.randomUUID().toString();
            mqttClient = new MqttClient(MQTT_BROKER, clientId, new MemoryPersistence()); // Aggiunto MemoryPersistence
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            // connOpts.setAutomaticReconnect(true); // Considera per robustezza

            System.out.println("AdminServer: Connecting to MQTT broker: " + MQTT_BROKER);
            mqttClient.connect(connOpts);
            System.out.println("AdminServer: Connected to MQTT broker.");

            mqttClient.setCallback(new MqttCallbackExtended() {
                @Override
                public void connectComplete(boolean reconnect, String serverURI) {
                    System.out.println("AdminServer: MQTT connectComplete (reconnect=" + reconnect + "), subscribing to " + POLLUTION_TOPIC);
                    subscribeToPollutionTopic();
                }

                @Override
                public void connectionLost(Throwable cause) {
                    System.err.println("AdminServer: MQTT connection lost! Cause: " + (cause != null ? cause.getMessage() : "Unknown"));
                    // Qui potresti implementare logica di riconnessione se setAutomaticReconnect non è sufficiente o non usato.
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    System.out.println("AdminServer: MQTT received message: " + new String(message.getPayload()));
                    String payloadStr = new String(message.getPayload()); // Ottieni la stringa JSON completa

                    try {
                        // ----- USA DIRETTAMENTE org.json.JSONObject PER PARSARE L'INTERA STRINGA -----
                        JSONObject payloadJson = new JSONObject(payloadStr);

                        int plantId = -1;
                        long submissionTimestamp = -1L;
                        List<Double> averages = new ArrayList<>();

                        if (payloadJson.has("plantId")) {
                            plantId = payloadJson.getInt("plantId");
                        } else {
                            System.err.println("AdminServer: Payload missing 'plantId'");
                            return; // O gestisci l'errore diversamente
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
                                    // JSONArray.getDouble() gestisce correttamente i numeri
                                    averages.add(averagesJsonArray.getDouble(i));
                                } catch (org.json.JSONException e_num) {
                                    // Fallback se per qualche motivo fossero stringhe nell'array JSON,
                                    // anche se il tuo codice client li invia come numeri formattati.
                                    try {
                                        averages.add(Double.parseDouble(averagesJsonArray.getString(i)));
                                    } catch (NumberFormatException e_str) {
                                        System.err.println("AdminServer: Could not parse average value from JSONArray: '" + averagesJsonArray.get(i) + "' - " + e_str.getMessage());
                                    }
                                }
                            }
                        } else {
                            System.err.println("AdminServer: Payload missing 'averagesCO2' array.");
                            // Potrebbe essere un messaggio valido senza medie, gestisci come appropriato
                        }
                        // --------------------------------------------------------------------------

                        if (plantId != -1 && submissionTimestamp != -1L) { // Controlla che i campi chiave siano stati parsati
                            recordPollutionData(plantId, submissionTimestamp, averages);
                        } else {
                            System.err.println("AdminServer: Critical data (plantId or timestamp) missing after parsing. Payload: " + payloadStr);
                        }

                    } catch (org.json.JSONException e_json) { // Cattura specificamente le eccezioni di parsing JSON
                        System.err.println("AdminServer: Error parsing MAIN JSON payload: '" + payloadStr + "' - " + e_json.getMessage());
                        // e_json.printStackTrace(); // Utile per debug
                    } catch (Exception e) { // Cattura altre eccezioni impreviste
                        System.err.println("AdminServer: Unexpected error processing MQTT message: '" + payloadStr + "'");
                        e.printStackTrace();
                    }
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    // Non rilevante per un subscriber
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
                mqttClient.subscribe(POLLUTION_TOPIC, 1); // QoS 1 o 2 per maggiore affidabilità
                System.out.println("AdminServer: Successfully subscribed to topic '" + POLLUTION_TOPIC + "'");
            } else {
                System.err.println("AdminServer: Cannot subscribe to topic '" + POLLUTION_TOPIC + "', MQTT client not connected.");
            }
        } catch (MqttException e) {
            System.err.println("AdminServer: Error subscribing to topic '" + POLLUTION_TOPIC + "': " + e.getMessage());
        }
    }


    // Metodo per il listener MQTT per aggiungere dati di inquinamento
    public void recordPollutionData(int plantId, long submissionTimestamp, List<Double> averages) {
        if (averages == null || averages.isEmpty()) {
            System.out.println("AdminServer: No averages to record for plant " + plantId);
            return;
        }
        // computeIfAbsent è thread-safe per ConcurrentHashMap per quanto riguarda l'aggiunta della chiave.
        // L'aggiunta alla lista è sicura qui perché ogni chiamata a recordPollutionData
        // aggiunge una nuova AdminPollutionEntry a una lista potenzialmente nuova o esistente.
        // Le liste stesse non sono modificate concorrentemente da più chiamate a recordPollutionData
        // per la *stessa* lista in modo da causare problemi (una nuova entry viene solo appesa).
        pollutionDataByPlantId.computeIfAbsent(plantId, k -> new ArrayList<>()) // ArrayList non è thread-safe di per sé,
                // ma qui l'operazione composta è protetta
                // da computeIfAbsent e l'aggiunta è singola.
                // Per maggiore sicurezza, potresti usare
                // una CopyOnWriteArrayList o sincronizzare
                // l'accesso alla lista se ci fossero più
                // modificatori per la *stessa* lista.
                .add(new AdminPollutionEntry(submissionTimestamp, averages));
        System.out.println("AdminServer: Recorded " + averages.size() + " pollution averages for plant " + plantId +
                " (submitted at " + submissionTimestamp + ")");
    }

    // Metodo per calcolare la statistica richiesta dal client
    // Il vecchio `public int getPollution(int timeA, int timeB)` deve essere sostituito/modificato
    public PollutionStatsResponse getAveragePollutionBetween(long t1_ms, long t2_ms) {
        double totalCo2Sum = 0;
        int individualAveragesCount = 0; // Numero di singole medie considerate

        // L'iterazione su .values() di ConcurrentHashMap è debolmente consistente.
        // Va bene per letture statistiche che non richiedono una coerenza transazionale stretta
        // con le scritture. Se necessario, si potrebbe usare un lock esterno.
        for (List<AdminPollutionEntry> entriesForOnePlant : pollutionDataByPlantId.values()) {
            for (AdminPollutionEntry entry : entriesForOnePlant) {
                // Il timestamp da controllare è quello dell'invio del batch (submissionTimestamp)
                if (entry.getSubmissionTimestamp() >= t1_ms && entry.getSubmissionTimestamp() <= t2_ms) {
                    for (Double singleAvgCo2 : entry.getCo2Averages()) {
                        totalCo2Sum += singleAvgCo2;
                        individualAveragesCount++;
                    }
                }
            }
        }

        if (individualAveragesCount == 0) {
            System.out.println("AdminServer: No pollution data found in range [" + t1_ms + ", " + t2_ms + "] for average calculation.");
            return new PollutionStatsResponse(0.0, 0);
        }

        double overallAverage = totalCo2Sum / individualAveragesCount;
        System.out.println("AdminServer: Calculated overall CO2 average: " + String.format("%.2f", overallAverage) +
                " from " + individualAveragesCount + " individual average readings in range [" + t1_ms + ", " + t2_ms + "].");
        return new PollutionStatsResponse(overallAverage, individualAveragesCount);
    }

    public List<ThermalPowerPlants> addThermalPlants(int ID, String address, int port) {
        synchronized (this) {
            ThermalPowerPlants dummyPlants = new ThermalPowerPlants(ID, address, port, "http://localhost:8080");
            if (isPresent(dummyPlants)) {
                System.out.println("Adding ThermalPlants not possible because already there!!");
                return null;
            }
            else {
                ThermalPlants.add(dummyPlants);
                System.out.println("Added ThermalPlant: " + dummyPlants);
                System.out.println("Lista piante della nuova pianta: " + dummyPlants.getAllPlants().toString());
                return ThermalPlants;
            }
        }
    }

    public List<ThermalPowerPlants> getThermalPlants() {
        synchronized (this) {
            if (ThermalPlants.isEmpty()) { return null; }
            return ThermalPlants;
        }
    }

    public Map<Integer, String> getThermalPlantsExcept(int ID) {
        Map<Integer, String> topology = new HashMap<>();
        synchronized (this) {
            for (ThermalPowerPlants t : ThermalPlants) {
                if (t.getId() != ID) {
                    topology.put(t.getId(), t.getAddress() + ":" + t.getPortNumber().toString());
                     //Si dovrebbe poter togliere
                }
            }
            return topology;
        }
    }

    public Map<String, Object> getAveragePollutionBetweenAsMap(long t1_ms, long t2_ms) {
        double totalCo2Sum = 0;
        int individualAveragesCount = 0;

        // ... (la tua logica per calcolare totalCo2Sum e individualAveragesCount rimane la stessa) ...
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

        Map<String, Object> responseMap = new HashMap<>();
        if (individualAveragesCount == 0) {
            System.out.println("AdminServer: No pollution data found in range [" + t1_ms + ", " + t2_ms + "] for average calculation.");
            responseMap.put("averageCo2", 0.0);
            responseMap.put("readingsCount", 0);
        } else {
            double overallAverage = totalCo2Sum / individualAveragesCount;
            System.out.println("AdminServer: Calculated overall CO2 average: " + String.format("%.2f", overallAverage) +
                    " from " + individualAveragesCount + " individual average readings in range [" + t1_ms + ", " + t2_ms + "].");
            responseMap.put("averageCo2", overallAverage);
            responseMap.put("readingsCount", individualAveragesCount);
        }
        return responseMap;
    }

    private void UpdateInformations(String info, int i){
        informations.put(i, info);
    }

    private boolean isPresent(ThermalPowerPlants dummyPlants) {
        for (ThermalPowerPlants thermalPlant : ThermalPlants) {
            if (Objects.equals(thermalPlant.getId(), dummyPlants.getId())) {
                return true;
            }
        }
        return false;
    }
}
