package ThermalPowerPlants;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.JSONObject;

import org.eclipse.paho.client.mqttv3.*;
import org.json.JSONObject;
import java.util.LinkedList;
import java.util.UUID;

public class CentralWorkQueue {

    // --- Singleton Pattern per garantire un'unica istanza ---
    private static volatile CentralWorkQueue instance;
    private static final Object singletonLock = new Object();

    public enum QueueState {
        OPEN_FOR_ELECTIONS,  // Lo stato normale: si possono avviare nuove elezioni
        AWAITING_RESULT      // Stato di transizione: un'elezione è vinta, stiamo aspettando che il risultato si propaghi.
    }

    private QueueState currentState = QueueState.OPEN_FOR_ELECTIONS;
    private String requestBeingFinalized = null; // L'ID della richiesta il cui risultato sta circolando

    public static CentralWorkQueue getInstance() {
        if (instance == null) {
            synchronized (singletonLock) {
                if (instance == null) {
                    instance = new CentralWorkQueue();
                }
            }
        }
        return instance;
    }
    // ----------------------------------------------------

    private final LinkedList<PendingRequest> requestQueue = new LinkedList<>();
    private final Object queueLock = new Object();
    private MqttClient mqttClient;
    private final String MQTT_BROKER = "tcp://localhost:1883";
    private final String ENERGY_REQUEST_TOPIC = "home/renewableEnergyProvider/power/new";

    // Costruttore privato
    private CentralWorkQueue() {
        System.out.println("CentralWorkQueue Singleton has been created. Starting its services.");
        startMqttListener();
    }

    /**
     * Avvia un client MQTT dedicato il cui unico scopo è ascoltare le nuove
     * richieste di energia e poplare la coda centralizzata.
     */
    private void startMqttListener() {
        try {
            String clientId = "Central-Queue-Listener-" + UUID.randomUUID().toString();
            this.mqttClient = new MqttClient(MQTT_BROKER, clientId, new MemoryPersistence());
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setAutomaticReconnect(true);
            connOpts.setCleanSession(true);

            this.mqttClient.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    System.err.println("CentralWorkQueue MQTT connection lost: " + cause.getMessage());
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) {
                    try {
                        String payload = new String(message.getPayload());
                        JSONObject json = new JSONObject(payload);
                        String requestId = json.getString("requestId");
                        double kwh = json.getDouble("kWh");

                        // Chiama il metodo per aggiungere la richiesta alla coda
                        enqueueRequest(new PendingRequest(requestId, kwh));
                    } catch (Exception e) {
                        System.err.println("CentralWorkQueue: Error parsing MQTT message: " + e.getMessage());
                    }
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) { /* Non usato */ }
            });

            System.out.println("CentralWorkQueue: Connecting to MQTT Broker...");
            this.mqttClient.connect(connOpts);
            this.mqttClient.subscribe(ENERGY_REQUEST_TOPIC, 1);
            System.out.println("CentralWorkQueue is now listening for energy requests on topic: " + ENERGY_REQUEST_TOPIC);

        } catch (MqttException e) {
            System.err.println("FATAL: CentralWorkQueue failed to start MQTT listener.");
            e.printStackTrace();
        }
    }

    /**
     * Aggiunge una nuova richiesta alla coda in modo thread-safe.
     */
    public void enqueueRequest(PendingRequest newRequest) {
        synchronized (queueLock) {
            boolean exists = this.requestQueue.stream()
                    .anyMatch(r -> r.getRequestId().equals(newRequest.getRequestId()));
            if (!exists) {
                this.requestQueue.add(newRequest);
                System.out.println("QUEUE: Added " + newRequest.getRequestId() + ". Queue size: " + this.requestQueue.size());
            }
        }
    }

    /**
     * Restituisce la prossima richiesta in coda senza rimuoverla (sbircia).
     * @return La prima richiesta, o null se la coda è vuota.
     */
    public PendingRequest peekNextRequest() {
        synchronized (queueLock) {
            return this.requestQueue.peek();
        }
    }

    /**
     * Rimuove una richiesta dalla cima della coda, ma solo se corrisponde all'ID fornito.
     * Questo previene che il risultato di un'elezione vecchia rimuova una richiesta nuova.
     * @param requestId L'ID della richiesta che ci si aspetta di trovare e rimuovere.
     * @return true se la richiesta è stata trovata in cima e rimossa, false altrimenti.
     */
    public synchronized boolean lockForFinalization(String requestId) {
        if (currentState == QueueState.OPEN_FOR_ELECTIONS &&
                !requestQueue.isEmpty() &&
                requestQueue.peek().getRequestId().equals(requestId)) {

            currentState = QueueState.AWAITING_RESULT;
            requestBeingFinalized = requestId;
            System.out.println("QUEUE: Locked for result propagation of " + requestId);
            return true;
        }
        return false;
    }

    public synchronized void unlockQueue(String requestId) {
        if (currentState == QueueState.AWAITING_RESULT &&
                requestId.equals(requestBeingFinalized)) {

            // Rimuovo la richiesta dalla coda solo ora!
            requestQueue.poll();
            currentState = QueueState.OPEN_FOR_ELECTIONS;
            requestBeingFinalized = null;
            System.out.println("QUEUE: Unlocked. Request " + requestId + " is fully completed.");
        }
    }

    /**
     * Un arbitro. Controlla se una richiesta è ancora valida (non già confermata).
     * Questo metodo viene chiamato da una centrale prima di avviare un'elezione.
     * @param requestId L'ID della richiesta.
     * @return true se la richiesta è ancora la prima in coda, false altrimenti.
     */
    public boolean isRequestStillPending(String requestId) {
        synchronized (queueLock) {
            PendingRequest head = requestQueue.peek();
            return head != null && head.getRequestId().equals(requestId);
        }
    }

    public boolean isQueueOpenForElections() {
        return currentState == QueueState.OPEN_FOR_ELECTIONS;
    }
}