package RenewableEnergyProvider;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence; // Aggiunto
import org.json.JSONObject; // Aggiungeremo questo per arricchire il payload

import java.nio.charset.StandardCharsets; // Per encoding
import java.util.*;

public class RenewableEnergyProvider {

    // Variabili esistenti dal tuo main, rese membri della classe se necessario
    // o passate ai metodi. Per semplicità, alcune diventano membri.
    private static String broker = "tcp://localhost:1883";
    private static String baseTopic = "home/renewableEnergyProvider/power"; // Il tuo topic originale
    private static int qos = 1;

    // Nuovi membri per la logica di accodamento e ACK
    private static MqttClient mqttClientInstance; // Istanza condivisa del client MQTT
    private static final String REP_ACK_LISTENER_CLIENT_ID = "REP_AckListener_" + UUID.randomUUID().toString().substring(0,4);
    private static final String ENERGY_REQUEST_PUBLISH_TOPIC = baseTopic + "/new"; // Topic per pubblicare richieste arricchite
    private static final String ENERGY_ACK_TOPIC = baseTopic + "/ack";   // Topic per ricevere ACK

    private static final Deque<RequestDetails> newRequestsQueue = new LinkedList<>(); // NUOVO
    private static final Object newRequestsQueueLock = new Object();
    private static final Map<String, RequestDetails> pendingAckMap = new HashMap<>(); // NUOVO
    private static final Object pendingAckMapLock = new Object();

    private static final Deque<RequestDetails> retryQueue = new LinkedList<>(); // NUOVO
    private static final Object retryQueueLock = new Object();
    private static volatile boolean repRunning = true; // Per fermare i thread

    private static final long GENERATE_NEW_REQUEST_INTERVAL_MS = 10000;
    private static final long PUBLISH_CHECK_INTERVAL_MS = 2000;
    private static final long ACK_TIMEOUT_MS = 7000; // Timeout per ACK
    private static final int MAX_PUBLISH_ATTEMPTS = 3; // Max tentativi per richiesta

    // Inner class per i dettagli della richiesta
    static class RequestDetails {
        final String requestId;
        final double kwhValue;
        final long createdAt;
        long lastPublishedAt; // Quando è stato pubblicato l'ultima volta (per timeout)
        int publishAttempts;
        // ScheduledFuture<?> ackTimeoutTask; // RIMUOVI QUESTO

        public RequestDetails(String requestId, double kwhValue) {
            this.requestId = requestId;
            this.kwhValue = kwhValue;
            this.createdAt = System.currentTimeMillis();
            this.publishAttempts = 0;
            this.lastPublishedAt = 0; // 0 significa non ancora pubblicato o timeout scaduto
        }
    }

    public RenewableEnergyProvider() {}

    // Metodo per avviare la logica aggiuntiva (chiamato una volta)
    public static void initializeAdvancedFeatures() throws MqttException {
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setAutomaticReconnect(true);

        // Usiamo la stessa istanza di MqttClient per il listener e il publisher (se possibile e gestito correttamente)
        // Oppure due client separati se più semplice. Per ora, proviamo con uno.
        // Se il tuo 'client.publish' nel main è locale, allora questo mqttClientInstance è per il listener.
        // Il publisher del main userà il suo 'client' locale. Dobbiamo coordinare.

        // Per semplicità, assumiamo che il main ora usi mqttClientInstance per pubblicare
        // oppure che questo setupAckListener crei un suo client solo per ascoltare.
        // Per evitare di toccare il client del main, creiamo un client separato per gli ACK.
        MqttClient ackListenerClient = new MqttClient(broker, REP_ACK_LISTENER_CLIENT_ID, new MemoryPersistence());
        System.out.println("REP AckListener (" + REP_ACK_LISTENER_CLIENT_ID + "): Connecting to Broker " + broker);
        ackListenerClient.connect(connOpts);
        System.out.println("REP AckListener (" + REP_ACK_LISTENER_CLIENT_ID + "): Connected.");

        ackListenerClient.subscribe(ENERGY_ACK_TOPIC, 1, (topic, message) -> {
            String payloadStr = new String(message.getPayload(), StandardCharsets.UTF_8);
            System.out.println("REP AckListener: ACK Received: " + payloadStr);
            try {
                JSONObject ackPayload = new JSONObject(payloadStr);
                String ackRequestId = ackPayload.getString("requestId");

                synchronized (pendingAckMapLock) {
                    RequestDetails rDetails = pendingAckMap.remove(ackRequestId);
                    if (rDetails != null) {
                        System.out.println("REP AckListener: Request " + ackRequestId + " ACKNOWLEDGED.");
                        // Non c'è più un ackTimeoutTask da cancellare direttamente
                    } else {
                        System.out.println("REP AckListener: ACK for unknown/handled request: " + ackRequestId);
                    }
                }
            }catch (Exception e) {
                System.err.println("REP AckListener: Error parsing ACK: " + payloadStr + " - " + e.getMessage());
            }
        });
        System.out.println("REP AckListener: Subscribed to ACK topic: " + ENERGY_ACK_TOPIC);

    }

    // Nuovo metodo che il main chiamerà invece di client.publish()
    private static void enqueueNewEnergyNeed(double kwhValue, String originalClientId) {
        String requestId = "REQ-" + UUID.randomUUID().toString().substring(0, 8) + "-" + originalClientId.substring(0, Math.min(4,originalClientId.length()));
        RequestDetails newReq = new RequestDetails(requestId, kwhValue);
        synchronized (newRequestsQueueLock) {
            newRequestsQueue.addLast(newReq);
        }
        System.out.println("REP ("+originalClientId+"): Energy need for " + String.format("%.2f", kwhValue) +
                " kWh (ID: " + requestId + ") enqueued. Queue size: " + newRequestsQueue.size());
    }

    private static void processAndPublishRequestsLoop() {
        System.out.println("REP: Main Publisher Loop STARTED.");
        while (repRunning && !Thread.currentThread().isInterrupted()) {
            try {
                RequestDetails requestToPublish = null;
                synchronized (retryQueueLock) {
                    if (!retryQueue.isEmpty()) {
                        requestToPublish = retryQueue.pollFirst();
                    }
                }
                if (requestToPublish != null) {
                    System.out.println("REP: Attempting to republish (from retry queue): " + requestToPublish.requestId);
                } else {
                    synchronized (newRequestsQueueLock) {
                        if (!newRequestsQueue.isEmpty()) {
                            requestToPublish = newRequestsQueue.pollFirst();
                        }
                    }
                    if (requestToPublish != null) {
                        System.out.println("REP: Attempting to publish new request: " + requestToPublish.requestId);
                    }
                }

                if (requestToPublish != null) {
                    publishRequestInternal(requestToPublish);
                }

                Thread.sleep(PUBLISH_CHECK_INTERVAL_MS); // Intervallo di controllo
            } catch (InterruptedException e) {
                System.out.println("REP: Main Publisher Loop interrupted.");
                repRunning = false;
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.err.println("REP: Error in Main Publisher Loop: " + e.getMessage());
                e.printStackTrace();
            }
        }
        System.out.println("REP: Main Publisher Loop STOPPED.");
    }

    private static void publishRequestInternal(RequestDetails request) {
        // 1. Controlla se il client MQTT è connesso e pronto
        if (mqttClientInstance == null || !mqttClientInstance.isConnected()) {
            System.err.println("REP Publisher: MQTT client non connesso. Impossibile pubblicare la richiesta " + request.requestId);
            // In un design più semplice, potremmo tentare di riconnetterci qui,
            // ma per ora logghiamo solo l'errore. Il thread principale riproverà al prossimo ciclo.
            return;
        }

        // 2. Costruisci il payload JSON
        JSONObject payloadJson = new JSONObject();
        payloadJson.put("requestId", request.requestId);
        payloadJson.put("kWh", request.kwhValue);
        payloadJson.put("timestamp", request.createdAt); // Usiamo il timestamp di creazione originale

        String payloadToSend = payloadJson.toString();

        try {
            // 3. Crea il messaggio MQTT
            MqttMessage message = new MqttMessage(payloadToSend.getBytes(StandardCharsets.UTF_8));
            message.setQos(qos);

            // ---- LA MODIFICA CHIAVE ----
            // Imposta il flag "retained" a true.
            // Questo dice al broker di conservare questo messaggio per questo topic.
            message.setRetained(true);
            // --------------------------

            // 4. Pubblica il messaggio
            mqttClientInstance.publish(ENERGY_REQUEST_PUBLISH_TOPIC, message);

            System.out.println("REP Publisher: PUBLISHED (retained) Request ID: '" + request.requestId +
                    "', kWh: " + String.format("%.2f", request.kwhValue) +
                    ", Topic: " + ENERGY_REQUEST_PUBLISH_TOPIC);

        } catch (MqttException e) {
            System.err.println("REP Publisher: Errore MQTT durante la pubblicazione della richiesta '" + request.requestId + "': " + e.getMessage());
            // Aggiungi qui la gestione degli errori, ad es. logging, tentativi di riconnessione, ecc.
            e.printStackTrace();
        }
    }

    private static void manageAckTimeoutsLoop() {
        System.out.println("REP: ACK Timeout Manager Loop STARTED.");
        while (repRunning && !Thread.currentThread().isInterrupted()) {
            try {
                long currentTime = System.currentTimeMillis();
                List<String> timedOutRequestIds = new ArrayList<>();

                synchronized (pendingAckMapLock) {
                    // Itera su una copia per evitare ConcurrentModificationException se modifichi la mappa
                    for (Map.Entry<String, RequestDetails> entry : new HashMap<>(pendingAckMap).entrySet()) {
                        RequestDetails req = entry.getValue();
                        if (req.lastPublishedAt > 0 && (currentTime - req.lastPublishedAt > ACK_TIMEOUT_MS)) {
                            timedOutRequestIds.add(req.requestId);
                        }
                    }

                    for (String reqId : timedOutRequestIds) {
                        RequestDetails timedOutRequest = pendingAckMap.remove(reqId); // Rimuovi
                        if (timedOutRequest != null) {
                            System.out.println("REP: ACK TIMEOUT for " + timedOutRequest.requestId + ". Adding to retry queue.");
                            synchronized (retryQueueLock) {
                                retryQueue.addLast(timedOutRequest); // Aggiungi per riprovare
                            }
                        }
                    }
                }
                Thread.sleep(1000); // Controlla i timeout ogni secondo
            } catch (InterruptedException e) {
                System.out.println("REP: ACK Timeout Manager Loop interrupted.");
                repRunning = false;
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.err.println("REP: Error in ACK Timeout Manager Loop: " + e.getMessage());
                e.printStackTrace();
            }
        }
        System.out.println("REP: ACK Timeout Manager Loop STOPPED.");
    }

    // IL TUO METODO MAIN MODIFICATO MINIMAMENTE
    public static void main(String[] argv) {
        String publisherClientId = "RenewableEnergyProvider_" + UUID.randomUUID().toString().substring(0, 8);
        MemoryPersistence persistence = new MemoryPersistence();

        try {
            // 1. Connessione del client
            mqttClientInstance = new MqttClient(broker, publisherClientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            connOpts.setAutomaticReconnect(true);

            System.out.println("REP (" + publisherClientId + "): Connessione al broker " + broker);
            mqttClientInstance.connect(connOpts);
            System.out.println("REP (" + publisherClientId + "): Connesso.");

            // 2. Thread per generare e pubblicare richieste
            Thread requestGeneratorThread = new Thread(() -> {
                while (repRunning) {
                    try {
                        // Genera i dati della richiesta
                        double kwhDemand = (Math.random() * (15000 - 5000 + 1) + 5000);
                        String requestId = "REQ-" + UUID.randomUUID().toString().substring(0, 8);
                        RequestDetails request = new RequestDetails(requestId, kwhDemand);

                        // Pubblica direttamente
                        publishRequestInternal(request);

                        Thread.sleep(GENERATE_NEW_REQUEST_INTERVAL_MS);

                    } catch (InterruptedException e) {
                        System.out.println("REP: Thread generatore interrotto.");
                        repRunning = false;
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        System.err.println("REP: Errore nel loop del generatore: " + e.getMessage());
                        // Aggiungi un piccolo ritardo per evitare di spammare errori in caso di problemi persistenti
                        try { Thread.sleep(2000); } catch (InterruptedException ie) {}
                    }
                }
            });

            // Gestione della chiusura pulita
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("REP: Shutdown in corso...");
                repRunning = false;
                try {
                    // Opzionale: pulisci l'ultimo messaggio "retained" prima di chiudere
                    MqttMessage clearMessage = new MqttMessage(new byte[0]); // Payload vuoto
                    clearMessage.setQos(qos);
                    clearMessage.setRetained(true);
                    if (mqttClientInstance.isConnected()) {
                        mqttClientInstance.publish(ENERGY_REQUEST_PUBLISH_TOPIC, clearMessage);
                        System.out.println("REP: Messaggio 'retained' pulito dal topic.");
                    }

                    mqttClientInstance.disconnect();
                    System.out.println("REP: Disconnesso dal broker.");
                } catch (MqttException e) {
                    System.err.println("REP: Errore durante la disconnessione: " + e.getMessage());
                }
                System.out.println("REP: Shutdown completato.");
            }));

            requestGeneratorThread.start();
            System.out.println("REP: Provider avviato. Genera una nuova richiesta ogni " + (GENERATE_NEW_REQUEST_INTERVAL_MS / 1000) + " secondi.");
            System.out.println("Premi Ctrl+C per uscire.");

            // Mantieni il main thread vivo
            requestGeneratorThread.join();

        } catch (MqttException me) {
            System.err.println("Errore MQTT fatale: " + me.getMessage());
            me.printStackTrace();
        } catch (InterruptedException e) {
            System.out.println("Applicazione interrotta.");
        }
    }

}
