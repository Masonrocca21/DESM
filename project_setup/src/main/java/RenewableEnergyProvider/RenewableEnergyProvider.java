package RenewableEnergyProvider;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence; // Aggiunto
import org.json.JSONObject; // Aggiungeremo questo per arricchire il payload

import java.nio.charset.StandardCharsets; // Per encoding
import java.util.UUID;
import java.util.Random; // Già usato implicitamente da Math.random()
import java.util.concurrent.*;
import java.util.Map;
import java.util.Deque;

public class RenewableEnergyProvider {

    // Variabili esistenti dal tuo main, rese membri della classe se necessario
    // o passate ai metodi. Per semplicità, alcune diventano membri.
    private static String broker = "tcp://localhost:1883";
    private static String baseTopic = "home/renewableEnergyProvider/power"; // Il tuo topic originale
    private static int qos = 2;

    // Nuovi membri per la logica di accodamento e ACK
    private static MqttClient mqttClientInstance; // Istanza condivisa del client MQTT
    private static final String REP_ACK_LISTENER_CLIENT_ID = "REP_AckListener_" + UUID.randomUUID().toString().substring(0,4);
    private static final String ENERGY_REQUEST_PUBLISH_TOPIC = baseTopic + "/new"; // Topic per pubblicare richieste arricchite
    private static final String ENERGY_ACK_TOPIC = baseTopic + "/ack";   // Topic per ricevere ACK

    private static final Deque<RequestDetails> requestsToProcessQueue = new ConcurrentLinkedDeque<>();
    private static final Map<String, RequestDetails> pendingAckMap = new ConcurrentHashMap<>();
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2); // 1 per pubblicare, 1 per il generatore

    private static final long ACK_TIMEOUT_MS = 7000; // Timeout per ACK
    private static final int MAX_PUBLISH_ATTEMPTS = 3; // Max tentativi per richiesta

    // Inner class per i dettagli della richiesta
    static class RequestDetails {
        final String requestId;
        final double kwhValue; // Il valore originale generato
        final long createdAt;
        long lastPublishedAt;
        int publishAttempts;
        ScheduledFuture<?> ackTimeoutTask;

        public RequestDetails(String requestId, double kwhValue) {
            this.requestId = requestId;
            this.kwhValue = kwhValue;
            this.createdAt = System.currentTimeMillis();
            this.publishAttempts = 0;
        }
    }

    // Il costruttore può rimanere vuoto o fare inizializzazioni leggere
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

                RequestDetails rDetails = pendingAckMap.remove(ackRequestId);
                if (rDetails != null) {
                    if (rDetails.ackTimeoutTask != null) {
                        rDetails.ackTimeoutTask.cancel(false);
                        System.out.println("REP AckListener: Request " + ackRequestId + " ACKNOWLEDGED.");
                    }
                } else {
                    System.out.println("REP AckListener: ACK for unknown/handled request: " + ackRequestId);
                }
            } catch (Exception e) {
                System.err.println("REP AckListener: Error parsing ACK: " + payloadStr + " - " + e.getMessage());
            }
        });
        System.out.println("REP AckListener: Subscribed to ACK topic: " + ENERGY_ACK_TOPIC);

        // Avvia il task che processa requestsToProcessQueue e pubblica
        scheduler.scheduleWithFixedDelay(RenewableEnergyProvider::processAndPublishRequests,
                1, 2, TimeUnit.SECONDS); // Inizia dopo 1s, poi ogni 2s
    }

    // Nuovo metodo che il main chiamerà invece di client.publish()
    private static void enqueueNewEnergyNeed(double kwhValue, String originalClientId) {
        String requestId = "REQ-" + UUID.randomUUID().toString().substring(0, 8) + "-" + originalClientId.substring(0, Math.min(4,originalClientId.length()));
        RequestDetails newReq = new RequestDetails(requestId, kwhValue);
        requestsToProcessQueue.addLast(newReq);
        System.out.println("REP ("+originalClientId+"): Energy need for " + String.format("%.2f", kwhValue) +
                " kWh (ID: " + requestId + ") enqueued. Queue size: " + requestsToProcessQueue.size());
    }

    private static void processAndPublishRequests() {
        RequestDetails requestToPublish = null;

        // Priorità alla retryQueue (se l'avessimo separata, per ora prendiamo da requestsToProcessQueue)
        // Se una richiesta è in pendingAckMap e il suo timer scade, verrà rimessa in requestsToProcessQueue
        // Per ora, processiamo semplicemente la testa di requestsToProcessQueue
        if (!requestsToProcessQueue.isEmpty()) {
            requestToPublish = requestsToProcessQueue.pollFirst(); // Prendi e rimuovi
        }

        if (requestToPublish != null) {
            if (requestToPublish.publishAttempts >= MAX_PUBLISH_ATTEMPTS) {
                System.out.println("REP Publisher: Request " + requestToPublish.requestId + " max retries. Discarding.");
                // Rimuovi da pendingAckMap se per caso fosse lì (es. timeout e ripubblicazione fallita)
                RequestDetails prevPending = pendingAckMap.remove(requestToPublish.requestId);
                if(prevPending != null && prevPending.ackTimeoutTask != null) prevPending.ackTimeoutTask.cancel(true);
                return;
            }

            requestToPublish.lastPublishedAt = System.currentTimeMillis();
            requestToPublish.publishAttempts++;

            JSONObject payloadJson = new JSONObject();
            payloadJson.put("requestId", requestToPublish.requestId);
            payloadJson.put("kWh", requestToPublish.kwhValue); // Il valore energetico originale
            payloadJson.put("timestamp", requestToPublish.lastPublishedAt);
            payloadJson.put("attempt", requestToPublish.publishAttempts);
            String payloadToSend = payloadJson.toString();

            try {
                // Assumiamo che il main abbia già connesso mqttClientInstance o che ne creiamo uno qui.
                // Per questo esempio, il main dovrebbe passare la sua istanza 'client'
                // o dobbiamo usare un'istanza condivisa.
                // Per ora, questo metodo dovrebbe avere accesso a un MqttClient connesso.
                // Se il client del main è locale, questo non funzionerà direttamente.
                // Modifichiamo per usare mqttClientInstance che deve essere inizializzato nel main.

                MqttMessage message = new MqttMessage(payloadToSend.getBytes(StandardCharsets.UTF_8));
                message.setQos(qos); // Usa il qos definito nel main

                if (mqttClientInstance == null || !mqttClientInstance.isConnected()) {
                    System.err.println("REP Publisher: Main MQTT client not available for publishing request " + requestToPublish.requestId);
                    requestsToProcessQueue.addFirst(requestToPublish); // Rimetti in testa per riprovare
                    requestToPublish.publishAttempts--;
                    return;
                }

                mqttClientInstance.publish(ENERGY_REQUEST_PUBLISH_TOPIC, message);
                System.out.println("REP Publisher: PUBLISHED (Attempt " + requestToPublish.publishAttempts +
                        ") Request ID: " + requestToPublish.requestId + ", Payload: " + payloadToSend);

                pendingAckMap.put(requestToPublish.requestId, requestToPublish);
                final String reqIdForTimeout = requestToPublish.requestId;

                requestToPublish.ackTimeoutTask = scheduler.schedule(() -> {
                    RequestDetails timedOutRequest = pendingAckMap.remove(reqIdForTimeout);
                    if (timedOutRequest != null) {
                        System.out.println("REP Publisher: ACK TIMEOUT for " + timedOutRequest.requestId +
                                " (Attempt " + timedOutRequest.publishAttempts + "). Re-enqueueing.");
                        requestsToProcessQueue.addFirst(timedOutRequest); // Rimetti in testa alla coda principale
                    }
                }, ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            } catch (MqttException e) {
                System.err.println("REP Publisher: MQTT Error publishing " + requestToPublish.requestId + ": " + e.getMessage());
                requestToPublish.publishAttempts--; // Non conta come tentativo
                requestsToProcessQueue.addFirst(requestToPublish); // Rimetti in testa
            }
        }
    }


    // IL TUO METODO MAIN MODIFICATO MINIMAMENTE
    public static void main(String[] argv) {
        // Le variabili broker, clientId, topic (ora baseTopic), qos sono già membri statici

        // clientId è per il publisher principale
        String publisherClientId = MqttClient.generateClientId();

        try {
            // Inizializza le funzionalità avanzate (listener ACK, scheduler per la coda di pubblicazione)
            initializeAdvancedFeatures(); // Questo imposterà il listener per gli ACK

            // Il client MQTT usato dal loop while(true) per pubblicare
            mqttClientInstance = new MqttClient(broker, publisherClientId, new MemoryPersistence());
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            // Non impostare setAutomaticReconnect qui se initializeAdvancedFeatures lo fa per il suo client,
            // o assicurati che non ci siano conflitti se usano lo stesso client ID (cosa che non fanno).

            System.out.println("REP Publisher (" + publisherClientId + "): Connecting Broker " + broker);
            mqttClientInstance.connect(connOpts);
            System.out.println("REP Publisher (" + publisherClientId + "): Connected.");

            // Il tuo loop originale che GENERA l'esigenza energetica
            while (true) {
                // Genera il valore dell'energia
                double kwhDemand = (Math.random() * (15000 - 5000 + 1) + 5000);
                String originalPayloadValue = String.valueOf(kwhDemand); // Il tuo payload originale

                System.out.println("REP Generator (" + publisherClientId + "): Generated demand: " + originalPayloadValue + " kWh");

                // Invece di pubblicare direttamente, accoda la necessità
                enqueueNewEnergyNeed(kwhDemand, publisherClientId);

                Thread.sleep(10000); // Usa la costante
            }

        } catch (MqttException me) {
            System.out.println("reason " + me.getReasonCode());
            System.out.println("msg " + me.getMessage());
            // ... (resto della tua gestione eccezioni) ...
            me.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // Lo scheduler e il listener ACK continueranno a girare.
        // Aggiungi uno shutdown hook per fermarli se necessario.
        // Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        //     scheduler.shutdownNow();
        //     try { if (mqttClientInstance.isConnected()) mqttClientInstance.disconnect(); } catch (Exception ignored) {}
        //     // ... ferma anche ackListenerClient ...
        // }));
    }
}
