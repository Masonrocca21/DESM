package RenewableEnergyProvider;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets; // Per encoding
import java.util.*;

public class RenewableEnergyProvider {

    private static String broker = "tcp://localhost:1883";
    private static String baseTopic = "home/renewableEnergyProvider/power"; // Il topic originale
    private static int qos = 1;

    private static final String ENERGY_REQUEST_PUBLISH_TOPIC = baseTopic + "/new"; // Topic per pubblicare richieste arricchite


    public RenewableEnergyProvider() {}


    // IL TUO METODO MAIN MODIFICATO MINIMAMENTE
    public static void main(String[] args) {
        String publisherId = "renewable-provider-" + UUID.randomUUID().toString().substring(0, 4);
        MqttClient client = null;

        try {
            client = new MqttClient(broker, publisherId, new MemoryPersistence());
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            connOpts.setAutomaticReconnect(true);

            System.out.println("Provider: Connecting to broker: " + broker);
            client.connect(connOpts);
            System.out.println("Provider: Connected.");

            Random random = new Random();

            while (!Thread.currentThread().isInterrupted()) {
                // 1. Genera dati
                double kwh = 5000 + random.nextInt(10001); // Intervallo [5000, 15000]
                String requestId = "REQ-" + UUID.randomUUID().toString().substring(0, 8);

                // 2. Costruisce il payload JSON
                JSONObject payloadJson = new JSONObject();
                payloadJson.put("requestId", requestId);
                payloadJson.put("kWh", kwh);
                payloadJson.put("timestamp", System.currentTimeMillis());
                String payload = payloadJson.toString();

                // 3. Crea e pubblica il messaggio MQTT
                MqttMessage message = new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
                message.setQos(qos);

                // *** PUBBLICA SUL TOPIC CORRETTO ***
                client.publish(ENERGY_REQUEST_PUBLISH_TOPIC, message);
                System.out.println("Provider: Published to '" + ENERGY_REQUEST_PUBLISH_TOPIC + "': " + payload);

                // 5. Attende 10 secondi
                Thread.sleep(10000);
            }

        } catch (MqttException | InterruptedException e) {
            System.err.println("Provider Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (client != null && client.isConnected()) {
                try {
                    client.disconnect();
                } catch (MqttException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
