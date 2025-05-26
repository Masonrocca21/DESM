package MQTT;

import org.eclipse.paho.client.mqttv3.*;

import java.sql.Timestamp;

public class MQTT_Controller {

    private static String clientId;
    private static MqttClient client;
    private static String broker;
    private static String subTopic;
    private static String pubTopic;

    public static void main(String[] args) {
        broker = "tcp://localhost:1883";
        clientId = MqttClient.generateClientId();
        subTopic = "home/sensors/temp";
        pubTopic = "home/controllers/temp";
    }
}
