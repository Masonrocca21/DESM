package MQTT;

import org.eclipse.paho.client.mqttv3.*;

import java.sql.Timestamp;

public class MQTT_Controller {

    private static String clientId;
    private static MqttClient client;
    private static String broker;
    private static String subTopic;
    private static int subQos = 2;



    public static void main(String[] args) {
        broker = "tcp://localhost:1883";
        clientId = MqttClient.generateClientId();
        subTopic = "home/renewableEnergyProvider/power";

        try {
            client = new MqttClient(broker, clientId);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);

            System.out.println(clientId + " Connecting Broker " + broker);
            client.connect(connOpts);
            System.out.println(clientId + " Connected " + Thread.currentThread().getId());

            client.setCallback(new MqttCallback() {

                public void messageArrived(String topic, MqttMessage message) {
                    String time = new Timestamp(System.currentTimeMillis()).toString();
                    String receivedMessage = new String(message.getPayload());
                    System.out.println(clientId + " Received a Message! - Callback - Thread PID: " + Thread.currentThread().getId() +
                            "\n\tTime:    " + time +
                            "\n\tTopic:   " + topic +
                            "\n\tMessage: " + receivedMessage);
                }

                public void connectionLost(Throwable cause) {
                    System.out.println(clientId + " Connectionlost! cause:" + cause.getMessage() + "-  Thread PID: " + Thread.currentThread().getId());
                }

                public void deliveryComplete(IMqttDeliveryToken token) {
                    //Not used here
                }
            });

            System.out.println(clientId + " Subscribing ... - Thread PID: " + Thread.currentThread().getId());
            client.subscribe(subTopic, subQos);
            System.out.println(clientId + " Subscribed to topics : " + subTopic);


        } catch (MqttException me) {
            System.out.println("reason " + me.getReasonCode());
            System.out.println("msg " + me.getMessage());
            System.out.println("loc " + me.getLocalizedMessage());
            System.out.println("cause " + me.getCause());
            System.out.println("excep " + me);
            me.printStackTrace();
        }


    }
}
