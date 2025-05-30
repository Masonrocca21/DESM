package RenewableEnergyProvider;

import org.eclipse.paho.client.mqttv3.*;

public class RenewableEnergyProvider {

    public RenewableEnergyProvider() {}

    public static void main(String[] argv){

        String broker = "tcp://localhost:1883";
        String clientId = MqttClient.generateClientId();
        String topic = "home/renewableEnergyProvider/power";
        int qos = 2;

        try {
            MqttClient client = new MqttClient(broker, clientId);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);

            System.out.println(clientId + " Connecting Broker " + broker);
            client.connect(connOpts);
            System.out.println(clientId + " Connected");

            //Every five seconds, generate the random value of temperature
            while(true) {
                String payload = String.valueOf((Math.random()*(15000 - 5000 + 1) + 5000));
                MqttMessage message = new MqttMessage(payload.getBytes());
                message.setQos(qos);
                System.out.println(clientId + " Publishing message: " + payload + " ...");
                client.publish(topic, message);
                System.out.println(clientId + " Message published");
                Thread.sleep(10000);
            }

        } catch (MqttException me ) {
            System.out.println("reason " + me.getReasonCode());
            System.out.println("msg " + me.getMessage());
            System.out.println("loc " + me.getLocalizedMessage());
            System.out.println("cause " + me.getCause());
            System.out.println("excep " + me);
            me.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
