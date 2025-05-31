package ThermalPowerPlants;

import org.eclipse.paho.client.mqttv3.*;

import java.util.ArrayList;
import java.util.List;

public class ThermalPowerPlants {
    private int id;
    private String address;
    private int portNumber;
    private String serverAddress;

    private List<ThermalPowerPlants> otherPlants = new ArrayList<>();
    private MqttClient mqttClient;

    public ThermalPowerPlants() { }

    public ThermalPowerPlants(int id, String address, int port, String adminAddress) {
        this.id = id;
        this.address = address;
        this.portNumber = port;
        this.serverAddress = adminAddress;
    }



    public void start() {
        // 1. Registrati all'amministratore
        registerToAdminServer();

        // 2. Avvia server gRPC
        startGrpcServer();

        // 3. Connettiti a tutti gli altri impianti via gRPC
        connectToOtherPlants();

        // 4. Inizializza MQTT
        setupMqtt();

        // 5. Subscrivi al topic per ricevere richieste energetiche
        subscribeToEnergyRequests();
    }

    private void registerToAdminServer() {
        // HTTP POST verso l'amministratore con ID, address e port
    }

    private void startGrpcServer() {
        // Avvia server gRPC con il tuo handler (implementazione dei metodi)
    }

    private void connectToOtherPlants() {
        // Per ogni impianto nella lista, crea un gRPC stub client
    }

    private void setupMqtt() {
        // Inizializza MQTT client, connect, setCallback, ecc.
    }

    private void subscribeToEnergyRequests() {
        // Subscrivi a un topic tipo "energy/requests"
    }

    private void publishPollutionData() {
        // Pubblica ogni tot secondi su topic "pollution/plant{ID}"
    }

    public Integer getId(){
        return this.id;
    }

    public String getAddress(){ return this.address; }

    public Integer getPortNumber(){
        return this.portNumber;
    }

    public int[] getPollution(){ return new int[]{1 ,2}; }

    public List<ThermalPowerPlants> getOtherPlants(){
        return this.otherPlants;
    }

    public String toString(){
        return "ID = " + this.getId() +"\nAddress = " + this.getAddress() +"\nPortNumber = " + this.getPortNumber();
    }

    public void setPlantsList (List<ThermalPowerPlants> plants){
        this.otherPlants = plants;
    }
}