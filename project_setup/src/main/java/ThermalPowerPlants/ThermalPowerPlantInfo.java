package ThermalPowerPlants;

public class ThermalPowerPlantInfo {
    private int id;
    private String address;
    private int portNumber;

    public ThermalPowerPlantInfo(int id, String address, int portNumber) {
        this.id = id;
        this.address = address;
        this.portNumber = portNumber;
    }

    public int getId() { return id; }
    public String getAddress() { return address; }
    public int getPortNumber() { return portNumber; }

    @Override
    public String toString() {
        return "PlantInfo{id='" + id + '\'' + ", address='" + address + '\'' + ", port=" + portNumber + '}';
    }
    // Implementa equals e hashCode se li metti in Set o usi come chiavi in Map
}