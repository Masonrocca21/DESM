package server;

public class DummyPlants {

    private Integer ID;
    private String address;
    private Integer portNumber;
    private String information;

    public DummyPlants(Integer ID, String address, Integer portNumber) {
        this.ID = ID;
        this.address = address;
        this.portNumber = portNumber;
        this.information = "C'è il sole";
    }

    int getID(){
        return this.ID;
    }

    String getAddress(){ return this.address; }

    int getPortNumber(){
        return this.portNumber;
    }

    String getInformation(){
        return this.information;
    }
}
