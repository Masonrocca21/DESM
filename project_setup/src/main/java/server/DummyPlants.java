package server;

import java.util.Objects;

public class DummyPlants {

    private Integer id;
    private String address;
    private Integer portNumber;
    private String information;

    public DummyPlants() {}

    public DummyPlants(Integer id, String address, Integer portNumber) {
        this.id = id;
        this.address = address;
        this.portNumber = portNumber;
        this.information = "C'è il sole";
    }

    public Integer getId(){
        return this.id;
    }

    public String getAddress(){ return this.address; }

    public Integer getPortNumber(){
        return this.portNumber;
    }

    public String getInformation(){
        return this.information;
    }

}
