package server;

import java.util.*;

import ThermalPowerPlants.ThermalPowerPlants;


import org.springframework.stereotype.Service;


@Service //Spring automatically makes this class a singleton bean
public class Administrator {

    private final List<ThermalPowerPlants> ThermalPlants = new ArrayList<ThermalPowerPlants>();
    private HashMap<Integer, String> informations = new HashMap<>();

    private HashMap<Integer, int[]> pollutionStatistics = new HashMap<>();


    public List<ThermalPowerPlants> addThermalPlants(int ID, String address, int port) {
        synchronized (this) {
            ThermalPowerPlants dummyPlants = new ThermalPowerPlants(ID, address, port, "http://localhost:8080");
            if (isPresent(dummyPlants)) {
                System.out.println("Adding ThermalPlants not possible because already there!!");
                return null;
            }
            else {
                ThermalPlants.add(dummyPlants);
                pollutionStatistics.put(ID, dummyPlants.getPollution());
                System.out.println("Added ThermalPlant: " + dummyPlants);
                System.out.println("Lista piante della nuova pianta: " + dummyPlants.getOtherPlants().toString());
                return ThermalPlants;
            }
        }
    }

    public List<ThermalPowerPlants> getThermalPlants() {
        synchronized (this) {
            if (ThermalPlants.isEmpty()) { return null; }
            return ThermalPlants;
        }
    }

    public Map<Integer, String> getThermalPlantsExcept(int ID) {
        Map<Integer, String> topology = new HashMap<>();
        synchronized (this) {
            for (ThermalPowerPlants t : ThermalPlants) {
                if (t.getId() != ID) {
                    topology.put(t.getId(), t.getAddress());
                     //Si dovrebbe poter togliere
                }
            }
            return topology;
        }
    }

    public int getPollution(int timeA, int timeB) {
        int averagePollution = 0;
        if (ThermalPlants.isEmpty()) { return -1; }
        for (ThermalPowerPlants thermalPlant : ThermalPlants) {
            if (pollutionStatistics.get(thermalPlant.getId())[0] > timeA && pollutionStatistics.get(thermalPlant.getId())[1] < timeB) {
                averagePollution += pollutionStatistics.get(thermalPlant.getId())[1];
            }
        }
        return averagePollution/ThermalPlants.size();
    }

    private void UpdateInformations(String info, int i){
        informations.put(i, info);
    }

    private boolean isPresent(ThermalPowerPlants dummyPlants) {
        for (ThermalPowerPlants thermalPlant : ThermalPlants) {
            if (Objects.equals(thermalPlant.getId(), dummyPlants.getId())) {
                return true;
            }
        }
        return false;
    }
}
