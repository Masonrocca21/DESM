package server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;


import org.springframework.stereotype.Service;


@Service //Spring automatically makes this class a singleton bean
public class Administrator {

    private final List<DummyPlants> ThermalPlants = new ArrayList<DummyPlants>();
    private HashMap<Integer, String> informations = new HashMap<>();

    private HashMap<Integer, int[]> pollutionStatistics = new HashMap<>();


    public int addThermalPlants(int ID, String address, int port) {
        synchronized (this) {
            DummyPlants dummyPlants = new DummyPlants(ID, address, port);
            if (isPresent(dummyPlants)) {
                System.out.println("Adding ThermalPlants not possible because already there!!");
                return -1;
            }
            else {
                ThermalPlants.add(dummyPlants);
                pollutionStatistics.put(ID, dummyPlants.getPollution());
                System.out.println("Added ThermalPlant: " + ThermalPlants.get(0));
                return 0;
            }
        }
    }

    public List<DummyPlants> getThermalPlants() {
        synchronized (this) {
            if (ThermalPlants.isEmpty()) { return null; }
            return ThermalPlants;
        }
    }

    public int getPollution(int timeA, int timeB) {
        int averagePollution = 0;
        if (ThermalPlants.isEmpty()) { return -1; }
        for (DummyPlants thermalPlant : ThermalPlants) {
            if (pollutionStatistics.get(thermalPlant.getId())[0] > timeA && pollutionStatistics.get(thermalPlant.getId())[1] < timeB) {
                averagePollution += pollutionStatistics.get(thermalPlant.getId())[1];
            }
        }
        return averagePollution/ThermalPlants.size();
    }

    private void UpdateInformations(String info, int i){
        informations.put(i, info);
    }

    private boolean isPresent(DummyPlants dummyPlants) {
        for (DummyPlants thermalPlant : ThermalPlants) {
            if (Objects.equals(thermalPlant.getId(), dummyPlants.getId())) {
                return true;
            }
        }
        return false;
    }
}
