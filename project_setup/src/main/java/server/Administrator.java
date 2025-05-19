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


    public int addThermalPlants(int ID, String address, int port) {
        synchronized (this) {
            DummyPlants dummyPlants = new DummyPlants(ID, address, port);
            if (isPresent(dummyPlants)) {
                System.out.println("Adding ThermalPlants " + dummyPlants + " not possible because already there");
                return -1;
            }
            else {
                ThermalPlants.add(dummyPlants);
                System.out.println("Added ThermalPlant: " + dummyPlants);
                return 0;
            }
        }
    }

    public int getThermalPlants(int Id_) {
        synchronized (this) {
            for (DummyPlants thermalPlant : ThermalPlants) {
                if (thermalPlant.getId() == Id_) {
                    UpdateInformations(thermalPlant.getInformation(), thermalPlant.getId()); //Aggiorno le informazioni
                    return 0;
                }
            }
            return -1;
        }
    }

    public String getInformation(int id_) {
        for (DummyPlants thermalPlant : ThermalPlants) {
            if (thermalPlant.getId() == id_) {
                return thermalPlant.getInformation();
            }
        }
        return "Nessuna informazione";
    }

    public List<DummyPlants> getThermalPlantsList() {
        return ThermalPlants;
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
