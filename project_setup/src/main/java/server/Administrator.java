package server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


import org.springframework.stereotype.Service;


@Service //Spring automatically makes this class a singleton bean
public class Administrator {

    private final List<DummyPlants> ThermalPlants = new ArrayList<DummyPlants>();
    private HashMap<Integer, String> informations = new HashMap<>();


    public int addThermalPlants(int ID, String address, int port) {
        synchronized (this) {
            DummyPlants dummyPlants = new DummyPlants(ID, address, port);
            if (ThermalPlants.contains(dummyPlants)) {
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

    public int getThermalPlants(int ID_) {
        synchronized (this) {
            for (DummyPlants thermalPlant : ThermalPlants) {
                if (thermalPlant.getID() == ID_) {
                    UpdateInformations(thermalPlant.getInformation(), thermalPlant.getID()); //Aggiorno le informazioni
                    return 0;
                }
            }
            return -1;
        }
    }

    public String getInformation(int ID_) {
        for (DummyPlants thermalPlant : ThermalPlants) {
            if (thermalPlant.getID() == ID_) {
                return thermalPlant.getInformation();
            }
        }
        return "Nessuna informazione";
    }

    private void UpdateInformations(String info, int i){
        informations.put(i, info);
    }
}
