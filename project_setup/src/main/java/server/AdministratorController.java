package server;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/Administrator")
public class AdministratorController {

    private final Administrator administrator;

    ObjectMapper mapper = new ObjectMapper();

    public AdministratorController(Administrator administrator) {

        this.administrator = administrator;
        System.out.println("Controller creato");
    }

    @PostMapping(value = "/add", consumes = {"application/json", "application/xml"})
    public ResponseEntity<Void> addThermalPlants(@RequestBody DummyPlants dummyPlants) {
        try {
            String json = mapper.writeValueAsString(dummyPlants);
            System.out.println("JSON inviato: " + json);
            System.out.println("Ricevuto ID: " + dummyPlants.getId());
            System.out.println("Ricevuto Address: " + dummyPlants.getAddress());
            System.out.println("Ricevuto Port: " + dummyPlants.getPortNumber());

            int result = administrator.addThermalPlants(dummyPlants.getId(), dummyPlants.getAddress(), dummyPlants.getPortNumber());
            if (result == -1) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
            } else {
                return ResponseEntity.ok().build();
            }
        }catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
}
    @GetMapping(value = "/get/{ID}", produces = "text/plain")
    public ResponseEntity<String> getThermalPlants(@PathVariable("ID") int IDplant) {

        int information = administrator.getThermalPlants(IDplant);
        if (information == -1) {
            return ResponseEntity.ok("Informations not present");
        }
        return ResponseEntity.ok(administrator.getInformation(IDplant));
        // ATTENZIONE qui probabilmente si può mettere tutto insieme...verificare
    }

    public List<DummyPlants> getPlants() {
        return administrator.getThermalPlantsList();
    }
}
