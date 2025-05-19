package server;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/Administrator")
public class AdministratorController {

    private final Administrator administrator;

    public AdministratorController(Administrator administrator) {
        this.administrator = administrator;
    }

    @PostMapping(value = "/add", consumes = {"application/json", "application/xml"})
    public ResponseEntity<Void> addThermalPlants(@RequestBody DummyPlants dummyPlants) {
        int result = administrator.addThermalPlants(dummyPlants.getID(), dummyPlants.getAddress(), dummyPlants.getPortNumber());
        if (result == -1) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
        } else {
            return ResponseEntity.ok().build();
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
}
