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
    @GetMapping(value = "/getList", produces = "text/plain")
    public ResponseEntity<String> getThermalPlants() {

        ;
        if (administrator.getThermalPlants() == null) {
            return ResponseEntity.ok("No Power Plant is present");
        }

        return ResponseEntity.ok("Ecco la lista: " + administrator.getThermalPlants().toString());
    }

    @GetMapping(value = "/getPollution/{timeA}/{timeB}", produces = "text/plain")
    public ResponseEntity<String> getPollution(@PathVariable("timeA") int timeA, @PathVariable("timeB") int timeB) {

        if (administrator.getPollution(timeA, timeB) == -1) {
            return ResponseEntity.ok("Informations not present");
        }
        return ResponseEntity.ok("Average Pollution between " + timeA + "s and " + timeB + "s = " + administrator.getPollution(timeA, timeB));
    }

}
