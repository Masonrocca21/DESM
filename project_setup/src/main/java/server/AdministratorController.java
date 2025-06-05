package server;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import ThermalPowerPlants.ThermalPowerPlants;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

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
    public ResponseEntity<List<ThermalPowerPlants>> addThermalPlants(@RequestBody ThermalPowerPlants dummyPlants) {
        try {
            String json = mapper.writeValueAsString(dummyPlants);
            System.out.println("JSON inviato: " + json);
            System.out.println("Ricevuto ID: " + dummyPlants.getId());
            System.out.println("Ricevuto Address: " + dummyPlants.getAddress());
            System.out.println("Ricevuto Port: " + dummyPlants.getPortNumber());

            List<ThermalPowerPlants> result = administrator.addThermalPlants(dummyPlants.getId(), dummyPlants.getAddress(), dummyPlants.getPortNumber());
            if (result == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
            } else {
                //  Map<Integer, String> topology = administrator.getThermalPlantsExcept(dummyPlants.getId());
                return ResponseEntity.ok(result);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @GetMapping(value = "/getList", produces = "text/plain")
    public ResponseEntity<String> getThermalPlants() {
        if (administrator.getThermalPlants() == null) {
            return ResponseEntity.ok("No Power Plant is present");
        }

        return ResponseEntity.ok("Ecco la lista: " + administrator.getThermalPlants().toString());
    }

    @GetMapping(value = "/getPollution/{t1_ms}/{t2_ms}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> getPollutionStatistics( // CAMBIA TIPO DI RITORNO
                                                                       @PathVariable("t1_ms") long timeA_milliseconds,
                                                                       @PathVariable("t2_ms") long timeB_milliseconds) {

        System.out.println("AdminController: Received request for pollution stats between " +
                timeA_milliseconds + "ms and " + timeB_milliseconds + "ms");

        if (timeB_milliseconds < timeA_milliseconds) {
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "End timestamp cannot be before start timestamp");
            errorResponse.put("averageCo2", -1.0); // o valore indicativo di errore
            errorResponse.put("readingsCount", -1);
            return ResponseEntity.badRequest().body(errorResponse);
        }
        Map<String, Object> stats = administrator.getAveragePollutionBetweenAsMap(timeA_milliseconds, timeB_milliseconds);

        return ResponseEntity.ok(stats);

    }
}
