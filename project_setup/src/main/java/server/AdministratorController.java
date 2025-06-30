package server;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import ThermalPowerPlants.PendingRequest;
import ThermalPowerPlants.PeerInfo;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

@RestController
@RequestMapping("/Administrator")
public class AdministratorController {

    private final Administrator administrator;

    public AdministratorController(Administrator administrator) {

        this.administrator = administrator;
        System.out.println("Controller creato");
    }

    @PostMapping(value = "/add", consumes = {"application/json", "application/xml"})
    public ResponseEntity<List<PeerInfo>> addThermalPlants(@RequestBody PeerInfo dummyPlants) {
        try {
            System.out.println("AdminController: Received request to add plant: " + dummyPlants);

            List<PeerInfo> result = administrator.addPlant(dummyPlants);
            if (result == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
            } else {
                return ResponseEntity.ok(result);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @PostMapping("/request-work")
    public ResponseEntity<PendingRequest> requestWork() {
        PendingRequest work = administrator.getNextWork();
        if (work == null) {
            // 204 No Content è la risposta HTTP corretta per "richiesta ok, ma non c'è nulla da darti"
            return ResponseEntity.noContent().build();
        }
        // 200 OK con il corpo della richiesta
        return ResponseEntity.ok(work);
    }

    @GetMapping(value = "/getList", produces = "text/plain")
    public ResponseEntity<String> getThermalPlants() {
        if (administrator.getAllRegisteredPlants() == null) {
            return ResponseEntity.ok("No Power Plant is present");
        }

        return ResponseEntity.ok("Ecco la lista: " + administrator.getAllRegisteredPlants().toString());
    }

    @GetMapping(value = "/getPollution/{t1_ms}/{t2_ms}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> getPollutionStatistics( @PathVariable("t1_ms") long timeA_milliseconds,
                                                                       @PathVariable("t2_ms") long timeB_milliseconds) {

        System.out.println("AdminController: Received request for pollution stats between " +
                timeA_milliseconds + "ms and " + timeB_milliseconds + "ms");

        if (timeB_milliseconds < timeA_milliseconds) {
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "End timestamp cannot be before start timestamp");
            errorResponse.put("averageCo2", -1.0); // errore
            errorResponse.put("readingsCount", -1);
            return ResponseEntity.badRequest().body(errorResponse);
        }
        Map<String, Object> stats = administrator.getAveragePollutionBetweenAsMap(timeA_milliseconds, timeB_milliseconds);

        return ResponseEntity.ok(stats);

    }


}
