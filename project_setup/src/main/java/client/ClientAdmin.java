package client;

import org.springframework.expression.spel.ast.Selection;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;
import ThermalPowerPlants.ThermalPowerPlants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

public class ClientAdmin {

    private static final RestTemplate restTemplate = new RestTemplate();
    private static final String serverAddress = "http://localhost:8080";

    private int inputMenuSelection;


    public static void main(String[] args) throws IOException {

        BufferedReader inputStream =
                new BufferedReader(new InputStreamReader(System.in));

        while (true) {
            //Compare il menù per poter selezionare cosa fare
            System.out.println("Select the service:\n 1) List of Thermal Plants\n 2) Statistics \n 3) Exit \n Number of the selection: ");
            int inputMenuSelection = Integer.parseInt(inputStream.readLine());

            if (inputMenuSelection == 4) { break; }

            switch (inputMenuSelection) {
                case 1: { //Se 2 Visualizzo la lista di tutte le piante termali
                    //Setto per la get request
                    String getPath = "/Administrator/getList";
                    ResponseEntity<String> getResponse = getRequest(serverAddress + getPath );
                    System.out.println(getResponse);
                    System.out.println(getResponse.getBody());
                }
                break;
                case 2: { // Se 3 fornisco i valori delle statistiche
                    String getPath = "/Administrator/getPollution/";
                    System.out.println("Enter timeA: ");
                    int timeA = Integer.parseInt(inputStream.readLine());
                    System.out.println("Enter timeB: ");
                    int timeB = Integer.parseInt(inputStream.readLine());
                    ResponseEntity<String> getResponse = getRequest(serverAddress + getPath + timeA + "/" + timeB); //Verificare se serve il lo slash
                    System.out.println(getResponse);
                    System.out.println(getResponse.getBody());
                }
                break;
                case 3: {
                    //Chiudere
                }
                break;
                default: {
                    System.out.println("Select a valid option!");
                }
            }
        }
    }

    public static ResponseEntity<ThermalPowerPlants[]> postRequest(String url, ThermalPowerPlants dummyPlants) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<ThermalPowerPlants> request = new HttpEntity<>(dummyPlants, headers);
            return restTemplate.postForEntity(url, request, ThermalPowerPlants[].class);
        } catch (Exception e) {
            System.out.println("Server not available: " + e.getMessage());
            return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
        }
    }

    public static ResponseEntity<String> getRequest(String url) {
        try {
            return restTemplate.getForEntity(url, String.class);
        } catch (Exception e) {
            System.out.println("Server not available: " + e.getMessage());
            return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
        }
    }
}
