package client;

import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import ThermalPowerPlants.ThermalPowerPlant;

public class ClientAdmin {

    private static final RestTemplate restTemplate = new RestTemplate();
    private static final String serverAddress = "http://localhost:8080";


    public static void main(String[] args) throws IOException {

        BufferedReader inputStream =
                new BufferedReader(new InputStreamReader(System.in));

        while (true) {
            //Compare il menù per poter selezionare cosa fare
            System.out.println("Select the service:\n 1) List of Thermal Plants\n 2) Get CO2 Pollution Statistics \n 3) Exit \n Number of the selection: ");
            int inputMenuSelection = Integer.parseInt(inputStream.readLine());

            if (inputMenuSelection == 3) { break; }

            switch (inputMenuSelection) {
                case 1:
                    listThermalPlants();
                    break;
                case 2:
                    getPollutionStatistics(inputStream);
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

    private static void listThermalPlants() {
        String getPath = "/Administrator/getList";
        try {
            ResponseEntity<String> response = restTemplate.getForEntity(serverAddress + getPath, String.class);
            System.out.println("Response Status: " + response.getStatusCode());
            System.out.println("Response Body:\n" + response.getBody());
        } catch (Exception e) {
            System.err.println("An unexpected error occurred: " + e.getMessage());
        }
    }

    private static void getPollutionStatistics(BufferedReader inputStream) throws IOException {
        String getPathBase = "/Administrator/getPollution/"; // O es. /admin/stats/pollution
        long timeA, timeB;

        try {
            System.out.print("Enter start timestamp (t1, in milliseconds from epoch): ");
            timeA = Long.parseLong(inputStream.readLine().trim());
            System.out.print("Enter end timestamp (t2, in milliseconds from epoch): ");
            timeB = Long.parseLong(inputStream.readLine().trim());
        } catch (NumberFormatException e) {
            System.out.println("Invalid timestamp format. Please enter a valid number.");
            return;
        }

        if (timeB < timeA) {
            System.out.println("End timestamp (t2) cannot be before start timestamp (t1).");
            return;
        }

        String fullPath = serverAddress + getPathBase + timeA + "/" + timeB;
        System.out.println("Fetching pollution statistics from: " + fullPath);

        try {
            ResponseEntity<PollutionStatsResponse> response = restTemplate.getForEntity(fullPath, PollutionStatsResponse.class);
            System.out.println("Response Status: " + response.getStatusCode());
            if (response.getBody() != null) {
                System.out.println("Pollution Statistics:");
                System.out.println("  Average CO2: " + String.format("%.2f", response.getBody().getAverageCo2()) + " g");
                System.out.println("  Based on " + response.getBody().getReadingsCount() + " readings.");
            } else {
                System.out.println("No statistics found or empty response body.");
            }

        } catch (Exception e) {
            System.err.println("An unexpected error occurred: " + e.getMessage());
        }
    }
}
