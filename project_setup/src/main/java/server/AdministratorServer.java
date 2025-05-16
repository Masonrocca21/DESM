package server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class AdministratorServer {
    public static void main(String[] args) {
        SpringApplication.run(AdministratorServer.class, args);
        System.out.println("Server running on http://localhost:8080");
    }
}
