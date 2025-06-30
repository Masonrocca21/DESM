package Arbiter;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;

public class ArbiterServer {

    public static void main(String[] args) {
        // Definiamo la porta su cui il nostro servizio Arbitro sarà in ascolto.
        final int PORT = 9090;

        // Creiamo il server gRPC.
        Server server = ServerBuilder.forPort(PORT)
                .addService(new ArbiterServiceImpl())
                .build();

        try {
            // Avviamo il server.
            server.start();
            System.out.println("Arbiter Server started, listening on port " + PORT);

            // Aggiungiamo un hook di shutdown per chiudere il server in modo pulito
            // quando il processo Java viene terminato
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutting down Arbiter Server...");
                server.shutdown();
                System.out.println("Server shut down.");
            }));

            // Il server ora attende indefinitamente che arrivino richieste
            // o che venga terminato.
            server.awaitTermination();

        } catch (IOException e) {
            System.err.println("Failed to start server: " + e.getMessage());
            e.printStackTrace(System.err);
        } catch (InterruptedException e) {
            System.err.println("Server interrupted: " + e.getMessage());
            e.printStackTrace(System.err);
            Thread.currentThread().interrupt();
        }
    }
}
