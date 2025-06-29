package ThermalPowerPlants;

import com.example.powerplants.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

// Importa le tue classi generate da Protobuf


/**
 * GrpcClient gestisce la comunicazione in uscita verso altre centrali termiche.
 * Questa versione è adattata al file .proto fornito e non utilizza
 * classi dal package java.util.concurrent.
 */
public class GrpcClient {
    private final Random networkLatencySimulator = new Random();

    private final Map<String, ManagedChannel> channels = new HashMap<>();
    private final Object channelsLock = new Object();

    /**
     * Ottiene un canale gRPC per un dato peer in modo thread-safe.
     * Se non esiste, ne crea uno nuovo e lo salva nella cache.
     *
     * @param targetInfo Le informazioni del peer a cui connettersi.
     * @return Un ManagedChannel pronto all'uso.
     */
    private ManagedChannel getChannel(PeerInfo targetInfo) {
        String cacheKey = targetInfo.getAddress() + ":" + targetInfo.getPort();

        synchronized (channelsLock) {
            // Controlla se il canale esiste già nella nostra cache.
            if (channels.containsKey(cacheKey)) {
                ManagedChannel existingChannel = channels.get(cacheKey);
                // Aggiungiamo un controllo per sicurezza: se il canale è stato spento, lo ricreiamo.
                if (!existingChannel.isShutdown() && !existingChannel.isTerminated()) {
                    return existingChannel;
                }
            }

            // Se il canale non esiste o è stato chiuso, ne creiamo uno nuovo.
            System.out.println("LOG (gRPC Client): Creating new channel for target " + cacheKey);

            // --- MODIFICA CHIAVE ---
            // Usiamo il metodo forAddress(host, port) che è più diretto e meno ambiguo.
            ManagedChannel newChannel = ManagedChannelBuilder
                    .forAddress(targetInfo.getAddress(), targetInfo.getPort())
                    .usePlaintext() // Per connessioni non sicure in sviluppo
                    .build();

            channels.put(cacheKey, newChannel);
            return newChannel;
        }
    }

    // --- METODI PER LE CHIAMATE RPC ASINCRONE (NON BLOCCANTI) ---

    /**
     * Invia un annuncio di presenza a un altro peer.
     * Usato sia per il join iniziale che per la propagazione di informazioni.
     * @param target Il peer a cui inviare l'annuncio.
     * @param message Il messaggio con le informazioni della centrale.
     */
    public void announcePresence(PeerInfo target, PlantInfoMessage message) {
        System.out.println("LOG (gRPC Client of " + message.getId() + "): Attempting to call 'announcePresence' on target " + target.getId() + " at " + target.getAddress() + ":" + target.getPort());

        simulateNetworkLatency();

        try {
            ManagedChannel channel = getChannel(target);
            PlantServiceGrpc.PlantServiceStub asyncStub = PlantServiceGrpc.newStub(channel);

            asyncStub.announcePresence(message, new StreamObserver<Ack>() {
                @Override
                public void onNext(Ack value) {
                    System.out.println("LOG (gRPC Client of " + message.getId() + "): SUCCESS - Received ACK for announcePresence from " + target.getId());
                }
                @Override
                public void onError(Throwable t) {
                    // L'errore che vedi viene già da qui, quindi questo log è già presente
                    System.err.println("RPC 'announcePresence' failed for target " + target.getId() + ": " + t.getMessage());
                }
                @Override
                public void onCompleted() {}
            });
        } catch (Exception e) {
            // Log per errori nella creazione del canale, anche se improbabile
            System.err.println("FATAL (gRPC Client of " + message.getId() + "): Could not even get a channel for target " + target.getId() + ". Error: " + e.getMessage());
        }
    }

    /**
     * Invia un messaggio di elezione a un altro peer.
     * @param target Il peer a cui inviare il messaggio.
     * @param message Il messaggio di elezione.
     */
    public void sendElection(PeerInfo target, ElectionMessage message,  RpcCallback callback) {
        System.out.println("LOG (gRPC Client): Sending [SendElection] to Plant " + target.getId() + " (Candidate: " + message.getCandidateId() + ")");

        simulateNetworkLatency();

        ManagedChannel channel = getChannel(target);
        PlantServiceGrpc.PlantServiceStub asyncStub = PlantServiceGrpc.newStub(channel);

        asyncStub.sendElection(message, new StreamObserver<Ack>() {
            @Override
            public void onNext(Ack value) {}
            @Override
            public void onError(Throwable t) {
                // Errore: notifichiamo il chiamante.
                callback.onError(t);
                System.err.println("RPC 'sendElection' failed for target " + target.getId() + ": " + t.getMessage());
            }
            @Override
            public void onCompleted() {
                // Completato: notifichiamo il chiamante.
                callback.onCompleted();
            }
        });
    }

    /**
     * Invia il risultato di un'elezione o un messaggio di comando speciale.
     * @param target Il peer a cui inviare il messaggio.
     * @param message Il messaggio ElectedMessage.
     */
    public void sendElected(PeerInfo target, ElectedMessage message, RpcCallback callback) {
        System.out.println("LOG (gRPC Client): Sending [SendElected] to Plant " + target.getId() + " (Winner/CMD: " + message.getWinnerId() + ")");
        simulateNetworkLatency();

        ManagedChannel channel = getChannel(target);
        PlantServiceGrpc.PlantServiceStub asyncStub = PlantServiceGrpc.newStub(channel);

        asyncStub.sendElected(message, new StreamObserver<Ack>() {
            @Override
            public void onNext(Ack value) {}
            @Override
            public void onError(Throwable t) {
                callback.onError(t);
                System.err.println("RPC 'sendElected' failed for target " + target.getId() + ": " + t.getMessage());
            }
            @Override
            public void onCompleted() {
                callback.onCompleted();
            }
        });
    }

    /**
     * Chiude tutti i canali gRPC aperti in modo pulito.
     * Da chiamare alla chiusura dell'applicazione.
     */
    public void shutdown() {
        System.out.println("Shutting down gRPC client and all open channels...");

        Map<String, ManagedChannel> channelsToClose;
        synchronized(channelsLock) {
            channelsToClose = new HashMap<>(channels);
            channels.clear();
        }

        for (ManagedChannel channel : channelsToClose.values()) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                System.err.println("Interrupted during channel shutdown. Forcing shutdown for one channel.");
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        System.out.println("gRPC client shutdown complete.");
    }

    /**
     * Invia un messaggio "Baton" a un altro peer per passargli la responsabilità
     * di avviare un'elezione. La chiamata è asincrona.
     *
     * @param target Il peer a cui passare il testimone (il nostro successore).
     * @param message Il messaggio BatonMessage contenente i dettagli della richiesta.
     */
    public void passElectionBaton(PeerInfo target, BatonMessage message) {
        // Se per qualche motivo il target è nullo, esci per evitare errori.
        if (target == null) {
            System.err.println("ERROR (gRPC Client): Attempted to pass baton to a null target.");
            return;
        }

        // Log per tracciare l'azione
        System.out.println("LOG (gRPC Client): Passing election baton for request '"
                + message.getRequestId() + "' to target " + target.getId());

        try {
            // Ottiene il canale (riutilizzandolo se esiste)
            ManagedChannel channel = getChannel(target);

            // Crea uno stub asincrono
            PlantServiceGrpc.PlantServiceStub asyncStub = PlantServiceGrpc.newStub(channel);

            // Esegue la chiamata RPC asincrona
            asyncStub.passElectionBaton(message, new StreamObserver<Ack>() {
                @Override
                public void onNext(Ack value) {
                    // L'ACK è stato ricevuto, la chiamata è andata a buon fine a livello di rete.
                    // Non è necessario loggare qui per non intasare la console.
                }

                @Override
                public void onError(Throwable t) {
                    // Logga l'errore se la chiamata fallisce.
                    // Qui si potrebbe implementare la logica di retry che abbiamo discusso.
                    System.err.println("RPC 'passElectionBaton' failed for target " + target.getId() + ": " + t.getMessage());
                }

                @Override
                public void onCompleted() {
                    // La chiamata è conclusa.
                }
            });

        } catch (Exception e) {
            // Cattura eccezioni impreviste durante la creazione del canale o dello stub.
            System.err.println("FATAL (gRPC Client): Unexpected error while trying to pass election baton to " + target.getId() + ". Error: " + e.getMessage());
        }
    }

    /**
     * (NUOVO METODO HELPER)
     * Simula la latenza di rete attendendo per un tempo casuale.
     */
    private void simulateNetworkLatency() {
        try {
            // Simula una latenza tra 1000 e 2000 millisecondi
            int latency = 1500 + networkLatencySimulator.nextInt(1501);
            System.out.println("DEBUG (gRPC Client): Simulating network latency of " + latency + " ms...");
            Thread.sleep(latency);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
