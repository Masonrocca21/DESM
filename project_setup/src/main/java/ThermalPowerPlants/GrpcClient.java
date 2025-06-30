package ThermalPowerPlants;

import powerplants.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

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
                // Se il canale è stato spento, lo ricreiamo.
                if (!existingChannel.isShutdown() && !existingChannel.isTerminated()) {
                    return existingChannel;
                }
            }

            // Se il canale non esiste o è stato chiuso, ne creiamo uno nuovo.
            System.out.println("LOG (gRPC Client): Creating new channel for target " + cacheKey);

            ManagedChannel newChannel = ManagedChannelBuilder
                    .forAddress(targetInfo.getAddress(), targetInfo.getPort())
                    .usePlaintext()
                    .build();

            channels.put(cacheKey, newChannel);
            return newChannel;
        }
    }


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
                    System.err.println("RPC 'announcePresence' failed for target " + target.getId() + ": " + t.getMessage());
                }
                @Override
                public void onCompleted() {}
            });
        } catch (Exception e) {
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
     * Simula la latenza di rete attendendo per un tempo casuale.
     */
    private void simulateNetworkLatency() {
        try {
            // Simula una latenza tra 1500 e 3000 millisecondi
            int latency = 1500 + networkLatencySimulator.nextInt(1501);
            System.out.println("DEBUG (gRPC Client): Simulating network latency of " + latency + " ms...");
            Thread.sleep(latency);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
