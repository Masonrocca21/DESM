package ThermalPowerPlants;

import com.example.powerplants.EnergyRequest;
import com.example.powerplants.EnergyResponse;
import com.example.powerplants.PlantServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

public class NetworkManager {
    private Map<Integer, String> topology = new HashMap<>(); // es: 1 → "localhost:50051"
    private final int selfId;

    public NetworkManager(int selfId) {
        this.selfId = selfId;
    }

    public void updateTopology(Map<Integer, String> newTopology) {
        this.topology = newTopology;
    }

    public Optional<Integer> electLeader(int requestId, int requiredEnergy, int pollutionLevel, int availableEnergy) {
        int bestCandidateId = selfId;
        int bestPollution = pollutionLevel;
        int bestEnergy = availableEnergy;

        for (Map.Entry<Integer, String> entry : topology.entrySet()) {
            int otherId = entry.getKey();
            String target = entry.getValue();

            if (otherId == selfId) continue; // non contattare sé stessi

            try {
                ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                        .usePlaintext()
                        .build();

                PlantServiceGrpc.PlantServiceBlockingStub stub = PlantServiceGrpc.newBlockingStub(channel);

                EnergyRequest req = EnergyRequest.newBuilder()
                        .setRequestId(requestId)
                        .setRequiredEnergy(requiredEnergy)
                        .setSenderId(selfId)
                        .build();

                EnergyResponse res = stub.handleEnergyRequest(req);
                channel.shutdown();

                if (res.getCanHandle()) {
                    // confronto: meno inquinamento è meglio, a parità scegli ID più basso
                    if (res.getPollution() < bestPollution ||
                            (res.getPollution() == bestPollution && otherId < bestCandidateId)) {
                        bestCandidateId = otherId;
                        bestPollution = res.getPollution();
                        bestEnergy = res.getAvailableEnergy();
                    }
                }

            } catch (Exception e) {
                System.out.println("[WARN] Non riesco a contattare la pianta " + otherId + " → " + target);
            }
        }

        return Optional.of(bestCandidateId);
    }
}
