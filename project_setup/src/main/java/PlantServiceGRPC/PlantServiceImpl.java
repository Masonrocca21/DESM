package PlantServiceGRPC;

import io.grpc.stub.StreamObserver;
import com.example.powerplants.PlantServiceGrpc;
import com.example.powerplants.EnergyRequest;
import com.example.powerplants.EnergyResponse;
import com.example.powerplants.ElectionMessage;
import com.example.powerplants.ElectedMessage;
import com.example.powerplants.Ack;

import ThermalPowerPlants.ThermalPowerPlants;
import ThermalPowerPlants.NetworkManager;

public class PlantServiceImpl extends PlantServiceGrpc.PlantServiceImplBase {

    private final ThermalPowerPlants plant;
    private final NetworkManager network;

    public PlantServiceImpl(ThermalPowerPlants plant) {
        this.plant = plant;
        this.network = new NetworkManager(
                plant.getId(),
                plant.getPollutionLevel(),  // o valore equivalente per eleggibilità
                plant.getSuccessorStub(),    // Map<Integer, Stub>
                plant.getSuccessorChannel()
        );
    }
    @Override
    public void handleEnergyRequest(EnergyRequest request, StreamObserver<EnergyResponse> responseObserver) {
        // Logica per decidere se soddisfare la richiesta
        int requiredEnergy = request.getRequiredEnergy();
        int requestId = request.getRequestId();

        System.out.println("Ricevuta richiesta gRPC: richiesta energia ID=" + requestId + ", quantità=" + requiredEnergy);

        // Avvia l'elezione dalla pianta che riceve la richiesta
        plant.handleIncomingEnergyRequest();

        // Risposta immediata per confermare la ricezione (non che può soddisfare)
        EnergyResponse response = EnergyResponse.newBuilder()
                .setCanHandle(false)
                .setMessage("Elezione avviata da pianta " + plant.getId())
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
    @Override
    public void sendElection(ElectionMessage request, StreamObserver<Ack> responseObserver) {
        network.onElectionMessage(request);
        responseObserver.onNext(Ack.newBuilder().setMessage("OK").build());
        responseObserver.onCompleted();
    }

    @Override
    public void sendElected(ElectedMessage request, StreamObserver<Ack> responseObserver) {
        network.onElectedMessage(request);
        responseObserver.onNext(Ack.newBuilder().setMessage("OK").build());
        responseObserver.onCompleted();
    }
}
