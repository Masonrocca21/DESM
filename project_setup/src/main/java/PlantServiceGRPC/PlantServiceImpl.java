package PlantServiceGRPC;

import io.grpc.stub.StreamObserver;
import com.example.powerplants.PlantServiceGrpc;
import com.example.powerplants.EnergyRequest;
import com.example.powerplants.EnergyResponse;
import com.example.powerplants.ElectionMessage;
import com.example.powerplants.ElectedMessage;
import com.example.powerplants.PlantInfoMessage;
import com.example.powerplants.Ack;

import ThermalPowerPlants.ThermalPowerPlants;
import ThermalPowerPlants.ThermalPowerPlantInfo;

public class PlantServiceImpl extends PlantServiceGrpc.PlantServiceImplBase {

    private final ThermalPowerPlants plant;

    public PlantServiceImpl(ThermalPowerPlants plant) {
        this.plant = plant;
    }
    /* @Override
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

     */
    @Override
    public void sendElection(ElectionMessage request, StreamObserver<Ack> responseObserver) {

        System.out.println("PlantServiceImpl (Plant " + (this.plant != null ? this.plant.getId() : "PLANT_IS_NULL") +
                "): In sendElection. this.plant is null? " + (this.plant == null));
        if (this.plant != null) {
            System.out.println("PlantServiceImpl (Plant " + this.plant.getId() +
                    "): this.plant.getNetworkManager() is null? " + (this.plant.getNetworkManager() == null) +
                    ". NM Hash if not null: " + (this.plant.getNetworkManager() != null ? System.identityHashCode(this.plant.getNetworkManager()) : "N/A"));
        }

        plant.getNetworkManager().onElectionMessage(request);
        responseObserver.onNext(Ack.newBuilder().setMessage("OK").build());
        responseObserver.onCompleted();
    }

    @Override
    public void sendElected(ElectedMessage request, StreamObserver<Ack> responseObserver) {
        plant.getNetworkManager().onElectedMessage(request);
        responseObserver.onNext(Ack.newBuilder().setMessage("OK").build());
        responseObserver.onCompleted();
    }

    @Override
    public void announcePresence(PlantInfoMessage request, StreamObserver<Ack> responseObserver) {
        System.out.println("TPP " + plant.getId() + ": Received AnnouncePresence from Plant ID " + request.getId() +
                " at " + request.getAddress() + ":" + request.getPortNumber());

        ThermalPowerPlantInfo newPlantInfo = new ThermalPowerPlantInfo(
                request.getId(),
                request.getAddress(),
                request.getPortNumber()
        );

        // Chiamare un metodo sulla nostra istanza di ThermalPowerPlants per gestire la nuova pianta
        boolean updated = plant.handleNewPlantAnnouncement(newPlantInfo);

        Ack ack = Ack.newBuilder().setMessage("Welcome Plant " + request.getId() + "! Presence acknowledged by Plant " + plant.getId()).build();
        responseObserver.onNext(ack);
        responseObserver.onCompleted();
    }
}
