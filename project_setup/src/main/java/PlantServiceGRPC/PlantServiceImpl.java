package PlantServiceGRPC;

import io.grpc.stub.StreamObserver;
import com.example.powerplants.PlantServiceGrpc;
import com.example.powerplants.ElectionMessage;
import com.example.powerplants.ElectedMessage;
import com.example.powerplants.PlantInfoMessage;
import com.example.powerplants.Ack;

import ThermalPowerPlants.ThermalPowerPlant;
import ThermalPowerPlants.NetManager;

public class PlantServiceImpl extends PlantServiceGrpc.PlantServiceImplBase {

  /*  private final ThermalPowerPlants plant;

    public PlantServiceImpl(ThermalPowerPlants plant) {
        this.plant = plant;
    }
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
    */

    // Riferimento al "cervello" della centrale.
    private final ThermalPowerPlant plant;
    // Riferimento diretto al NetManager per comodità.
    private final NetManager netManager;

    public PlantServiceImpl(ThermalPowerPlant plant) {
        if (plant == null) {
            throw new IllegalArgumentException("ThermalPowerPlant instance cannot be null");
        }
        this.plant = plant;
        this.netManager = plant.getNetManager(); // Assumendo che TPP abbia un getter per il suo NetManager
        if (this.netManager == null) {
            throw new IllegalStateException("NetManager has not been initialized in ThermalPowerPlant");
        }
    }

    @Override
    public void sendElection(ElectionMessage request, StreamObserver<Ack> responseObserver) {
        // Quando riceve una chiamata "sendElection", la passa al metodo che gestisce
        // i messaggi di elezione in arrivo.
        netManager.handleElectionMessage(request);

        // Risponde con un ACK per confermare la ricezione.
        responseObserver.onNext(Ack.newBuilder().setMessage("OK").build());
        responseObserver.onCompleted();
    }

    @Override
    public void sendElected(ElectedMessage request, StreamObserver<Ack> responseObserver) {
        netManager.handleElectedMessage(request);
        responseObserver.onNext(Ack.newBuilder().setMessage("OK").build());
        responseObserver.onCompleted();
    }

    @Override
    public void announcePresence(PlantInfoMessage request, StreamObserver<Ack> responseObserver) {
        // PRIMISSIMA RIGA DEL METODO
        System.out.println("\nLOG (Plant " + plant.getId() + ", ServiceImpl): gRPC call 'announcePresence' RECEIVED from Plant " + request.getId() + ".\n");

        netManager.handleAnnouncePresence(request);

        Ack ack = Ack.newBuilder().setMessage("Welcome Plant " + request.getId()).build();
        responseObserver.onNext(ack);
        responseObserver.onCompleted();
    }

}
