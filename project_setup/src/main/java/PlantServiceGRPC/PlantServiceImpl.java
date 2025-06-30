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

    private final ThermalPowerPlant plant;
    private final NetManager netManager;

    public PlantServiceImpl(ThermalPowerPlant plant) {
        if (plant == null) {
            throw new IllegalArgumentException("ThermalPowerPlant instance cannot be null");
        }
        this.plant = plant;
        this.netManager = plant.getNetManager();
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

        System.out.println("\nLOG (Plant " + plant.getId() + ", ServiceImpl): gRPC call 'announcePresence' RECEIVED from Plant " + request.getId() + ".\n");

        netManager.handleAnnouncePresence(request);

        Ack ack = Ack.newBuilder().setMessage("Welcome Plant " + request.getId()).build();
        responseObserver.onNext(ack);
        responseObserver.onCompleted();
    }

}
