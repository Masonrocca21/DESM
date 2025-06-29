package PlantServiceGRPC;

import io.grpc.stub.StreamObserver;
import com.example.powerplants.PlantServiceGrpc;
import com.example.powerplants.ElectionMessage;
import com.example.powerplants.ElectedMessage;
import com.example.powerplants.PlantInfoMessage;
import com.example.powerplants.BatonMessage;
import com.example.powerplants.Ack;

import ThermalPowerPlants.ThermalPowerPlant;
import ThermalPowerPlants.NetManager;

public class PlantServiceImpl extends PlantServiceGrpc.PlantServiceImplBase {

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

    @Override
    public void passElectionBaton(BatonMessage request, StreamObserver<Ack> responseObserver) {
        // La primissima cosa da fare è loggare che il messaggio è arrivato.
        System.out.println("LOG (Plant " + plant.getId() + ", ServiceImpl): gRPC call 'passElectionBaton' RECEIVED for request " + request.getRequestId());

        // Delega la logica al NetManager, che sa cosa fare con il testimone.
        netManager.handlePassBaton(request);

        // Rispondi con un ACK per confermare la ricezione.
        responseObserver.onNext(Ack.newBuilder().setMessage("Baton received").build());
        responseObserver.onCompleted();
    }

}
