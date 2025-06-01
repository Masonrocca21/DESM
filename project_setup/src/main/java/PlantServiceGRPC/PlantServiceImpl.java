package PlantServiceGRPC;

import io.grpc.stub.StreamObserver;
import com.example.powerplants.PlantServiceGrpc;
import com.example.powerplants.EnergyRequest;
import com.example.powerplants.EnergyResponse;

import ThermalPowerPlants.ThermalPowerPlants;

public class PlantServiceImpl extends PlantServiceGrpc.PlantServiceImplBase {

    private final ThermalPowerPlants plant;

    public PlantServiceImpl(ThermalPowerPlants plant) {
        this.plant = plant;
    }
    @Override
    public void handleEnergyRequest(EnergyRequest request, StreamObserver<EnergyResponse> responseObserver) {
        // Logica per decidere se soddisfare la richiesta
        int requiredEnergy = request.getRequiredEnergy();
        int senderId = request.getSenderId();

        // Simuliamo una risposta semplice
        boolean canHandle = requiredEnergy <= 100; // per esempio
        String message = canHandle ? "Energia disponibile" : "Energia insufficiente";

        EnergyResponse response = EnergyResponse.newBuilder()
                .setCanHandle(true)
                .setMessage("Posso gestire")
                .setPollution(plant.getPollutionLevel())
                .setAvailableEnergy(plant.getAvailableEnergy())
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
