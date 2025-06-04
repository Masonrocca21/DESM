package Simulators;

public class PollutionSensor extends Simulator {

    private final int mean = 125000;
    private final int variance = 5000;
    private static int ID = 1;

    public PollutionSensor(String id, Buffer buffer){
        super(id, "CO2", buffer);
    }

    //Use this constructor to initialize the Pollution Sensor simulator in your project
    public PollutionSensor(Buffer buffer){
        this("CO2-"+(ID++), buffer);
    }

    @Override
    public void run() {

        double i = rnd.nextInt();
        long waitingTime;

        while(!stopCondition){

            double co2 = getCO2Value(i);
            addMeasurement(co2);

            waitingTime = 2000;
            sensorSleep(waitingTime);

            i+=0.2;

        }

    }

    private double getCO2Value(double t){
        double gaussian = rnd.nextGaussian();
        return mean + Math.sqrt(variance) * gaussian;
    }
}
