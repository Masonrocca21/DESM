package RenewableEnergyProvider;

public class RenewableEnergyProvider {

    private int time = 0;
    public RenewableEnergyProvider() {}

    public int[] request (){
        time++; //Cpire come gestire l tempo che passa

        return new int[]{time - 1, (int)(Math.random()*(15000 - 5000 + 1) + 5000)};
    }
}
