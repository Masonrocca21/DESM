package ThermalPowerPlants;

public interface RpcCallback {

    /**
     * Chiamato quando l'operazione è stata completata con successo.
     */
    void onCompleted();

    /**
     * Chiamato quando l'operazione è fallita.
     * @param t L'eccezione o l'errore che ha causato il fallimento.
     */
    void onError(Throwable t);
}