package client;

public class PollutionStatsResponse {

    private double averageCo2;
    private int readingsCount;

    /**
     * Costruttore vuoto.
     * Necessario per alcune librerie di (de)serializzazione come Jackson
     * se deve creare un'istanza e poi popolarla tramite setter.
     */
    public PollutionStatsResponse() {
    }

    /**
     * Costruttore con parametri per inizializzare l'oggetto.
     * @param averageCo2 La media calcolata dei livelli di CO2.
     * @param readingsCount Il numero di singole medie usate per calcolare l'averageCo2.
     */
    public PollutionStatsResponse(double averageCo2, int readingsCount) {
        this.averageCo2 = averageCo2;
        this.readingsCount = readingsCount;
    }

    // Getters
    public double getAverageCo2() {
        return averageCo2;
    }

    public int getReadingsCount() {
        return readingsCount;
    }

    // Setters (opzionali se usi solo il costruttore con parametri per creare l'oggetto,
    // ma utili se una libreria come Jackson ha bisogno di un costruttore vuoto e poi usa i setter)
    public void setAverageCo2(double averageCo2) {
        this.averageCo2 = averageCo2;
    }

    public void setReadingsCount(int readingsCount) {
        this.readingsCount = readingsCount;
    }

    @Override
    public String toString() {
        return "PollutionStatsResponse{" +
                "averageCo2=" + averageCo2 +
                ", readingsCount=" + readingsCount +
                '}';
    }
}