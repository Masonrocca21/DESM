package ThermalPowerPlants;

public class PendingRequest {

    // Rimuovi 'final' per permettere la deserializzazione
    private String requestId;
    private double kwh;
    private long createdAt; // Nome più chiaro di 'receivedAt'

    // --- MODIFICA 1: Aggiungi un costruttore vuoto ---
    // Questo è OBBLIGATORIO per la libreria Jackson (usata da Spring)
    public PendingRequest() {
    }

    // Il tuo costruttore originale, leggermente modificato
    public PendingRequest(String requestId, double kwh) {
        this.requestId = requestId;
        this.kwh = kwh;
        this.createdAt = System.currentTimeMillis();
    }

    // --- MODIFICA 2: Aggiungi Getter e Setter per TUTTI i campi ---
    // Questo permette a Jackson di leggere e scrivere i valori.

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public double getKwh() {
        return kwh;
    }

    public void setKwh(double kwh) {
        this.kwh = kwh;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    // Il campo 'isBeingHandled' non serve più in questo nuovo modello,
    // perché la logica di "chi sta gestendo cosa" è ora centralizzata
    // nell'Administrator. Possiamo rimuoverlo per pulizia.
}