package ThermalPowerPlants;

public class PendingRequest {

    private String requestId;
    private double kwh;
    private long createdAt;

    public PendingRequest() {
    }

    public PendingRequest(String requestId, double kwh) {
        this.requestId = requestId;
        this.kwh = kwh;
        this.createdAt = System.currentTimeMillis();
    }

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

}