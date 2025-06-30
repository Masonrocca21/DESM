package ThermalPowerPlants;

// Importa la classe generata da Protobuf
import powerplants.PlantInfoMessage;
import java.util.Objects;


/**
 * PeerInfo è una classe di dati immutabile che rappresenta le informazioni
 * di una centrale termica (un peer) nella rete.
 * Contiene l'ID, l'indirizzo e la porta necessari per la comunicazione.
 */
public final class PeerInfo {

    private int id;
    private String address;
    private int port;

    public PeerInfo() {}


    /**
     * Costruttore per creare una nuova istanza di PeerInfo.
     * @param id L'ID univoco della centrale.
     * @param address L'indirizzo di rete (es. "localhost").
     * @param port La porta su cui la centrale ascolta le chiamate gRPC.
     */
    public PeerInfo(int id, String address, int port) {
        this.id = id;
        this.address = address;
        this.port = port;
    }

    // --- Getters ---

    public int getId() {
        return id;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    // --- Metodi di Conversione Protobuf ---

    /**
     * Converte questa istanza di PeerInfo nel suo formato Protobuf (PlantInfoMessage).
     * @return Un'istanza di PlantInfoMessage.
     */
    public PlantInfoMessage toProtobuf() {
        return PlantInfoMessage.newBuilder()
                .setId(this.id)
                .setAddress(this.address)
                .setPortNumber(this.port)
                .build();
    }

    /**
     * Metodo factory statico per creare un'istanza di PeerInfo da un messaggio Protobuf.
     * @param proto L'oggetto PlantInfoMessage ricevuto dalla rete.
     * @return Una nuova istanza di PeerInfo.
     */
    public static PeerInfo fromProtobuf(PlantInfoMessage proto) {
        return new PeerInfo(
                proto.getId(),
                proto.getAddress(),
                proto.getPortNumber()
        );
    }

    // --- Metodi Standard di Object ---

    /**
     * Fornisce una rappresentazione testuale dell'oggetto, utile per il logging.
     */
    @Override
    public String toString() {
        return "PeerInfo{" +
                "id=" + id +
                ", address='" + address + '\'' +
                ", port=" + port +
                '}';
    }

    /**
     * Controlla l'uguaglianza tra due oggetti PeerInfo.
     * Due peer sono considerati uguali se hanno lo stesso ID.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PeerInfo peerInfo = (PeerInfo) o;
        return id == peerInfo.id;
    }

    /**
     * Calcola l'hash code basandosi sull'ID.
     * È essenziale per il corretto funzionamento in strutture dati come HashMap.
     */
    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}