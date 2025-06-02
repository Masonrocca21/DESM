package ThermalPowerPlants;

import com.example.powerplants.EnergyRequest;
import com.example.powerplants.EnergyResponse;
import com.example.powerplants.PlantServiceGrpc;
import com.example.powerplants.ElectionMessage;
import com.example.powerplants.ElectedMessage;
import io.grpc.ManagedChannel;

import java.util.*;

public class NetworkManager {
    private final int myId;
    private double myValue; // es. livello di inquinamento, o energia disponibile

    private PlantServiceGrpc.PlantServiceBlockingStub successorStub;
    private ManagedChannel successorChannel;
    private int nextId = 0;
    private boolean participant = false;


    public NetworkManager(int myId, double myValue,
                           PlantServiceGrpc.PlantServiceBlockingStub stub, ManagedChannel channel ) {
        this.myId = myId;
        this.myValue = myValue;
        this.successorStub = stub;
        this.successorChannel = channel;


        // Verifica che lo stub del successore sia fornito se c'è un canale
        if (this.successorChannel != null && this.successorStub == null) {
            System.err.println("NetworkManager (Plant " + this.myId + "): Warning - Successor channel is provided but stub is null.");
            // Potrebbe essere un problema, ma non fatale se la pianta è l'unica.
        }
        if (this.successorChannel == null && this.successorStub != null) {
            System.err.println("NetworkManager (Plant " + this.myId + "): Warning - Successor stub is provided but channel is null.");
        }

    }
    public void updateSuccessor(PlantServiceGrpc.PlantServiceBlockingStub newSuccessorStub,
                                ManagedChannel newSuccessorChannel) {
        System.out.println("NetworkManager (Plant " + this.myId + "): Updating successor");

        // Il chiamante (ThermalPowerPlant) è responsabile della gestione del ciclo di vita del vecchio canale.
        // NetworkManager si limita a usare i nuovi riferimenti.

        this.successorStub = newSuccessorStub;
        this.successorChannel = newSuccessorChannel;

        if (this.successorChannel != null && this.successorStub == null) {
            System.err.println("NetworkManager (Plant " + this.myId + "): Warning - Updated successor channel provided but stub is null.");
        }
        if (this.successorChannel == null && this.successorStub != null) {
            System.err.println("NetworkManager (Plant " + this.myId + "): Warning - Updated successor stub provided but channel is null.");
        }
        System.out.println("NetworkManager (Plant " + this.myId + "): Successor updated successfully");
    }

    public void startElection() {
        if (this.successorStub == null) {
            System.out.println("NetworkManager (Plant " + this.myId + "): Cannot start election, no successor defined. Am I alone?");
            // Se sono solo, potrei auto-eleggermi (se la logica lo prevede)
            handleSelfElection();
            return;
        }

        participant = true;
        ElectionMessage msg = ElectionMessage.newBuilder()
                .setInitiatorId(myId)
                .setCandidateId(myId)
                .setCandidateValue(myValue)
                .build();

        System.out.println("NetworkManager (Plant " + this.myId + "): Starting election with my value " + myValue);
        sendElectionToNext(msg);
    }

    public void handleSelfElection() {
        System.out.printf("Pianta %d si autoproclama vincitrice (valore %.3f)%n", myId, myValue);
        participant = false;

        ElectedMessage msg = ElectedMessage.newBuilder()
                .setWinnerId(myId)
                .setWinnerValue(myValue)
                .build();

        // chiama direttamente il metodo come se avesse ricevuto Elected
        onElectedMessage(msg);
    }

    public void onElectionMessage(ElectionMessage receivedMsg) {
        System.out.println("NetworkManager (Plant " + this.myId + "): Received election message. Candidate: " +
                receivedMsg.getCandidateId() + " with value " + receivedMsg.getCandidateValue());

        if (this.successorStub == null) {
            System.out.println("NetworkManager (Plant " + this.myId + "): No successor. Assuming I am the candidate " +
                    receivedMsg.getCandidateId() + " if IDs match, or the winner if the message originated from me.");
            // Se il messaggio è tornato a chi lo ha inviato e non ci sono altri, è il vincitore.
            // La logica di Chang & Roberts prevede che il messaggio giri. Se torna all'ID del candidato
            // nel messaggio E sono io, allora ho vinto.
            if (receivedMsg.getCandidateId() == this.myId) {
                System.out.println("NetworkManager (Plant " + this.myId + "): Election message returned to me. I ("+this.myId+") am the winner with value " + this.myValue);
                participant = false;
                sendElectedToNext(this.myId, this.myValue); // Invia il messaggio di eletto
            } else {
                // C'è un candidato migliore di me, ma non posso inoltrare.
                // Questo caso è strano in un anello funzionante. Potrebbe significare che sono l'ultimo
                // e il candidato è qualcun altro. L'elezione si concluderà quando il messaggio ELECTED girerà.
                System.out.println("NetworkManager (Plant " + this.myId + "): Received election for " + receivedMsg.getCandidateId() + " but no successor to forward.");
                // Potrei diventare non partecipante se il candidato è migliore
                if (receivedMsg.getCandidateValue() < this.myValue ||
                        (receivedMsg.getCandidateValue() == this.myValue && receivedMsg.getCandidateId() < this.myId)) { // Attenzione alla logica < vs > ID
                    participant = false;
                }
            }
            return;
        }

        // Logica di Chang & Roberts:
        // 1. Se il CandidateID nel messaggio è il mio ID, allora il mio messaggio ha fatto il giro
        //    e nessun altro era migliore. Io sono l'eletto.
        if (receivedMsg.getCandidateId() == this.myId) {
            System.out.println("NetworkManager (Plant " + this.myId + "): My election message returned. I am the winner!");
            participant = false; // Ho vinto
            // Invio il messaggio ELECTED
            sendElectedToNext(this.myId, this.myValue); // Devo passare anche il valore del vincitore
            return;
        }

        // 2. Se non sono io il candidato nel messaggio, confronto i valori.
        // La specifica del progetto dice: "lowest offered price", "plant with the highest ID wins" in caso di parità.
        // Il tuo codice attuale fa: myValue < msg.getCandidateValue() (quindi valore più basso vince)
        // E: myId < msg.getCandidateId() (quindi ID più basso vince in caso di parità). Questo è l'OPPOSTO.
        // Adattiamo alla specifica del progetto (assumendo myValue sia il prezzo):
        boolean iAmBetterCandidate = this.myValue < receivedMsg.getCandidateValue() ||
                (this.myValue == receivedMsg.getCandidateValue() && this.myId > receivedMsg.getCandidateId());

        ElectionMessage messageToForward;

        if (iAmBetterCandidate) {
            System.out.println("NetworkManager (Plant " + this.myId + "): My value " + this.myValue + " is better. Becoming candidate.");
            messageToForward = ElectionMessage.newBuilder(receivedMsg) // Copia i campi non modificati (es. requestId se presente)
                    .setCandidateId(this.myId)
                    .setCandidateValue(this.myValue)
                    .build();
            participant = true; // Sto partecipando attivamente
        } else {
            System.out.println("NetworkManager (Plant " + this.myId + "): Candidate " + receivedMsg.getCandidateId() +
                    " (value " + receivedMsg.getCandidateValue() + ") is better or equal. Forwarding.");
            messageToForward = receivedMsg; // Inoltro il messaggio così com'è
            participant = false; // Non sono più un partecipante attivo se c'è un candidato migliore
        }
        sendElectionToNext(messageToForward);
    }


    public void onElectedMessage(ElectedMessage electedMsg) {
        int winnerId = electedMsg.getWinnerId();
        double winnerValue = electedMsg.getWinnerValue(); // Recupera anche il valore del vincitore
        System.out.println("NetworkManager (Plant " + this.myId + "): Received ELECTED message. Winner: " + winnerId + " with value " + winnerValue);

        participant = false; // L'elezione è finita

        // Qui dovresti salvare chi è il vincitore e il suo valore se necessario per la TPP
        // es. this.thermalPowerPlantInstance.setElectionWinner(winnerId, winnerValue);

        if (winnerId != this.myId) { // Se non sono io il vincitore
            if (this.successorStub != null) { // E ho un successore a cui inoltrare
                sendElectedToNext(winnerId, winnerValue); // Inoltro il messaggio ELECTED
            } else {
                System.out.println("NetworkManager (Plant " + this.myId + "): I am not the winner, but no successor to forward ELECTED message.");
                // Questo significa che il messaggio ha fatto il giro e io sono l'ultimo a riceverlo
                // prima che torni (o sarebbe tornato) al vincitore.
            }
        } else {
            // Il messaggio ELECTED è tornato a me, il vincitore. Non devo fare altro con l'inoltro.
            System.out.println("NetworkManager (Plant " + this.myId + "): My ELECTED message completed the ring or I was notified as winner.");
        }
    }

    private void sendElectionToNext(ElectionMessage msgToSend) {
        if (this.successorStub == null) {
            System.err.println("NetworkManager (Plant " + this.myId + "): Cannot send ELECTION, successorStub is null.");
            // Potrebbe significare che sono solo e il messaggio era per me (già gestito in onElectionMessage)
            return;
        }
        try {
            // Assicurati che il tuo servizio gRPC in PlantServiceGrpc.PlantServiceBlockingStub
            // abbia un metodo che accetta ElectionMessage. Dal tuo codice sembra 'sendElection'.
            System.out.println("NetworkManager (Plant " + this.myId + "): Sending ELECTION to successor " +
                    " (Candidate: " + msgToSend.getCandidateId() + ", Value: " + msgToSend.getCandidateValue() + ")");
            this.successorStub.sendElection(msgToSend); // Usa lo stub del successore fornito
        } catch (Exception e) {
            System.err.println("NetworkManager (Plant " + this.myId + "): Error sending Election to successor " +
                     ": " + e.getMessage());
            // Qui potresti gestire la logica di un vicino irraggiungibile
        }
    }

    // Modificato per includere anche il winnerValue
    private void sendElectedToNext(int winnerId, double winnerValue) {
        if (this.successorStub == null) {
            System.err.println("NetworkManager (Plant " + this.myId + "): Cannot send ELECTED, successorStub is null.");
            return;
        }
        try {
            ElectedMessage electedMsgToSend = ElectedMessage.newBuilder()
                    .setWinnerId(winnerId)
                    .setWinnerValue(winnerValue) // Includi il valore del vincitore nel messaggio
                    // .setRequestId("some_unique_request_id")
                    .build();
            // Assicurati che il tuo servizio gRPC abbia un metodo 'sendElected'.
            System.out.println("NetworkManager (Plant " + this.myId + "): Sending ELECTED to successor " +
                    " (Winner: " + winnerId + ", Value: " + winnerValue + ")");
            this.successorStub.sendElected(electedMsgToSend); // Usa lo stub del successore fornito
        } catch (Exception e) {
            System.err.println("NetworkManager (Plant " + this.myId + "): Error sending Elected to successor " +
                     ": " + e.getMessage());
        }
    }

    public void setValue(double value) {
        this.myValue = value;
        System.out.println("NetworkManager (Plant " + this.myId + "): My election value updated to " + value);
    }

    public boolean isParticipant() {
        return participant;
    }
}
