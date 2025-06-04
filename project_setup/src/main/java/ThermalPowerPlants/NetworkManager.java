package ThermalPowerPlants;

import com.example.powerplants.EnergyRequest;
import com.example.powerplants.EnergyResponse;
import com.example.powerplants.PlantServiceGrpc;
import com.example.powerplants.ElectionMessage;
import com.example.powerplants.ElectedMessage;
import io.grpc.ManagedChannel;
import ThermalPowerPlants.ThermalPowerPlants;

import java.util.*;

public class NetworkManager {
    private final int myId;
    private double myValue; // es. livello di inquinamento, o energia disponibile

    private volatile PlantServiceGrpc.PlantServiceBlockingStub successorStub;
    private volatile ManagedChannel successorChannel;
    private boolean participant = false;
    private final ThermalPowerPlants plantInstance;
    private double currentElectionKWh;


    public NetworkManager(ThermalPowerPlants plantOwner, int myId, double myValue,
                           PlantServiceGrpc.PlantServiceBlockingStub stub, ManagedChannel channel ) {
        this.myId = myId;
        this.myValue = myValue;
        this.successorStub = stub;
        this.successorChannel = channel;
        this.plantInstance = plantOwner;

        System.out.println("NetworkManager (Plant " + this.myId + ") CONSTRUCTOR: this.successorStub is null? " + (this.successorStub == null) + " (Passed stub was null? " + (stub == null) + ")");


        // Verifica che lo stub del successore sia fornito se c'è un canale
        if (this.successorChannel != null && this.successorStub == null) {
            System.err.println("NetworkManager (Plant " + this.myId + "): Warning - Successor channel is provided but stub is null.");
            // Potrebbe essere un problema, ma non fatale se la pianta è l'unica.
        }
        if (this.successorChannel == null && this.successorStub != null) {
            System.err.println("NetworkManager (Plant " + this.myId + "): Warning - Successor stub is provided but channel is null.");
        }

    }
    public synchronized void updateSuccessor(PlantServiceGrpc.PlantServiceBlockingStub newSuccessorStub,
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

    public void startElection(double kWh) {
        if (this.successorStub == null) {
            System.out.println("NetworkManager (Plant " + this.myId + "): Cannot start election, no successor defined. Am I alone?");
            // Se sono solo, potrei auto-eleggermi (se la logica lo prevede)
            handleSelfElection();
            return;
        }

        // ...
        if (this.plantInstance != null && !this.plantInstance.isReadyForElection()){
            System.out.println("NetworkManager (Plant " + this.myId + "): Plant is not generally ready for election logic yet. Aborting startElection.");
            return;
        }

        this.currentElectionKWh = kWh;
        this.participant = true; // Questo 'participant' è specifico del NetworkManager per l'elezione corrente

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

        try {
            // DELAY SIMULATO
            long processingDelay = 2000 + (long)(Math.random() * 2000); // Delay tra 30 e 180 ms
            System.out.println("NetworkManager (Plant " + this.myId + "): Simulating processing delay of " + processingDelay + "ms for received ELECTION from candidate " + receivedMsg.getCandidateId());
            Thread.sleep(processingDelay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("NetworkManager (Plant " + this.myId + "): Sleep interrupted during ELECTION processing.");
        }

        // Stato della pianta proprietaria
        boolean isOwningPlantBusy = false;
        if (this.plantInstance != null) {
            isOwningPlantBusy = this.plantInstance.isBusyProducing();
        } else {
            System.err.println("NetworkManager (Plant " + this.myId + "): plantInstance is null in onElectionMessage!");
            // In questo caso, non possiamo determinare lo stato busy. Per sicurezza, non partecipiamo attivamente.
            // Ma dobbiamo comunque provare a inoltrare se abbiamo uno stub.
        }

        // Cattura lo stub e le info del successore in modo sincronizzato
        // Questo è importante per mitigare il CANCELLED error, come discusso prima
        PlantServiceGrpc.PlantServiceBlockingStub stubToUseForForwarding;
        ManagedChannel channelInUseForForwarding;

        synchronized (this) { // Sincronizza l'accesso allo stato del successore
            stubToUseForForwarding = this.successorStub;
            channelInUseForForwarding = this.successorChannel;
        }

        if (stubToUseForForwarding == null) {
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

            // ----- CHIAMATA A handleElectionWinAndStartProduction -----
            if (this.plantInstance != null) { // plantInstance è il riferimento a ThermalPowerPlants
                this.plantInstance.handleElectionWinAndStartProduction(this.currentElectionKWh);
            } else {
                System.err.println("NetworkManager (Plant " + this.myId + "): CRITICAL - plantInstance is null, cannot start production for win!");
            }
            // ---------------------------------------------------------


            // Invio il messaggio ELECTED
            sendElectedToNext(this.myId, this.myValue); // Devo passare anche il valore del vincitore
            return;
        }

        // Determina se questa pianta DEVE solo inoltrare o può partecipare attivamente
        // Una pianta nuova o non ancora completamente inizializzata per le elezioni (dal punto di vista di questo NM)
        // potrebbe non avere this.currentElectionRequestId impostato per la richiesta ricevuta,
        // o this.participant potrebbe essere false.
        boolean shouldOnlyForward = false;
        String forwardReason = "";

        if (isOwningPlantBusy) {
            shouldOnlyForward = true;
            forwardReason = "owning plant is busy";
        }


        if (shouldOnlyForward) {
            System.out.println("NetworkManager (Plant " + this.myId + "): Forwarding ELECTION for request '" +
                    receivedMsg.getInitiatorId() + "' as is (candidate " + receivedMsg.getCandidateId() +
                    ") because " + forwardReason + ".");
            sendElectionToNext(receivedMsg); // Inoltra il messaggio. sendElectionToNext usa lo stub corrente.
            return;
        }

        // Se arrivo qui, NON sono occupato E STO attivamente partecipando.
        // Quindi, confronto la mia offerta.

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
        PlantServiceGrpc.PlantServiceBlockingStub stubToUse;
        ManagedChannel channelToUse;

        // Blocco sincronizzato per leggere lo stato corrente del successore
        synchronized (this) {
            stubToUse = this.successorStub;
            channelToUse = this.successorChannel;
        }

        if (stubToUse == null) {
            System.err.println("NetworkManager (Plant " + this.myId + "): Cannot send ELECTION, successorStub is null.");
            // Potrebbe significare che sono solo e il messaggio era per me (già gestito in onElectionMessage)
            return;
        }
        try {
            // Assicurati che il tuo servizio gRPC in PlantServiceGrpc.PlantServiceBlockingStub
            // abbia un metodo che accetta ElectionMessage. Dal tuo codice sembra 'sendElection'.
            System.out.println("NetworkManager (Plant " + this.myId + "): Sending ELECTION to successor " +
                    " (Candidate: " + msgToSend.getCandidateId() + ", Value: " + msgToSend.getCandidateValue() + ")");
            long networkDelay = 50 + (long)(Math.random() * 200); // Delay tra 50 e 250 ms
            Thread.sleep(networkDelay);

            stubToUse.sendElection(msgToSend); // Usa lo stub del successore fornito
        } catch (Exception e) {
            if (e instanceof io.grpc.StatusRuntimeException) {
                io.grpc.StatusRuntimeException sre = (io.grpc.StatusRuntimeException) e;
                if (sre.getStatus().getCode() == io.grpc.Status.Code.UNAVAILABLE ||
                        sre.getStatus().getCode() == io.grpc.Status.Code.CANCELLED) {
                    System.err.println("NetworkManager (Plant " + this.myId + "): ELECTION to successor FAILED (" + sre.getStatus().getCode() + "). " +
                            "Channel used: " + channelToUse +
                            ". Successor might have changed or channel was closed. Msg: " + sre.getMessage());
                    // QUI: Potresti voler controllare se this.successorChannel (quello attuale) è diverso da channelSnapshot.
                    // Se sì, il successore è cambiato. Potresti ritentare con il nuovo successore (con cautela per evitare loop).
                    // Per ora, logghiamo solo l'errore.
                    synchronized(this) {
                        if (this.successorChannel != channelToUse) {
                            System.err.println("NetworkManager (Plant " + this.myId + "): Successor has changed since attempting to send. New successor ID: ");
                        }
                    }
                    return; // Non tentare più su questo canale/stub
                }
            }
            System.err.println("NetworkManager (Plant " + this.myId + "): Generic error sending Election to successor " +
                     ": " + e.getMessage());
        }
    }

    // Modificato per includere anche il winnerValue
    private void sendElectedToNext(int winnerId, double winnerValue) {
        PlantServiceGrpc.PlantServiceBlockingStub stubToUse;

        // Blocco sincronizzato per leggere lo stato corrente del successore
        synchronized (this) {
            stubToUse = this.successorStub;
        }

        if (stubToUse == null) {
            System.err.println("NetworkManager (Plant " + this.myId + "): Cannot send ELECTED, successorStub is null.");
            return;
        }
        try {
            ElectedMessage electedMsgToSend = ElectedMessage.newBuilder()
                    .setWinnerId(winnerId)
                    .setWinnerValue(winnerValue) // Includi il valore del vincitore nel messaggio
                    .build();
            // Assicurati che il tuo servizio gRPC abbia un metodo 'sendElected'.
            System.out.println("NetworkManager (Plant " + this.myId + "): Sending ELECTED to successor " +
                    " (Winner: " + winnerId + ", Value: " + winnerValue + ")");
            stubToUse.sendElected(electedMsgToSend); // Usa lo stub del successore fornito
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

    public void setCurrentElectionKWh(double kWh) {
        this.currentElectionKWh = kWh;
    }
}
