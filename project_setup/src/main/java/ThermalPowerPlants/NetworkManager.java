package ThermalPowerPlants;

import com.example.powerplants.PlantServiceGrpc;
import com.example.powerplants.ElectionMessage;
import com.example.powerplants.ElectedMessage;
import io.grpc.ManagedChannel;

public class NetworkManager {
    private final int myId;
    private boolean participant = false;
    private final ThermalPowerPlants plantInstance;

    public NetworkManager(ThermalPowerPlants plantOwner, int myId, double myValue,
                           PlantServiceGrpc.PlantServiceBlockingStub stub, ManagedChannel channel ) {
        this.myId = myId;
        this.plantInstance = plantOwner;

        System.out.println("NetworkManager (Plant " + this.myId + ") CONSTRUCTOR. Will fetch stub from TPP.");

    }

    public void startElection(String requestID, double kWh) {

        PlantServiceGrpc.PlantServiceBlockingStub currentStub = null;

        if (this.plantInstance != null && !this.plantInstance.isReadyForElection()){
            System.out.println("NetworkManager (Plant " + this.myId + "): Plant is not generally ready for election logic yet. Aborting startElection.");
            return;
        }

        if (requestID == null || requestID.isEmpty()) {
            System.err.println("NetworkManager (Plant " + this.myId + "): ERROR - Attempting to start election with no currentRequestId set!");
            return;
        }

        if (!plantInstance.hasPreparedPriceForRequest(requestID)) {
            System.err.println("NM (" + this.myId + "): CRITICAL - TPP has not prepared a price for request '" + requestID + "'. Aborting startElection.");
            // Questo indica un problema nel flusso logico se accade.
            return;
        }
        double myValue = plantInstance.getCurrentElectionPrice(); // Prende il prezzo dalla TPP

        if (plantInstance != null) {
            currentStub = plantInstance.getCurrentSuccessorStub(); // Metodo synchronized in TPP
        }

        if (currentStub == null) {
            System.out.println("NetworkManager (Plant " + this.myId + "): Cannot start election, no successor defined. Am I alone?");
            // Se sono solo, potrei auto-eleggermi (se la logica lo prevede)
            handleSelfElection(requestID, kWh, myValue);
            return;
        }

        this.participant = true; // Questo 'participant' è specifico del NetworkManager per l'elezione corrente

        ElectionMessage msg = ElectionMessage.newBuilder()
                .setCandidateId(myId)
                .setCandidateValue(myValue)
                .setCurrentElectionRequestId(requestID) // Usa il requestId passato
                .setRequiredKwh(kWh)                  // Imposta il nuovo campo kWh
                .build();

        System.out.println("NetworkManager (Plant " + this.myId + "): Starting election with my value " + myValue);
        sendElectionToNext(msg, currentStub);
    }

    public void handleSelfElection(String requestId, double kWh, double myValue) {
        System.out.printf("Pianta %d si autoproclama vincitrice (valore %.3f)%n", myId, myValue);
        participant = false;

        ElectedMessage msg = ElectedMessage.newBuilder()
                .setWinnerId(myId)
                .setWinnerValue(myValue)
                .setCurrentElectionRequestId(requestId)
                .setRequiredKwh(kWh) // Imposta il nuovo campo
                .build();

        // chiama direttamente il metodo come se avesse ricevuto Elected
        onElectedMessage(msg);
    }

    public void onElectionMessage(ElectionMessage receivedMsg) {

        String receivedRequestId = receivedMsg.getCurrentElectionRequestId();
        double receivedKwh = receivedMsg.getRequiredKwh(); // Prendi kWh dal messaggio

        System.out.println("NetworkManager (Plant " + this.myId + "): Received election message. Candidate: " +
                receivedMsg.getCandidateId() + " with value " + receivedMsg.getCandidateValue());

        try {
            // DELAY SIMULATO
            long processingDelay = 100 + (long)(Math.random() * 250); // Delay tra 100 e 350 ms
            System.out.println("NetworkManager (Plant " + this.myId + "): Simulating processing delay of " + processingDelay + "ms for received ELECTION from candidate " + receivedMsg.getCandidateId());
            Thread.sleep(processingDelay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("NetworkManager (Plant " + this.myId + "): Sleep interrupted during ELECTION processing.");
        }

        // Cattura lo stub e le info del successore in modo sincronizzato
        // Questo è importante per mitigare il CANCELLED error, come discusso prima
        PlantServiceGrpc.PlantServiceBlockingStub stubToUseForForwarding = null;
        ManagedChannel channelInUseForForwarding;

        synchronized (this) { // Sincronizza l'accesso allo stato del successore
            if(plantInstance != null) {
                stubToUseForForwarding = plantInstance.getCurrentSuccessorStub();
            }
        }

        // PRIMO CONTROLLO: L'elezione per questa requestId è già conclusa?
        if (plantInstance.isElectionConcluded(receivedRequestId)) {
            System.out.println("NM (" + this.myId + "): Election for request '" + receivedRequestId +
                    "' ALREADY CONCLUDED. Candidate in msg: " + receivedMsg.getCandidateId() +
                    ". My ID: " + this.myId);
            // Se l'elezione è conclusa, non si dovrebbe più partecipare attivamente o inoltrare il proprio messaggio.
            // Si potrebbe inoltrare passivamente il messaggio di un ALTRO candidato se non si è ancora visto l'Elected di quel candidato.
            // MA, se il messaggio è il MIO (candidateId == myId) e l'elezione è conclusa, lo devo assorbire.
            if (receivedMsg.getCandidateId() == this.myId) {
                System.out.println("NM (" + this.myId + "): Discarding my own ELECTION message for already concluded request '" + receivedRequestId + "'.");
            } else if (stubToUseForForwarding != null) {
                // Inoltra passivamente il messaggio di un ALTRO candidato, anche se l'elezione è conclusa.
                // Questo permette al messaggio "orfano" di un'altra pianta di completare il suo giro e essere assorbito.
                System.out.println("NM (" + this.myId + "): Passively forwarding ELECTION message from candidate " + receivedMsg.getCandidateId() +
                        " for concluded request '" + receivedRequestId + "'.");
                sendElectionToNext(receivedMsg, stubToUseForForwarding);
            }
            return; // Termina l'elaborazione per questa elezione conclusa
        }

        // CASO 1: Il messaggio è tornato al suo mittente originale (questa pianta)
        if (receivedMsg.getCandidateId() == this.myId) {

            // Controlla se la TPP si considera ancora attivamente partecipante a QUESTA elezione
            // OPPURE se il prezzo nel messaggio corrisponde al prezzo con cui ha INIZIATO l'elezione
            // (anche se ora potrebbe averlo pulito perché si è già dichiarata vincitrice un istante prima)
            boolean isStillActivelyParticipatingThisRequest = receivedRequestId.equals(plantInstance.getActivelyParticipatingInElectionForRequestId());

            // Verifica che il prezzo e la requestId corrispondano a ciò che questa pianta avrebbe inviato.
            // Questo implica che la TPP deve avere un prezzo preparato per questa requestId.
            if (isStillActivelyParticipatingThisRequest && plantInstance.hasPreparedPriceForRequest(receivedRequestId) &&
                    Math.abs(receivedMsg.getCandidateValue() - plantInstance.getCurrentElectionPrice()) < 0.0001) { // Confronto double

                System.out.println("NM (" + this.myId + "): My ELECTION message for request '" + receivedRequestId + "' returned. I am the winner!");
                participant = false; // Ho vinto

                // Ottieni il prezzo vincente PRIMA di pulirlo!
                double winningPrice = plantInstance.getCurrentElectionPrice();

                plantInstance.handleElectionWinAndStartProduction(receivedRequestId, receivedKwh);

                if (stubToUseForForwarding != null) {
                    sendElectedToNext(this.myId, winningPrice, receivedRequestId, receivedKwh, stubToUseForForwarding);
                } else {
                    System.out.println("NM (" + this.myId + "): I won (request '"+receivedRequestId+"'), and I'm the only one. No ELECTED message to send.");
                }
                return;
            } else {
                System.out.println("NM (" + this.myId + "): Received ELECTION with my ID ("+this.myId+") for request '"+receivedRequestId +
                        "' but current price/requestId in TPP (" + String.format("%.3f", plantInstance.getCurrentElectionPrice()) + " for '" + plantInstance.getRequestIdForCurrentPrice() +
                        "') does not match message value ("+ String.format("%.3f", receivedMsg.getCandidateValue()) +"). Forwarding as is.");
                // Questo scenario è insolito se la logica di TPP è corretta. Potrebbe essere un messaggio vecchio.
                if (stubToUseForForwarding != null) {
                    sendElectionToNext(receivedMsg, stubToUseForForwarding);
                } else {
                    System.err.println("NM (" + this.myId + "): ELECTION for me ("+this.myId+", req '"+receivedRequestId+
                            "') with mismatch, and no successor. Election potentially stuck.");
                }
                return;
            }
        }

        // CASO 2: Determinare se inoltrare passivamente o partecipare attivamente.
        // Condizioni per l'inoltro passivo:

        boolean plantIsBusyProducing = plantInstance.isBusyProducing();
        String activeReqId = plantInstance.getActivelyParticipatingInElectionForRequestId();
        boolean plantInDifferentActiveElection = activeReqId != null && !receivedRequestId.equals(activeReqId);

        boolean hasPreparedPrice = plantInstance.hasPreparedPriceForRequest(receivedRequestId);
        boolean plantHasNoPriceForThisRequest = !hasPreparedPrice;


        if (plantIsBusyProducing || plantInDifferentActiveElection || plantHasNoPriceForThisRequest) {
            String reason = "";
            if (plantIsBusyProducing) reason += "plant is busy producing (for '" + plantInstance.currentProductionRequestId + "'); ";
            if (plantInDifferentActiveElection) reason += "plant actively in different election ('" + plantInstance.getActivelyParticipatingInElectionForRequestId() + "'); ";
            if (plantHasNoPriceForThisRequest) reason += "plant has no price prepared for this request ('" + receivedRequestId + "'); ";

            System.out.println("NM (" + this.myId + "): Forwarding ELECTION (candidate " + receivedMsg.getCandidateId() +
                    ", req '" + receivedRequestId + "') as is because: " + reason.trim());
            participant = false; // Non partecipo attivamente a questa tornata

            if (stubToUseForForwarding != null) {
                sendElectionToNext(receivedMsg, stubToUseForForwarding);
            } else {
                System.out.println("NM (" + this.myId + "): Was to forward ELECTION for req '"+receivedRequestId +
                        "' (candidate "+receivedMsg.getCandidateId()+") due to: " + reason.trim() +
                        " but NO SUCCESSOR. Election may be stuck if this node isn't the intended winner.");
            }
            return;
        }

        // CASO 3: Partecipazione attiva.
        // La pianta non è busy, non è in un'altra elezione, e HA un prezzo per questa requestId.
        // Assicura che la TPP sia marcata come partecipante attiva per QUESTA elezione.
        // (TPP.prepareForNewElection dovrebbe averlo già fatto, ma una chiamata qui è una salvaguardia).
        System.out.println("NM (" + this.myId + "): Entering CASO 3 for req " + receivedRequestId);
        System.out.println("NM (" + this.myId + "): PRE-CALL plantInstance.startedActiveParticipationInElection() for req " + receivedRequestId);
        plantInstance.startedActiveParticipationInElection(receivedRequestId);
        System.out.println("NM (" + this.myId + "): POST-CALL plantInstance.startedActiveParticipationInElection()");

        System.out.println("NM (" + this.myId + "): PRE-CALL plantInstance.getCurrentElectionPrice() for req " + receivedRequestId);
        double myCurrentPriceForThisRequest = plantInstance.getCurrentElectionPrice();
        System.out.println("NM (" + this.myId + "): POST-CALL plantInstance.getCurrentElectionPrice(), result: " + myCurrentPriceForThisRequest);

        ElectionMessage messageToForward;
        // Logica di Chang & Roberts: miglior candidato ha il valore più basso (prezzo),
        // e in caso di parità, l'ID più alto.
        boolean iAmBetterCandidate = myCurrentPriceForThisRequest < receivedMsg.getCandidateValue() ||
                (Math.abs(myCurrentPriceForThisRequest - receivedMsg.getCandidateValue()) < 0.0001 && this.myId > receivedMsg.getCandidateId());

        if (iAmBetterCandidate) {
            System.out.println("NM (" + this.myId + "): My price " + String.format("%.3f", myCurrentPriceForThisRequest) +
                    " for request '" + receivedRequestId + "' is better. Becoming candidate.");
            messageToForward = ElectionMessage.newBuilder(receivedMsg) // Copia requestId e kWh
                    .setCandidateId(this.myId)
                    .setCandidateValue(myCurrentPriceForThisRequest)
                    .build();
            participant = true; // NM si considera partecipante attivo
        } else {
            System.out.println("NM (" + this.myId + "): Candidate " + receivedMsg.getCandidateId() +
                    " (price " + String.format("%.3f", receivedMsg.getCandidateValue()) + ") for request '" + receivedRequestId +
                    "' is better or equal. Forwarding.");
            messageToForward = receivedMsg; // Inoltra il messaggio del candidato migliore
            participant = false; // NM non è più il candidato attivo per questa tornata
            // Non chiamo TPP.electionProcessConcludedForRequest qui, l'elezione non è finita.
        }

        if (stubToUseForForwarding != null) {
            sendElectionToNext(messageToForward, stubToUseForForwarding);
        } else {
            // Se non c'è successore e non sono il vincitore (il CASO 1 non è scattato), l'elezione è bloccata.
            // Questo non dovrebbe accadere in un anello con >1 nodo se il CASO 1 è corretto.
            // Se sono l'unico nodo, il CASO 1 o handleSelfElection dovrebbero coprirlo.
            System.err.println("NM (" + this.myId + "): Processed ELECTION for req '"+receivedRequestId+"', result: candidate " +
                    messageToForward.getCandidateId() + ", but NO SUCCESSOR to forward to. Election may be stuck.");
        }
    }


    public void onElectedMessage(ElectedMessage electedMsg) {
        int winnerId = electedMsg.getWinnerId();
        double winnerValue = electedMsg.getWinnerValue(); // Recupera anche il valore del vincitore
        String requestId = electedMsg.getCurrentElectionRequestId();
        double kwh = electedMsg.getRequiredKwh(); // Prendi kWh dal messaggio

        System.out.println("NetworkManager (Plant " + this.myId + "): Received ELECTED message. Winner: " + winnerId + " with value " + winnerValue);

        participant = false; // L'elezione è finita
        plantInstance.electionProcessConcludedForRequest(requestId);
        plantInstance.markElectionAsConcluded(requestId);

        PlantServiceGrpc.PlantServiceBlockingStub stubToUseForForwarding = null;
        if (plantInstance != null) {
            stubToUseForForwarding = plantInstance.getCurrentSuccessorStub();
        }

        // Qui dovresti salvare chi è il vincitore e il suo valore se necessario per la TPP
        // es. this.thermalPowerPlantInstance.setElectionWinner(winnerId, winnerValue);

        if (winnerId != this.myId) { // Se non sono io il vincitore
            if (stubToUseForForwarding != null) { // E ho un successore a cui inoltrare
                sendElectedToNext(winnerId, winnerValue, requestId, kwh, stubToUseForForwarding); // Inoltro il messaggio ELECTED
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

    private void sendElectionToNext(ElectionMessage msgToSend, PlantServiceGrpc.PlantServiceBlockingStub stubToUse) {

        if (stubToUse == null) {
            System.err.println("NetworkManager (Plant " + this.myId + "): Cannot send ELECTION, successorStub is null.");
            // Potrebbe significare che sono solo e il messaggio era per me (già gestito in onElectionMessage)
            return;
        }
        try {
            System.out.println("NetworkManager (Plant " + this.myId + "): Sending ELECTION to successor " +
                    " (Candidate: " + msgToSend.getCandidateId() + ", Value: " + msgToSend.getCandidateValue() + ")");
            long networkDelay = 50 + (long)(Math.random() * 200); // Delay tra 50 e 250 ms
            Thread.sleep(networkDelay);

            stubToUse.sendElection(msgToSend); // Usa lo stub del successore fornito
        } catch (Exception e) {
            // Gestione più specifica delle eccezioni gRPC se necessario
            System.err.println("NM (" + this.myId + "): Error sending ELECTION for req '" + msgToSend.getCurrentElectionRequestId() +
                    "' to successor: " + e.getMessage());
            // Considerare una logica di fallback o notifica alla TPP se l'invio fallisce ripetutamente.
        }
    }

    // Modificato per includere anche il winnerValue
    private void sendElectedToNext(int winnerId, double winnerValue, String requestId, double kwh, PlantServiceGrpc.PlantServiceBlockingStub stubToUse) {

        if (stubToUse == null) {
            System.err.println("NetworkManager (Plant " + this.myId + "): Cannot send ELECTED, successorStub is null.");
            return;
        }
        try {
            ElectedMessage electedMsgToSend = ElectedMessage.newBuilder()
                    .setWinnerId(winnerId)
                    .setWinnerValue(winnerValue)
                    .setCurrentElectionRequestId(requestId)
                    .setRequiredKwh(kwh) // Includi kWh
                    .build();

            System.out.println("NetworkManager (Plant " + this.myId + "): Sending ELECTED to successor " +
                    " (Winner: " + electedMsgToSend.getWinnerId() + ", Value: " + electedMsgToSend.getWinnerValue() + ")");
            long networkDelay = 50 + (long)(Math.random() * 200); // Delay tra 50 e 250 ms
            Thread.sleep(networkDelay);

            // Assicurati che il tuo servizio gRPC abbia un metodo 'sendElected'.
            System.out.println("NetworkManager (Plant " + this.myId + "): Sending ELECTED to successor " +
                    " (Winner: " + winnerId + ", Value: " + winnerValue + ")");
            stubToUse.sendElected(electedMsgToSend); // Usa lo stub del successore fornito
        } catch (Exception e) {
            System.err.println("NetworkManager (Plant " + this.myId + "): Error sending Elected to successor " +
                     ": " + e.getMessage());
        }
    }

}
