package Arbiter;

import io.grpc.stub.StreamObserver;

import Arbiter.ArbiterProto.Ack;
import Arbiter.ArbiterProto.WorkRequest;
import Arbiter.ArbiterProto.Empty;
import Arbiter.ArbiterProto.LockResponse;
import Arbiter.ArbiterProto.QueueSize;
import Arbiter.ArbiterProto.QueueStatus;

import java.util.LinkedList;

/**
 * Implementazione del servizio gRPC Arbiter.
 * Questa classe è un Singleton di fatto all'interno del server
 * e gestisce lo stato della coda in modo thread-safe.
 */
public class ArbiterServiceImpl extends ArbiterServiceGrpc.ArbiterServiceImplBase {

    // --- Coda interna e stato  ---
    private final LinkedList<WorkRequest> requestQueue = new LinkedList<>();
    private final Object lock = new Object(); // Lock per tutte le operazioni

    private enum QueueState { OPEN_FOR_ELECTIONS, AWAITING_RESULT }
    private QueueState currentState = QueueState.OPEN_FOR_ELECTIONS;
    private String requestBeingFinalized = null;

    // --- Metodi gRPC ---

    @Override
    public void addRequest(WorkRequest request, StreamObserver<Ack> responseObserver) {
        synchronized (lock) {
            // Evita di aggiungere duplicati
            boolean alreadyExists = requestQueue.stream()
                    .anyMatch(r -> r.getRequestId().equals(request.getRequestId()));

            if (!alreadyExists) {
                requestQueue.add(request);
                System.out.println("[Arbiter] Added: " + request.getRequestId() + ". Queue size: " + requestQueue.size());
            } else {
                System.out.println("[Arbiter] Ignored duplicate add for: " + request.getRequestId());
            }
            lock.notifyAll();
        }
        Ack ack = Ack.newBuilder().setSuccess(true).setMessage("Request added/ignored.").build();
        responseObserver.onNext(ack);
        responseObserver.onCompleted();
    }

    @Override
    public void peekNextRequest(Empty request, StreamObserver<WorkRequest> responseObserver) {
        WorkRequest nextRequest;
        synchronized (lock) {
            nextRequest = requestQueue.peekFirst();
        }
        // Se la coda è vuota, restituisce una richiesta vuota di default
        responseObserver.onNext(nextRequest != null ? nextRequest : WorkRequest.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void lockForFinalization(WorkRequest request, StreamObserver<LockResponse> responseObserver) {
        boolean acquired = false;
        synchronized (lock) {
            if (currentState == QueueState.OPEN_FOR_ELECTIONS) {
                currentState = QueueState.AWAITING_RESULT;
                requestBeingFinalized = request.getRequestId();
                acquired = true;
                System.out.println("[Arbiter] LOCKED for: " + request.getRequestId());
            }
        }
        LockResponse response = LockResponse.newBuilder().setAcquired(acquired).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void unlockAndRemove(WorkRequest request, StreamObserver<Ack> responseObserver) {
        String message = "Unlock failed: state was inconsistent.";
        boolean success = false;
        synchronized (lock) {
            if (currentState == QueueState.AWAITING_RESULT && request.getRequestId().equals(requestBeingFinalized)) {

                requestQueue.removeIf(r -> r.getRequestId().equals(request.getRequestId()));

                currentState = QueueState.OPEN_FOR_ELECTIONS;
                requestBeingFinalized = null;
                success = true;
                message = "Queue unlocked and request removed.";

                System.out.println("[Arbiter] UNLOCKED. Ready for new elections. Queue size: " + requestQueue.size());
                lock.notifyAll();
            }
        }
        Ack ack = Ack.newBuilder().setSuccess(success).setMessage(message).build();
        responseObserver.onNext(ack);
        responseObserver.onCompleted();
    }

    @Override
    public void isQueueOpen(Empty request, StreamObserver<QueueStatus> responseObserver) {
        boolean isOpen;
        synchronized (lock) {
            isOpen = (currentState == QueueState.OPEN_FOR_ELECTIONS);
        }
        QueueStatus status = QueueStatus.newBuilder().setIsOpen(isOpen).build();
        responseObserver.onNext(status);
        responseObserver.onCompleted();
    }

    @Override
    public void getQueueSize(Empty request, StreamObserver<QueueSize> responseObserver) {
        int size;
        synchronized (lock) {
            size = requestQueue.size();
        }
        QueueSize queueSize = QueueSize.newBuilder().setSize(size).build();
        responseObserver.onNext(queueSize);
        responseObserver.onCompleted();
    }

    @Override
    public void checkRequestInQueue(WorkRequest request, StreamObserver<QueueStatus> responseObserver) {
        boolean present;
        synchronized (lock) {
            present = requestQueue.stream()
                    .anyMatch(r -> r.getRequestId().equals(request.getRequestId()));
        }

        QueueStatus status = QueueStatus.newBuilder().setIsOpen(present).build();
        responseObserver.onNext(status);
        responseObserver.onCompleted();
    }
}