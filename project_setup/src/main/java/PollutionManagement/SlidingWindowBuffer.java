package PollutionManagement;

import Simulators.Buffer;
import Simulators.Measurement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class SlidingWindowBuffer implements Buffer {
    private final LinkedList<Measurement> measurementsWindow;
    private final List<Double> computedAverages;
    private final int windowSize = 8;
    private final int elementsToDiscard = 4; // 50% di overlap, scarta i 4 più vecchi

    private final Object lock = new Object();

    public SlidingWindowBuffer() {
        this.measurementsWindow = new LinkedList<>();
        this.computedAverages = new ArrayList<>();
    }

    @Override
    public void addMeasurement(Measurement m) {
        synchronized (lock) {
            measurementsWindow.add(m);


            if (measurementsWindow.size() >= windowSize) {
                computeAndStoreAverage();
                slideWindow();
            }
        }
    }

    private void computeAndStoreAverage() {
        // Chiamato da addMeasurement, quindi già sotto lock
        if (measurementsWindow.size() < windowSize) {
            return;
        }

        double sum = 0;
        // Considera le prime 'windowSize' misurazioni (le più vecchie nella finestra attuale)
        for (int i = 0; i < windowSize; i++) {
            sum += measurementsWindow.get(i).getValue();
        }

        double average = sum / windowSize;
        computedAverages.add(average);
    }

    private void slideWindow() {
        // Chiamato da addMeasurement, quindi già sotto lock
        // Scarta i più vecchi 'elementsToDiscard'
        if (measurementsWindow.size() >= windowSize) {
            for (int i = 0; i < elementsToDiscard; i++) {
                if (!measurementsWindow.isEmpty()) {
                    measurementsWindow.removeFirst();
                }
            }
        }
    }

    /**
     * Metodo specifico per ottenere le medie CO2 calcolate e pulire la lista delle medie.
     * Questo è il metodo che ThermalPowerPlants userà per inviare i dati.
     */
    public List<Double> getComputedAveragesAndClear() {
        synchronized (lock) {
            if (computedAverages.isEmpty()) {
                return new ArrayList<>();
            }
            List<Double> averagesToSend = new ArrayList<>(computedAverages);
            computedAverages.clear();
            return averagesToSend;
        }
    }

    /**
     * Implementazione del metodo dell'interfaccia Buffer.
     * Restituisce le misurazioni grezze ATTUALMENTE nella finestra e pulisce la finestra.
     */
    @Override
    public List<Measurement> readAllAndClean() {
        synchronized (lock) {
            List<Measurement> currentMeasurements = new ArrayList<>(measurementsWindow);
            measurementsWindow.clear();
            // Pulisce tutto
            return currentMeasurements;
        }
    }
}