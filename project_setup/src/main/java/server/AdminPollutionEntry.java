package server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AdminPollutionEntry {
    final long submissionTimestamp; // Timestamp dell'invio del batch di medie
    final List<Double> co2Averages; // Lista delle medie CO2 in quel batch

    public AdminPollutionEntry(long submissionTimestamp, List<Double> co2Averages) {
        this.submissionTimestamp = submissionTimestamp;
        // Crea una copia immutabile o una copia difensiva
        this.co2Averages = Collections.unmodifiableList(new ArrayList<>(co2Averages));
    }

    public long getSubmissionTimestamp() { return submissionTimestamp; }
    public List<Double> getCo2Averages() { return co2Averages; }
}
