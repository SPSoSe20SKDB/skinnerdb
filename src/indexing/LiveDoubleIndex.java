package indexing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

public class LiveDoubleIndex extends LiveIndex {
    /**
     * Structure for index hash table
     */
    public final ConcurrentHashMap<Double, ArrayList<Integer>> index;

    /**
     * Initialize for given cardinality of indexed table.
     *
     * @param cardinality number of rows to index
     */
    public LiveDoubleIndex(int cardinality) {
        super(cardinality);

        index = new ConcurrentHashMap<>();
    }

    /**
     * Datum mit Zeilennummer dem Hash hinzufügen
     *
     * @param n    Zeilennummer
     * @param data Datum
     */
    public void addHash(int n, Double data) {
        if (nrIndexed == cardinality) return;
        if (data != null) {
            if (!index.containsKey(data)) {
                index.put(data, new ArrayList<>(Collections.singletonList(n)));
            } else {
                index.get(data).add(n);
            }
            nrIndexed++;
        }
    }

    /**
     * Hash-Tabellen-Abfrage zu Datum
     *
     * @param data Datenobjekt in erster Tabelle, zu dem die nächste verfügbaren Zeile in zweiter Tabelle zurückgegeben wird.
     * @return Zeilenindice zu dem gegebenen Datum ()
     */
    public int getNextHashLine(Double data, int prevTuple) {
        // get position of date in table
        ArrayList<Integer> dataPositions = index.getOrDefault(data, null);

        // if positions equals null, data is not present in table
        if (dataPositions == null) {
            return cardinality;
        }

        // loop through index, find next index
        for (int i = 0; i < dataPositions.size(); i++) {
            int nextTuple = dataPositions.get(i);
            if (nextTuple > prevTuple) return nextTuple;
        }

        // if not found return cardinality
        return cardinality;
    }
}
