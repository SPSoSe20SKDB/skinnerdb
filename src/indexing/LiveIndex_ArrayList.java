package indexing;

import types.JavaType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

public class LiveIndex_ArrayList<T> extends Index {
    /**
     * Structure for index hash table
     */
    private final ConcurrentHashMap<T, ArrayList<Integer>> index;
    /**
     * indexed lines
     */
    public int nrIndexed = 0;
    /**
     * state if index is fully build
     */
    private boolean isReady = false;

    /**
     * current row when indexing
     */
    private int listIndex = 0;

    /**
     * Initialize for given cardinality of indexed table.
     *
     * @param cardinality number of rows to index
     * @param javaType    type of Column
     */
    public LiveIndex_ArrayList(int cardinality, JavaType javaType) {
        super(cardinality);

        index = new ConcurrentHashMap<>();
    }

    /**
     * Nächsten Tabelleninces (Iterator-mäßig) zurückgeben
     *
     * @return Zeilennummer innerhalb der Spalte
     */
    public int getNextNotHashed() {
        if (listIndex == cardinality) {
            listIndex = 0;
            isReady = true;
            return cardinality;
        }
        int returnIndex = listIndex;
        listIndex++;
        return returnIndex;
    }

    /**
     * Datum mit Zeilennummer dem Hash hinzufügen
     *
     * @param n    Zeilennummer
     * @param data Datum
     */
    public void addHash(int n, T data) {
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
    public int getNextHashLine(T data, int prevTuple) {
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

    /**
     * Ist Hash-Tabelle fertig gebaut
     *
     * @return Hash-Tabelle fertig
     */
    public boolean isReady() {
        return isReady;
    }
}
