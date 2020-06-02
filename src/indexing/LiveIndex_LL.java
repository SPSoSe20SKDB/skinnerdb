package indexing;

import types.JavaType;

import java.util.LinkedList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

public class LiveIndex_LL<T> extends Index {
    public int nrIndexed = 0;
    private final ConcurrentHashMap<T, LinkedList<Integer>> index;
    private int indexPosition;
    //private ConcurrentHashMap<T, Integer> indexPositions;
    private T lastRequest;
    private boolean isReady = false;

    private int listIndex = 0;

    /**
     * Initialize for given cardinality of indexed table.
     *
     * @param cardinality number of rows to index
     * @param javaType    type of Column
     */
    public LiveIndex_LL(int cardinality, JavaType javaType) {
        super(cardinality);

        index = new ConcurrentHashMap<>();
        indexPosition = 0;
        //indexPositions = new ConcurrentHashMap<>();
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
                index.put(data, new LinkedList<>(Collections.singletonList(n)));
            } else {
                index.get(data).addLast(n);
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
    public int getNextHashLine(T data) {
        if (!data.equals(lastRequest)) indexPosition = 0;
        LinkedList<Integer> dataPositions = index.getOrDefault(data, null);
        lastRequest = data;
        if (dataPositions == null) {
            indexPosition = 0;
            return cardinality;
        }
        if (indexPosition >= dataPositions.size()) {
            indexPosition = 0;
            return cardinality;
        }
        int returnLine = dataPositions.get(indexPosition);
        indexPosition++;
        return returnLine;
    }

    /**
     * Ist Hash-Tabelle fertig gebaut
     *
     * @return Hash-Tabelle fertig
     */
    public boolean isReady() {
        return isReady;
    }

    public void resetCurrent() {
        indexPosition = 0;
    }
}
