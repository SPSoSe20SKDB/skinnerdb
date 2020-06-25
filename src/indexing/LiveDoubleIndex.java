package indexing;

import com.koloboke.collect.map.DoubleIntMap;
import com.koloboke.collect.map.hash.HashDoubleIntMaps;
import config.JoinConfig;

import java.util.*;

public class LiveDoubleIndex extends LiveIndex {
    /**
     * Structure for index hash table
     */
    public final Map<Double, List<Integer>> index;
    public int[][] solidList;
    public DoubleIntMap keylist;

    /**
     * Initialize for given cardinality of indexed table.
     *
     * @param cardinality number of rows to index
     */
    public LiveDoubleIndex(int cardinality) {
        super(cardinality);

        index = new HashMap<>();
    }

    @Override
    void solidify() {
        int hashCount = index.size();
        solidList = new int[hashCount][];
        keylist = HashDoubleIntMaps.newMutableMap(hashCount);
        int ctr = 0;
        Set<Map.Entry<Double, List<Integer>>> entrySet = index.entrySet();
        for (Map.Entry<Double, List<Integer>> ent : entrySet) {
            List<Integer> value = ent.getValue();
            int entSize = value.size();
            int[] entArray = new int[entSize];
            int ctr2 = 0;
            for (int val : value) {
                entArray[ctr2] = val;
                ctr2++;
            }
            solidList[ctr] = entArray;
            keylist.put(ent.getKey().doubleValue(), ctr);
            ctr++;
        }
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
                List<Integer> newList = new ArrayList<>(11);
                newList.add(n);
                index.put(data, newList);
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
        if (JoinConfig.USE_RIPPLE_SOLIDIFY && isReady()) {
            if (solidifyThread != null) try {
                solidifyThread.join();
            } catch (InterruptedException err) {
            }

            int key = keylist.getOrDefault(data.doubleValue(), -1);
            if (key < 0) return cardinality;

            int[] positions = solidList[key];

            int lowerBound = 0;
            int upperBound = positions.length - 1;

            while (upperBound - lowerBound > 1) {
                int middle = lowerBound + (upperBound - lowerBound) / 2;
                if (positions[middle] > prevTuple) {
                    upperBound = middle;
                } else {
                    lowerBound = middle;
                }
            }

            // loop through index, find next index
            for (int i = lowerBound; i <= upperBound; i++) {
                int nextTuple = positions[i];
                if (nextTuple > prevTuple) return nextTuple;
            }

            return cardinality;
        }

        // get position of date in table
        List<Integer> dataPositions = index.getOrDefault(data, null);

        // if positions equals null, data is not present in table
        if (dataPositions == null) {
            return cardinality;
        }

        int lowerBound = 0;
        int upperBound = dataPositions.size() - 1;

        while (upperBound - lowerBound > 1) {
            int middle = lowerBound + (upperBound - lowerBound) / 2;
            if (dataPositions.get(middle) > prevTuple) {
                upperBound = middle;
            } else {
                lowerBound = middle;
            }
        }

        // loop through index, find next index
        for (int i = lowerBound; i <= upperBound; i++) {
            int nextTuple = dataPositions.get(i);
            if (nextTuple > prevTuple) return nextTuple;
        }

        // if not found return cardinality
        return cardinality;
    }
}
