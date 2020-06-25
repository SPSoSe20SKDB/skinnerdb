package indexing;

import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;

import java.util.*;

public class LiveIntIndex extends LiveIndex {
    /**
     * Structure for index hash table
     */
    public Map<Integer, List<Integer>> index;
    public int[][] solidList;
    public IntIntMap keylist;
    public boolean isKeyColumn = false;
    public IntIntMap keyIndex;

    /**
     * Initialize for given cardinality of indexed table.
     *
     * @param cardinality number of rows to index
     */
    public LiveIntIndex(int cardinality, boolean isKey) {
        super(cardinality);

        //isKeyColumn = isKey;

        if (isKeyColumn) {
            //keyIndex = HashIntIntMaps.newMutableMap(cardinality);
        } else {
            index = new WeakHashMap<>();
        }
    }

    @Override
    void solidify() {
        int hashCount = index.size();
        solidList = new int[hashCount][];
        keylist = HashIntIntMaps.newMutableMap(hashCount);
        int ctr = 0;
        Set<Map.Entry<Integer, List<Integer>>> entrySet = index.entrySet();

        for (Map.Entry<Integer, List<Integer>> ent : entrySet) {
            List<Integer> value = ent.getValue();
            int entSize = value.size();
            int[] entArray = new int[entSize];
            int ctr2 = 0;
            for (int val : value) {
                entArray[ctr2] = val;
                ctr2++;
            }
            solidList[ctr] = entArray;
            keylist.put(ent.getKey().intValue(), ctr);
            ctr++;
        }
        index = null;
    }

    /**
     * Datum mit Zeilennummer dem Hash hinzufügen
     *
     * @param n    Zeilennummer
     * @param data Datum
     */
    public void addHash(int n, Integer data) {
        if (isKeyColumn) {
            //keyIndex.put(data.intValue(),n);
            //return;
        }
        //if (nrIndexed == cardinality) return;
        if (data != null) {
            //if(addThread != null) try {
            //    addThread.join(10);
            //} catch(InterruptedException err) {

            //}
            //addThread = new Thread(() -> {
            if (!index.containsKey(data)) {
                List<Integer> newList = new ArrayList<>(11);
                newList.add(n);
                index.put(data, newList);
            } else {
                index.get(data).add(n);
            }
            //});
            //addThread.start();
            //addThread.run();
            nrIndexed++;
        }
    }

    /**
     * Hash-Tabellen-Abfrage zu Datum
     *
     * @param data Datenobjekt in erster Tabelle, zu dem die nächste verfügbaren Zeile in zweiter Tabelle zurückgegeben wird.
     * @return Zeilenindice zu dem gegebenen Datum ()
     */
    public int getNextHashLine(Integer data, int prevTuple) {
        /*
        if(isReady()) {
            if(solidifyThread != null) try {
                solidifyThread.join();
            } catch (InterruptedException err) {

            }

            int key = keylist.getOrDefault(data.intValue(), -1);
            if(key < 0) return cardinality;

            int[] positions = solidList[key];

            int lowerBound = 0;
            int upperBound = positions.length - 1;

            while (upperBound-lowerBound>1) {
                int middle = lowerBound + (upperBound-lowerBound)/2;
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
        */

        if (isKeyColumn) {
            //return data.intValue();
            //return keyIndex.getOrDefault(data.intValue(), cardinality);
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
