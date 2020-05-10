package indexing;

import types.JavaType;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class LiveIndex extends Index {
    public ConcurrentHashMap<Integer, Object> index;

    public int nrIndexed;

    public JavaType type;

    private ArrayList<Integer> linesSet;
    private int listIndex;

    /**
     * Initialize for given cardinality of indexed table.
     *
     * @param cardinality number of rows to index
     * @param javaType    type of Column
     */
    public LiveIndex(int cardinality, JavaType javaType) {
        super(cardinality);
        nrIndexed = 0;
        type = javaType;

        index = new ConcurrentHashMap<>();

        linesSet = new ArrayList<>();
        for (int i = 0; i < cardinality; i++) {
            linesSet.add(i);
        }
        resetRandomList();
    }

    public int getRandomNotHashed() {
        if (listIndex == cardinality) {
            resetRandomList();
            return cardinality;
        }
        int n = linesSet.get(listIndex);
        listIndex++;
        return n;
    }

    public void addHash(int n, Object data) {
        if (nrIndexed == cardinality) return;
        if (data != null) {
            index.put(n, data);
            nrIndexed++;
        }
    }

    public void resetRandomList() {
        listIndex = 0;
        //Collections.shuffle(linesSet);
    }
}
