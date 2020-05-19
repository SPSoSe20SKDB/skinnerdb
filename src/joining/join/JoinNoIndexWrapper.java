package joining.join;

import buffer.BufferManager;
import data.DoubleData;
import data.IntData;
import indexing.LiveIndex;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import types.JavaType;

import java.util.Set;

public class JoinNoIndexWrapper<T> extends JoinIndexWrapper {
    public static int nextIndexCalled = 0;
    private final JavaType type;
    private LiveIndex<T> liveIndex;

    /**
     * Initialize index wrapper for
     * given query and join order.
     *
     * @param queryInfo  query meta-data
     * @param preSummary maps query columns to intermediate result columns
     * @param joinCols   pair of columns in equi-join predicate
     * @param order      join order
     * @param javaType
     * @throws Exception
     */
    public JoinNoIndexWrapper(QueryInfo queryInfo, Context preSummary, Set<ColumnRef> joinCols, int[] order, JavaType javaType) throws Exception {
        super(queryInfo, preSummary, joinCols, order);
        type = javaType;

        if (nextIndex == null) {
            nextIndex = new LiveIndex<T>(nextData.cardinality, javaType);
            nextIndex.data = nextData;
            BufferManager.colToIndex.put(nextRef, nextIndex);
        }

        liveIndex = (LiveIndex) nextIndex;
    }

    @Override
    public int nextIndex(int[] tupleIndices) {
        nextIndexCalled++;

        // Hashtabelle bereits fertig vorhanden
        if (liveIndex.isReady()) {
            Object priorVal = null;
            int priorTuple = tupleIndices[priorTable];

            // Aktuelles Datum aus erster Tabelle laden
            switch (priorData.getClass().getSimpleName()) {
                case "IntData":
                    priorVal = ((IntData) priorData).data[priorTuple];
                    break;
                case "DoubleData":
                    priorVal = ((DoubleData) priorData).data[priorTuple];
                    break;
            }
            // Datum in zweiter Tabelle finden
            int nextTuple = liveIndex.getNextHashLine((T) priorVal);

            // Abfrage verhindert Endlosschleife
            return tupleIndices[nextTable] < nextTuple ? nextTuple : liveIndex.cardinality;
        }
        // Hashtabelle ist nicht fertig aufgebaut
        else {
            int n = liveIndex.getNextNotHashed();
            if (n >= liveIndex.cardinality) return n;
            Object data = null;
            switch (nextData.getClass().getSimpleName()) {
                case "IntData":
                    data = ((IntData) nextData).data[n];
                    break;
                case "DoubleData":
                    data = ((DoubleData) nextData).data[n];
                    break;
            }
            liveIndex.addHash(n, (T) data);

            return n;
        }

    }

    @Override
    public int nrIndexed(int[] tupleIndices) {
        return liveIndex.nrIndexed;
    }
}
