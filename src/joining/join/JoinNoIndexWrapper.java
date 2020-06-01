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
    private final JavaType type;
    private final LiveIndex<T> liveIndex;
    private final String priorDataType;
    private final String nextDataType;

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

        priorDataType = priorData.getClass().getSimpleName();
        nextDataType = nextData.getClass().getSimpleName();
    }

    @Override
    public int nextIndex(int[] tupleIndices) {
        Object priorVal = null;
        T priorValT;
        int priorTuple = tupleIndices[priorTable];
        // Aktuelles Datum aus erster Tabelle laden
        switch (priorDataType) {
            case "IntData":
                priorVal = ((IntData) priorData).data[priorTuple];
                break;
            case "DoubleData":
                priorVal = ((DoubleData) priorData).data[priorTuple];
                break;
        }
        priorValT = (T) priorVal;

        int nextTuple = liveIndex.getNextHashLine(priorValT);

        if (nextTuple >= liveIndex.cardinality || tupleIndices[nextTable] > nextTuple) {
            if (liveIndex.isReady()) {
                return liveIndex.cardinality;
            } else {
                Object nextVal = null;
                T nextValT;
                do {
                    nextTuple = liveIndex.getNextNotHashed();
                    if (nextTuple >= liveIndex.cardinality) {
                        return nextIndex(tupleIndices);
                    }
                    switch (nextDataType) {
                        case "IntData":
                            nextVal = ((IntData) nextData).data[nextTuple];
                            break;
                        case "DoubleData":
                            nextVal = ((DoubleData) nextData).data[nextTuple];
                            break;
                    }
                    nextValT = (T) nextVal;
                    liveIndex.addHash(nextTuple, nextValT);
                } while (!priorValT.equals(nextValT));
                if (priorValT.equals(nextValT)) {
                    return nextTuple;
                } else {
                    return liveIndex.cardinality;
                }
            }
        } else {
            return nextTuple;
        }
    }

    @Override
    public int nrIndexed(int[] tupleIndices) {
        return liveIndex.nrIndexed;
    }

    public void resetCurrent() {
        liveIndex.resetCurrent();
    }
}
