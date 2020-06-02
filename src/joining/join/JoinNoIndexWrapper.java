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

        // get line of left table
        int priorTuple = tupleIndices[priorTable];

        // get data from left table
        switch (priorDataType) {
            case "IntData":
                priorVal = ((IntData) priorData).data[priorTuple];
                break;
            case "DoubleData":
                priorVal = ((DoubleData) priorData).data[priorTuple];
                break;
        }
        priorValT = (T) priorVal;

        // get last probed row from right table
        int nextCurTuple = tupleIndices[nextTable];

        // try to find appropriate line index from right table
        int nextTuple = liveIndex.getNextHashLine(priorValT, nextCurTuple);

        // if new line index from right table is not applicable
        if (nextTuple >= liveIndex.cardinality || nextCurTuple > nextTuple) {
            // if live join is fully build, return
            if (liveIndex.isReady()) {
                return liveIndex.cardinality;
                // else build hash table further
            } else {
                Object nextVal = null;
                T nextValT;
                // loop through remaining table to find next appropriate line
                do {
                    // get next not hashed line
                    nextTuple = liveIndex.getNextNotHashed();

                    // when next line is higher than table, hash table is ready, delegate to this function
                    if (nextTuple >= liveIndex.cardinality) {
                        return nextIndex(tupleIndices);
                    }

                    // get data from right table
                    switch (nextDataType) {
                        case "IntData":
                            nextVal = ((IntData) nextData).data[nextTuple];
                            break;
                        case "DoubleData":
                            nextVal = ((DoubleData) nextData).data[nextTuple];
                            break;
                    }
                    nextValT = (T) nextVal;

                    // add line to hash
                    liveIndex.addHash(nextTuple, nextValT);
                    // do while not found appropriate line
                } while (!priorValT.equals(nextValT));

                // if end, test if data is correct, else return cardinality
                if (priorValT.equals(nextValT)) {
                    return nextTuple;
                } else {
                    return liveIndex.cardinality;
                }
            }
            // else if new line index from right table is applicable
        } else {
            return nextTuple;
        }
    }

    @Override
    public int nrIndexed(int[] tupleIndices) {
        return liveIndex.nrIndexed;
    }
}
