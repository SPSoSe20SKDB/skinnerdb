package joining.join;

import buffer.BufferManager;
import data.DoubleData;
import indexing.LiveDoubleIndex;
import indexing.LiveIntIndex;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;

import java.util.Set;

public class JoinNoIndexDoubleWrapper extends JoinIndexWrapper {
    private final LiveDoubleIndex liveIndex;
    private final DoubleData nextDoubleData;
    private final DoubleData priorDoubleData;

    /**
     * Initialize index wrapper for
     * given query and join order.
     *
     * @param queryInfo  query meta-data
     * @param preSummary maps query columns to intermediate result columns
     * @param joinCols   pair of columns in equi-join predicate
     * @param order      join order
     * @throws Exception
     */
    public JoinNoIndexDoubleWrapper(QueryInfo queryInfo, Context preSummary, Set<ColumnRef> joinCols, int[] order) throws Exception {
        super(queryInfo, preSummary, joinCols, order);

        if (nextIndex == null) {
            nextIndex = new LiveIntIndex(nextData.cardinality);
            nextIndex.data = nextData;
            BufferManager.colToIndex.put(nextRef, nextIndex);
        }

        liveIndex = (LiveDoubleIndex) nextIndex;

        nextDoubleData = ((DoubleData) nextData);
        priorDoubleData = ((DoubleData) priorData);
    }

    @Override
    public int nextIndex(int[] tupleIndices) {
        // get line of left table
        int priorTuple = tupleIndices[priorTable];

        // get data from left table
        Double priorVal = priorDoubleData.data[priorTuple];

        // get last probed row from right table
        int nextCurTuple = tupleIndices[nextTable];

        // try to find appropriate line index from right table
        int nextTuple = liveIndex.getNextHashLine(priorVal, nextCurTuple);

        // if new line index from right table is not applicable
        if (nextTuple >= liveIndex.cardinality || nextCurTuple > nextTuple) {
            // if live join is fully build, return
            if (liveIndex.isReady()) {
                return liveIndex.cardinality;
                // else build hash table further
            } else {
                Double nextVal;
                // loop through remaining table to find next appropriate line
                do {
                    // get next not hashed line
                    nextTuple = liveIndex.getNextNotHashed();

                    // when next line is higher than table, hash table is ready, delegate to this function
                    if (nextTuple >= liveIndex.cardinality) {
                        return liveIndex.getNextHashLine(priorVal, nextCurTuple);
                    }

                    // get data from right table
                    nextVal = nextDoubleData.data[nextTuple];

                    // add line to hash
                    liveIndex.addHash(nextTuple, nextVal);
                    // do while not found appropriate line
                } while (!priorVal.equals(nextVal));
                return nextTuple;
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

    @Override
    public int getUnique() {
        return liveIndex.index.size();
    }
}
