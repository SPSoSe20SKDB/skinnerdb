package joining.join;

import java.util.Iterator;
import java.util.Set;

import buffer.BufferManager;
import config.LoggingConfig;
import data.IntData;
import indexing.IntIndex;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;

/**
 * Uses index on join column to identify next
 * tuple to satisfy binary equality condition
 * on two integer columns.
 * 
 * @author immanueltrummer
 *
 */
public class JoinIndexWrapper {
	/**
	 * Prior table in join order from
	 * which we obtain value for lookup.
	 */
	final int priorTable;
	/**
	 * Next table in join order for
	 * which we propose a tuple index.
	 */
	final int nextTable;
	/**
	 * Reference to prior column data.
	 */
	final IntData priorData;
	/**
	 * Index on join column to use.
	 */
	final IntIndex nextIndex;
	/**
	 * Initialize index wrapper for
	 * given query and join order.
	 * 
	 * @param queryInfo		query meta-data
	 * @param preSummary	maps query columns to intermediate result columns
	 * @param joinCols		pair of columns in equi-join predicate
	 * @param order			join order
	 * @throws Exception
	 */
	public JoinIndexWrapper(QueryInfo queryInfo, 
			Context preSummary, Set<ColumnRef> joinCols, 
			int[] order) throws Exception {
		// Get table indices of join columns
		Iterator<ColumnRef> colIter = joinCols.iterator();
		ColumnRef col1 = colIter.next();
		ColumnRef col2 = colIter.next();
		int table1 = tableIndex(queryInfo, col1);
		int table2 = tableIndex(queryInfo, col2);
		// Determine position of tables in join order
		int pos1 = tablePos(order, table1);
		int pos2 = tablePos(order, table2);
		// Assign prior and next table accordingly
		priorTable = pos1<pos2?table1:table2;
		nextTable = pos1<pos2?table2:table1;
		// Get column data reference for prior table
		ColumnRef priorQueryCol = pos1<pos2?col1:col2;
		ColumnRef priorDbCol = preSummary.columnMapping.get(priorQueryCol);
		priorData = (IntData)BufferManager.getData(priorDbCol);
		// Get index for next table
		ColumnRef nextQueryCol = pos1<pos2?col2:col1;
		ColumnRef nextDbCol = preSummary.columnMapping.get(nextQueryCol);
		nextIndex = (IntIndex)BufferManager.colToIndex.get(nextDbCol);
		// Generate logging output
		if (LoggingConfig.INDEX_WRAPPER_VERBOSE) {
			System.out.println("Initialized join index wrapper: ");
			System.out.println("col1: " + col1);
			System.out.println("col2: " + col2);
			System.out.println("priorQueryCol: " + priorQueryCol);
			System.out.println("priorDBcol: " + priorDbCol);
			System.out.println("nextQueryCol: " + nextQueryCol);
			System.out.println("nextDBcol: " + nextDbCol);
		}
	}
	/**
	 * Extracts index of table in query column reference.
	 * 
	 * @param query		query to process
	 * @param queryCol	column that appears in query
	 * @return			index of query table
	 */
	int tableIndex(QueryInfo query, ColumnRef queryCol) {
		return query.aliasToIndex.get(queryCol.aliasName);
	}
	/**
	 * Returns position of given table in join order
	 * or -1 if the table is not found.
	 * 
	 * @param order		join order
	 * @param table		index of table
	 * @return			position of table in join order
	 */
	int tablePos(int[] order, int table) {
		int nrTables = order.length;
		for (int pos=0; pos<nrTables; ++pos) {
			if (order[pos] == table) {
				return pos;
			}
		}
		return -1;
	}
	/**
	 * Propose next index in next table that
	 * satisfies equi-join condition with
	 * current tuple in prior table, returns
	 * cardinality if no such tuple is found.
	 * 
	 * @param tupleIndices	current tuple indices
	 * @return	next interesting tuple index or cardinality
	 */
	public int nextIndex(int[] tupleIndices) {
		int priorTuple = tupleIndices[priorTable];
		int priorVal = priorData.data[priorTuple];
		int curTuple = tupleIndices[nextTable];
		return nextIndex.nextTuple(priorVal, curTuple);
	}
	/**
	 * Propose next index in next table that
	 * satisfies equi-join condition with
	 * current tuple in prior table and locates in thread scope, returns
	 * cardinality if no such tuple is found.
	 *
	 * @param tupleIndices	current tuple indices
	 * @param tid			thread id
	 * @param nrThreads		number of threads
	 * @return	next interesting tuple index or cardinality
	 */
	public int nextIndexInScope(int[] tupleIndices, int tid, int nrThreads) {
		int priorTuple = tupleIndices[priorTable];
		int priorVal = priorData.data[priorTuple];
		int curTuple = tupleIndices[nextTable];
		return nextIndex.nextTuple(priorVal, curTuple);
	}

	/**
	 * Returns number of tuples indexed in next
	 * table for given value in prior table.
	 * 
	 * @param tupleIndices	current tuple indices
	 * @return	number of indexed tuples in next table
	 */
	public int nrIndexed(int[] tupleIndices) {
		int priorTuple = tupleIndices[priorTable];
		int priorVal = priorData.data[priorTuple];
		return nextIndex.nrIndexed(priorVal);
	}
	/**
	 * Initializes iterator over tuples in next table
	 * which match tuple value in prior table.
	 * 
	 * @param tupleIndices	tuple indices for base tables
	 */
	public void initIter(int[] tupleIndices) {
		int priorTuple = tupleIndices[priorTable];
		int priorVal = priorData.data[priorTuple];
		nextIndex.initIter(priorVal);
	}
	/**
	 * Returns next element of iterator.
	 * 
	 * @return	next element in iterator
	 */
	public int iterNext() {
		return nextIndex.iterNext();
	}
	/**
	 * Returns next tuple from iterator whose
	 * index exceeds the current tuple index.
	 * Advances iterator accordingly.
	 * 
	 * @param tupleIndices	tuple indices for base tables
	 */
	public int iterNextHigher(int[] tupleIndices) {
		int curTuple = tupleIndices[nextTable];
		return nextIndex.iterNextHigher(curTuple);
	}
	@Override
	public String toString() {
		return "Prior table:\t" + priorTable + "; Next:\t" + nextTable;
	}
}
