package joining.join;

import catalog.CatalogManager;
import config.LoggingConfig;
import expressions.ExpressionInfo;
import expressions.compilation.EvaluatorType;
import expressions.compilation.ExpressionCompiler;
import expressions.compilation.KnaryBoolEval;
import joining.progress.ProgressTracker;
import joining.result.JoinResult;
import net.sf.jsqlparser.expression.Expression;
import preprocessing.Context;
import query.QueryInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A multi-way join operator that executes joins in small
 * episodes, using for each episode a newly specified join
 * order. Collects result tuples over different episodes
 * and contains finally a complete join result.
 * 
 * @author immanueltrummer
 *
 */
public abstract class MultiWayJoin {
    /**
     * The query for which join orders are evaluated.
     */
    protected final QueryInfo query;
    /**
     * Number of tables joined by query.
     */
    protected final int nrJoined;
    /**
     * At i-th position: cardinality of i-th joined table
     * (after pre-processing).
     */
    public final int[] cardinalities;
    /**
     * Summarizes pre-processing steps.
     */
    protected final Context preSummary;
    /**
     * Maps non-equi join predicates to compiled evaluators.
     */
    protected final Map<Expression, KnaryBoolEval> predToEval;
    /**
     * Collects result tuples and contains
     * finally a complete result.
     */
    public final JoinResult result;
    /**
     * Avoids redundant evaluation work by tracking evaluation progress.
     */
    public ProgressTracker tracker;

    /**
     * This constructor only serves for testing purposes.
     * It initializes most field to null pointers.
     *
     * @param query query to test
     * @throws Exception
     */
    public MultiWayJoin(QueryInfo query) throws Exception {
        this.query = query;
        this.nrJoined = query.nrJoined;
        this.preSummary = null;
        this.cardinalities = null;
        this.result = null;
        predToEval = null;
    }
    /**
     * Initializes join operator for given query
     * and initialize new join result.
     * 
     * @param query			query to process
     * @param preSummary	summarizes pre-processing steps
     */
    public MultiWayJoin(QueryInfo query, Context preSummary) throws Exception {
        this.query = query;
        this.nrJoined = query.nrJoined;
        this.preSummary = preSummary;
        // Retrieve table cardinalities
        this.cardinalities = new int[nrJoined];
        for (Entry<String,Integer> entry : 
        	query.aliasToIndex.entrySet()) {
        	String alias = entry.getKey();
        	String table = preSummary.aliasToFiltered.get(alias);
        	int index = entry.getValue();
        	int cardinality = CatalogManager.getCardinality(table);
        	cardinalities[index] = cardinality;
        }
        this.result = new JoinResult(nrJoined);
        // Compile predicates
        predToEval = new HashMap<>();
        for (ExpressionInfo predInfo : query.wherePredicates) {
    		// Log predicate compilation if enabled
    		if (LoggingConfig.MAX_JOIN_LOGS>0) {
    			System.out.println("Compiling predicate " + predInfo + " ...");
    		}
    		// Compile predicate and store in lookup table
        	Expression pred = predInfo.finalExpression;
        	ExpressionCompiler compiler = new ExpressionCompiler(predInfo, 
        			preSummary.columnMapping, query.aliasToIndex, null,
        			EvaluatorType.KARY_BOOLEAN);
        	predInfo.finalExpression.accept(compiler);
        	KnaryBoolEval boolEval = (KnaryBoolEval)compiler.getBoolEval();
        	predToEval.put(pred, boolEval);        		
        }
    }
    /**
     * Executes given join order for a given number of steps.
     * 
     * @param order		execute this join order
     * @return			reward (higher reward means faster progress)
     * @throws Exception 
     */
    public abstract double execute(int[] order) throws Exception;
    /**
     * Returns true iff a complete join result was generated.
     * 
     * @return	true iff query processing is finished
     */
    public abstract boolean isFinished();
}