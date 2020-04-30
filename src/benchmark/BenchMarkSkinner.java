package benchmark;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.TableInfo;
import config.GeneralConfig;
import config.NamingConfig;
import config.StartupConfig;
import diskio.PathUtil;
import expressions.ExpressionInfo;
import indexing.Indexer;
import joining.JoinProcessor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import postprocessing.PostProcessor;
import preprocessing.Context;
import preprocessing.Preprocessor;
import print.RelationPrinter;
import query.ColumnRef;
import query.QueryInfo;
import statistics.JoinStats;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Benchmarks pre-, join, and post-processing stage
 */
public class BenchMarkSkinner {

    public static void main(String[] args) throws Exception{
        Runtime rt = Runtime.getRuntime();

        // Check for command line parameters
        if (args.length != 1 && args.length != 2) {
            System.out.println("Specify Skinner DB dir, "
                                + "query directory");
            return;
        }

        //Get required information
        String SkinnerDbDir = args[0];
        String queryDir = args[1];

        //Initialize database
        PathUtil.initSchemaPaths(SkinnerDbDir);
        CatalogManager.loadDB(PathUtil.schemaPath);
        PathUtil.initDataPaths(CatalogManager.currentDB);
        System.out.println("Loading data ...");
        GeneralConfig.inMemory = true;

        long beforeLoadingMillis = System.currentTimeMillis();
        long memoryBeforeLoading = rt.totalMemory() - rt.freeMemory();
        BufferManager.loadDB();
        long loadMemory = rt.totalMemory() - rt.freeMemory() - memoryBeforeLoading;
        long loadingMillis = System.currentTimeMillis() - beforeLoadingMillis;
        System.out.println("Used Memory for Loading: " + ((double) loadMemory) / 1024 / 1024 + " MB");
        System.out.println("Millis for Loading: " + loadingMillis + " ms");
        System.out.println("Data loaded.");

        long beforeIndexingMillis = System.currentTimeMillis();
        long memoryBeforeIndexing = rt.totalMemory() - rt.freeMemory();
        Indexer.indexAll(StartupConfig.INDEX_CRITERIA);
        long hashMemory = rt.totalMemory() - rt.freeMemory() - memoryBeforeIndexing;
        long indexingMillis = System.currentTimeMillis() - beforeIndexingMillis;
        System.out.println("Used Memory for Hashing: " + ((double) hashMemory) / 1024 / 1024 + " MB");
        System.out.println("Millis for Hashing: " + indexingMillis + " ms");

        // Read all queries from files
        Map<String, PlainSelect> nameToQuery =
                BenchUtil.readAllQueries(queryDir);

        //Opening benchmark result files
        PrintWriter benchOut = new PrintWriter("bench.txt");
        PrintStream skinnerOut = new PrintStream("skinnerResults.txt");
        PrintStream console = System.out;

        //Measuring pre-processing time for each query
        BenchUtil.writeBenchHeader(benchOut);
        for (Entry<String, PlainSelect> entry : nameToQuery.entrySet()){
            System.out.println(entry.getKey());
            System.out.println(entry.getValue().toString());
            long startMillis = System.currentTimeMillis();
            QueryInfo query = new QueryInfo(entry.getValue(),
                    false, -1, -1, "D:\\Projects\\skinnerdb\\");
            Context preSummary = Preprocessor.process(query);
            long preMillis = System.currentTimeMillis() - startMillis;
            JoinProcessor.process(query, preSummary);
            long postStartMillis = System.currentTimeMillis();
            PostProcessor.process(query, preSummary,
                    NamingConfig.FINAL_RESULT_NAME, true);
            long postMillis = System.currentTimeMillis() - postStartMillis;
            long totalMillis = System.currentTimeMillis() - startMillis;


            // Check consistency with Postgres results: unary preds
            for (ExpressionInfo expr : query.unaryPredicates) {
                // Unary predicates must refer to one table
                if (expr.aliasesMentioned.size() != 1) {
                    throw new Exception("Alias " + expr + " must mention one table!");
                }

                // Get cardinality after Skinner filtering
                String alias = expr.aliasesMentioned.iterator().next();
                String filteredName = preSummary.aliasToFiltered.get(alias);
                TableInfo filteredTable = CatalogManager.currentDB.
                        nameToTable.get(filteredName);
                String columnName = filteredTable.nameToCol.keySet().iterator().next();
                ColumnRef colRef = new ColumnRef(filteredName, columnName);
                int skinnerCardinality = BufferManager.colToData.get(colRef).getCardinality();
                System.out.println("Skinner card:\t" + skinnerCardinality);
            }

            // Get cardinality of Skinner join result
            int skinnerJoinCard = CatalogManager.getCardinality(
                    NamingConfig.JOINED_NAME);
            System.out.println("Skinner join card:\t" + skinnerJoinCard);

            // Output final result for Skinner
            String resultRel = NamingConfig.FINAL_RESULT_NAME;
            System.setOut(skinnerOut);
            RelationPrinter.print(resultRel);
            skinnerOut.flush();
            System.setOut(console);
            // Generate output
            benchOut.print(entry.getKey() + "\t");
            benchOut.print(totalMillis + "\t");
            benchOut.print(preMillis + "\t");
            benchOut.print(postMillis + "\t");
            benchOut.print(JoinStats.nrTuples + "\t");
            benchOut.print(JoinStats.nrIterations + "\t");
            benchOut.print(JoinStats.nrIndexLookups + "\t");
            benchOut.print(JoinStats.nrIndexEntries + "\t");
            benchOut.print(JoinStats.nrUniqueIndexLookups + "\t");
            benchOut.print(JoinStats.nrUctNodes + "\t");
            benchOut.print(JoinStats.nrPlansTried + "\t");
            benchOut.print(skinnerJoinCard + "\t");
            benchOut.print(JoinStats.nrSamples + "\t");
            benchOut.print(JoinStats.avgReward + "\t");
            benchOut.print(JoinStats.maxReward + "\t");
            benchOut.println(JoinStats.totalWork);
            benchOut.flush();
            // Clean up
            BufferManager.unloadTempData();
            CatalogManager.removeTempTables();
        }
        benchOut.close();
        skinnerOut.close();
    }
}
