package benchmark;

/*
import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.TableInfo;
import config.GeneralConfig;
import config.NamingConfig;
import diskio.PathUtil;
import expressions.ExpressionInfo;
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
*/

import config.JoinConfig;


/**
 * TODO: compare old vs new join
 */
public class JoinCompare {

    public static void main(String[] args) throws Exception {
        Runtime rt = Runtime.getRuntime();

        // Check for command line parameters
        if (args.length != 2) {
            System.out.println("Specify Skinner DB dir, " + "query directory");
            return;
        }

        //Get required information
        //String SkinnerDbDir = args[0];
        //String queryDir = args[1];

        JoinConfig.USE_RIPPLE = true;
        BenchMarkSkinner.main(args);


        JoinConfig.USE_RIPPLE = false;
        BenchMarkSkinner.main(args);

    }
}