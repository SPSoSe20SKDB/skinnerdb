package benchmark;

import buffer.BufferManager;
import catalog.CatalogManager;
import config.GeneralConfig;
import config.JoinConfig;
import config.PreConfig;
import diskio.PathUtil;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

public class JoinCompare {
    public static int yes = 0;
    public static int no = 0;
    public static int sum = 0;
    public static final boolean printDebug = true;
    public static Runtime rt = Runtime.getRuntime();
    public static double timeDiff = 0;
    public static double ramDiff = 0;

    public static void main(String[] args) throws Exception {
        GeneralConfig.isComparing = true;

        long rippleTime;
        long noRippleTime;
        long rippleRam;
        long noRippleRam;

        if (args.length != 2) {
            System.out.println("Specify Skinner DB dir, " + "query directory");
            return;
        }

        // load database to ram on first run
        if (sum == 0) {
            String SkinnerDbDir = args[0];
            PathUtil.initSchemaPaths(SkinnerDbDir);
            CatalogManager.loadDB(PathUtil.schemaPath);
            PathUtil.initDataPaths(CatalogManager.currentDB);
            BufferManager.loadDB();
        }

        Path resultsPath = new File("skinnerResults.txt").toPath();

        // clear hash tables
        BufferManager.colToIndex.clear();
        rt.gc();

        // setup config for original-join
        PreConfig.PRE_FILTER = true;
        PreConfig.CONSIDER_INDICES = true;
        JoinConfig.USE_RIPPLE = false;
        // initialize time and ram measurement
        noRippleTime = System.currentTimeMillis();
        noRippleRam = rt.totalMemory() - rt.freeMemory();
        // run join
        BenchMarkSkinner.main(args);
        // save time and ram measurement
        noRippleTime = System.currentTimeMillis() - noRippleTime;
        noRippleRam = rt.totalMemory() - rt.freeMemory() - noRippleRam;
        // save result-tuples
        String[] result1 = Files.readAllLines(resultsPath).toArray(new String[]{});

        // clear hash tables
        BufferManager.colToIndex.clear();
        rt.gc();

        // setup config for ripple-join
        PreConfig.PRE_FILTER = true;
        PreConfig.CONSIDER_INDICES = false;
        JoinConfig.USE_RIPPLE = true;
        // initialize time and ram measurement
        rippleTime = System.currentTimeMillis();
        rippleRam = rt.totalMemory() - rt.freeMemory();
        // run join
        BenchMarkSkinner.main(args);
        // save time and ram measurement
        rippleTime = System.currentTimeMillis() - rippleTime;
        rippleRam = rt.totalMemory() - rt.freeMemory() - rippleRam;
        // save result-tuples
        String[] result2 = Files.readAllLines(resultsPath).toArray(new String[]{});

        // test is tuples are same
        boolean arePermutations = arePermutations(result1, result2);

        sum++;

        if (arePermutations) {
            yes++;
            if (printDebug) System.out.println("Same: " + yes + " times / " + sum);
        } else {
            no++;
            if (printDebug) System.out.println("Not Same: " + no + " times / " + sum);
        }
        if (printDebug)
            System.out.println("Time-Diff: " + (rippleTime - noRippleTime) + " ms; " + round2((rippleTime - noRippleTime) * 100.0d / noRippleTime) + " %; NoR: " + noRippleTime + " ms; R: " + rippleTime + " ms");
        if (printDebug)
            System.out.println("Ram-Diff: " + round2((rippleRam - noRippleRam) / 1024.0d / 1024) + " MB; " + round2((rippleRam - noRippleRam) * 100.0d / noRippleRam) + " %; NoR: " + round2(noRippleRam / 1024.0d / 1024) + " MB; R: " + round2(rippleRam / 1024.0d / 1024) + " MB");

        timeDiff = (timeDiff * (sum - 1) + ((rippleTime - noRippleTime) * 100.0d / noRippleTime)) / sum;
        ramDiff = (ramDiff * (sum - 1) + ((rippleRam - noRippleRam) * 100.0d / noRippleRam)) / sum;

        System.out.println("-----------------------------------");

        // at the end print summary
        if (sum == 100) {
            System.out.println("End");
            System.out.println("Same: " + yes);
            System.out.println("Not Same: " + no);
            System.out.println("Time-Diff-Sum: " + round2(timeDiff) + " %");
            System.out.println("Ram-Diff-Sum: " + round2(ramDiff) + " %");
            return;
        }

        // continue testing
        main(args);
    }

    static boolean arePermutations(String[] arr1, String[] arr2) {
        if (arr1.length != arr2.length) return false;
        HashMap<String, Integer> hM = new HashMap<>();
        for (int i = 0; i < arr1.length; i++) {
            String x = arr1[i];
            if (hM.get(x) == null)
                hM.put(x, 1);
            else {
                int k = hM.get(x);
                hM.put(x, k + 1);
            }
        }
        for (int i = 0; i < arr2.length; i++) {
            String x = arr2[i];
            if (hM.get(x) == null || hM.get(x) == 0) return false;
            int k = hM.get(x);
            hM.put(x, k - 1);
        }
        return true;
    }

    static double round2(double d) {
        return Math.round(d * 100.0) / 100.0;
    }
}