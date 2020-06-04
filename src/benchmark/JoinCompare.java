package benchmark;

import buffer.BufferManager;
import catalog.CatalogManager;
import config.GeneralConfig;
import config.JoinConfig;
import config.PreConfig;
import diskio.PathUtil;
import statistics.JoinStats;
import statistics.PostStats;
import statistics.PreStats;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

public class JoinCompare {
    public static final boolean printDebug = false;
    public static final int amountTesting = 100;
    public static int yes = 0;
    public static int no = 0;
    public static int sum = 0;
    public static Runtime rt = Runtime.getRuntime();
    public static double timeDiff = 0;
    public static double timeDiffPre = 0;
    public static double timeDiffJoin = 0;
    public static double timeDiffPost = 0;
    public static double ramDiff = 0;
    public static double ramDiffPre = 0;
    public static double ramDiffJoin = 0;
    public static double ramDiffPost = 0;
    public static double ramDiffIndex = 0;

    public static void main(String[] args) throws Exception {
        GeneralConfig.isComparing = true;

        for (int i = 1; i <= amountTesting; i++) {
            test(args);
        }

        // at the end print summary
        System.out.println("End");
        System.out.println("Same: " + yes);
        System.out.println("Not Same: " + no);
        System.out.println();
        String pattern = "%7s %1s %8s %3s %8s %1s";
        System.out.println(String.format(pattern, "Metrik", "|", "RAM", "|", "Zeit", ""));
        System.out.println(String.format("%s", "---------------------------------"));
        System.out.println(String.format(pattern, "Sum", "|", (ramDiff >= 0 ? "+" : "") + round2(ramDiff), "% |", (timeDiff >= 0 ? "+" : "") + round2(timeDiff), "%"));
        System.out.println(String.format(pattern, "Pre", "|", (ramDiffPre >= 0 ? "+" : "") + round2(ramDiffPre), "% |", (timeDiffPre >= 0 ? "+" : "") + round2(timeDiffPre), "%"));
        System.out.println(String.format(pattern, "Join", "|", (ramDiffJoin >= 0 ? "+" : "") + round2(ramDiffJoin), "% |", (timeDiffJoin >= 0 ? "+" : "") + round2(timeDiffJoin), "%"));
        System.out.println(String.format(pattern, "Post", "|", (ramDiffPost >= 0 ? "+" : "") + round2(ramDiffPost), "% |", (timeDiffPost >= 0 ? "+" : "") + round2(timeDiffPost), "%"));
        System.out.println(String.format(pattern, "Index", "|", (ramDiffIndex >= 0 ? "+" : "") + round2(ramDiffIndex), "% |", "", ""));
    }

    public static void test(String[] args) throws Exception {
        long rippleTime;
        long noRippleTime;
        long rippleTimePre;
        long noRippleTimePre;
        long rippleTimeJoin;
        long noRippleTimeJoin;
        long rippleTimePost;
        long noRippleTimePost;

        long rippleRam;
        long noRippleRam;
        long rippleRamPre;
        long noRippleRamPre;
        long rippleRamJoin;
        long noRippleRamJoin;
        long rippleRamPost;
        long noRippleRamPost;
        long rippleRamIndex;
        long noRippleRamIndex;

        if (args.length != 2) {
            System.out.println("Specify Skinner DB dir, " + "query directory");
            return;
        }

        // load database to ram on first run
        if (sum == 0) {
            String SkinnerDbDir = args[0];
            GeneralConfig.inMemory = true;
            PathUtil.initSchemaPaths(SkinnerDbDir);
            CatalogManager.loadDB(PathUtil.schemaPath);
            PathUtil.initDataPaths(CatalogManager.currentDB);
            BufferManager.loadDB();
        }

        Path resultsPath = new File("skinnerResults.txt").toPath();

        PreStats.preRam = 0;
        JoinStats.joinRam = 0;
        PostStats.postRam = 0;

        PreStats.preMillis = 0;
        JoinStats.joinMillis = 0;
        PostStats.postMillis = 0;

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

        rt.gc();

        // clear hash tables
        noRippleRamIndex = rt.totalMemory() - rt.freeMemory();
        BufferManager.colToIndex.clear();
        rt.gc();
        noRippleRamIndex = noRippleRamIndex - (rt.totalMemory() - rt.freeMemory());

        noRippleRamPre = PreStats.preRam;
        noRippleRamJoin = JoinStats.joinRam;
        noRippleRamPost = PostStats.postRam;

        noRippleTimePre = PreStats.preMillis;
        noRippleTimeJoin = JoinStats.joinMillis;
        noRippleTimePost = PostStats.postMillis;

        PreStats.preRam = 0;
        JoinStats.joinRam = 0;
        PostStats.postRam = 0;

        PreStats.preMillis = 0;
        JoinStats.joinMillis = 0;
        PostStats.postMillis = 0;

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

        rt.gc();

        // clear hash tables
        rippleRamIndex = rt.totalMemory() - rt.freeMemory();
        BufferManager.colToIndex.clear();
        rt.gc();
        rippleRamIndex = rippleRamIndex - (rt.totalMemory() - rt.freeMemory());

        rippleRamPre = PreStats.preRam;
        rippleRamJoin = JoinStats.joinRam;
        rippleRamPost = PostStats.postRam;

        rippleTimePre = PreStats.preMillis;
        rippleTimeJoin = JoinStats.joinMillis;
        rippleTimePost = PostStats.postMillis;

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
        timeDiffPre = (timeDiffPre * (sum - 1) + ((rippleTimePre - noRippleTimePre) * 100.0d / noRippleTimePre)) / sum;
        timeDiffJoin = (timeDiffJoin * (sum - 1) + ((rippleTimeJoin - noRippleTimeJoin) * 100.0d / noRippleTimeJoin)) / sum;
        timeDiffPost = (timeDiffPost * (sum - 1) + ((rippleTimePost - noRippleTimePost) * 100.0d / noRippleTimePost)) / sum;

        ramDiff = (ramDiff * (sum - 1) + ((rippleRam - noRippleRam) * 100.0d / noRippleRam)) / sum;
        ramDiffPre = (ramDiffPre * (sum - 1) + ((rippleRamPre - noRippleRamPre) * 100.0d / noRippleRamPre)) / sum;
        ramDiffJoin = (ramDiffJoin * (sum - 1) + ((rippleRamJoin - noRippleRamJoin) * 100.0d / noRippleRamJoin)) / sum;
        ramDiffPost = (ramDiffPost * (sum - 1) + ((rippleRamPost - noRippleRamPost) * 100.0d / noRippleRamPost)) / sum;
        ramDiffIndex = (ramDiffIndex * (sum - 1) + ((rippleRamIndex - noRippleRamIndex) * 100.0d / noRippleRamIndex)) / sum;

        System.out.println("-----------------------------------");
    }

    public static boolean arePermutations(String[] arr1, String[] arr2) {
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