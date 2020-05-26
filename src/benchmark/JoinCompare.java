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

    public static void main(String[] args) throws Exception {
        GeneralConfig.isComparing = true;

        long rippleTime = 0;
        long noRippleTime = 0;


        if (args.length != 2) {
            System.out.println("Specify Skinner DB dir, " + "query directory");
            return;
        }

        if (sum == 0) {
            String SkinnerDbDir = args[0];
            PathUtil.initSchemaPaths(SkinnerDbDir);
            CatalogManager.loadDB(PathUtil.schemaPath);
            PathUtil.initDataPaths(CatalogManager.currentDB);
            BufferManager.loadDB();
        }

        Path resultsPath = new File("skinnerResults.txt").toPath();

        BufferManager.colToIndex.clear();

        PreConfig.CONSIDER_INDICES = true;
        JoinConfig.USE_RIPPLE = false;
        noRippleTime = System.currentTimeMillis();
        BenchMarkSkinner.main(args);
        noRippleTime = System.currentTimeMillis() - noRippleTime;
        String[] result1 = Files.readAllLines(resultsPath).toArray(new String[]{});

        BufferManager.colToIndex.clear();

        PreConfig.CONSIDER_INDICES = false;
        JoinConfig.USE_RIPPLE = true;
        rippleTime = System.currentTimeMillis();
        BenchMarkSkinner.main(args);
        rippleTime = System.currentTimeMillis() - rippleTime;
        String[] result2 = Files.readAllLines(resultsPath).toArray(new String[]{});

        boolean arePermutations = arePermutations(result1, result2);

        sum++;

        if (arePermutations) {
            yes++;
            System.out.println("All Same: " + yes + " times / " + sum);
        } else {
            no++;
            System.out.println("Not Same: " + no + " times / " + sum);
        }
        System.out.println("Time-Diff: " + (rippleTime - noRippleTime) + " ms; " + ((rippleTime - noRippleTime) * 100.0d / noRippleTime) + " %");

        if (sum == 100) {
            System.out.println("End");
            System.out.println("Yes: " + yes);
            System.out.println("No: " + no);
            return;
        }
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
}