package benchmark;

import buffer.BufferManager;
import config.JoinConfig;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

public class JoinCompare {
    public static int yes = 0;
    public static int no = 0;
    public static int sum = 0;

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Specify Skinner DB dir, " + "query directory");
            return;
        }
        /*
        if(sum == 0) {
            String SkinnerDbDir = args[0];
            PathUtil.initSchemaPaths(SkinnerDbDir);
            CatalogManager.loadDB(PathUtil.schemaPath);
            PathUtil.initDataPaths(CatalogManager.currentDB);
            BufferManager.loadDB();
        }
        */

        Path resultsPath = new File("skinnerResults.txt").toPath();

        JoinConfig.USE_RIPPLE = true;
        BenchMarkSkinner.main(args);
        String[] result1 = Files.readAllLines(resultsPath).toArray(new String[]{});

        BufferManager.colToIndex.clear();

        JoinConfig.USE_RIPPLE = false;
        BenchMarkSkinner.main(args);
        String[] result2 = Files.readAllLines(resultsPath).toArray(new String[]{});

        BufferManager.colToIndex.clear();

        boolean arePermutations = arePermutations(result1, result2);

        sum++;

        if (arePermutations) {
            yes++;
            System.out.println("All Same: " + yes + " times");
        } else {
            no++;
            System.out.println("Not Same: " + no + " times");
        }
        if (sum == 10) {
            System.out.println("End");
            System.out.println("Yes: " + yes);
            System.out.println("No: " + no);
            return;
        }
        main(args);
    }

    static boolean arePermutations(String[] arr1, String[] arr2) {
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