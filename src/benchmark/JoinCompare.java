package benchmark;

import buffer.BufferManager;
import config.JoinConfig;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

public class JoinCompare {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Specify Skinner DB dir, " + "query directory");
            return;
        }

        Path resultsPath = new File("skinnerResults.txt").toPath();

        JoinConfig.USE_RIPPLE = true;
        BenchMarkSkinner.main(args);
        String[] result1 = Files.readAllLines(resultsPath).toArray(new String[]{});

        BufferManager.colToIndex.clear();

        JoinConfig.USE_RIPPLE = false;
        BenchMarkSkinner.main(args);
        String[] result2 = Files.readAllLines(resultsPath).toArray(new String[]{});

        boolean arePermutations = arePermutations(result1, result2);

        if (arePermutations) {
            System.out.println("All Same");
        } else {
            System.out.println("Not Same");
        }
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