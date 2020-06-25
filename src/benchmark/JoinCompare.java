package benchmark;

import buffer.BufferManager;
import catalog.CatalogManager;
import config.GeneralConfig;
import config.JoinConfig;
import config.PreConfig;
import diskio.PathUtil;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.labels.StandardCategoryItemLabelGenerator;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.renderer.category.CategoryItemRenderer;
import org.jfree.data.category.DefaultCategoryDataset;
import statistics.JoinStats;
import statistics.PostStats;
import statistics.PreStats;

import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Random;

public class JoinCompare {
    public static final boolean printDebug = true;
    public static final int amountTesting = 10;
    public static final int forgetSkip = 0;
    public static int yes = 0;
    public static int no = 0;
    public static int sum = 0;
    public static Runtime rt = Runtime.getRuntime();
    public static Path resultsPath = new File("skinnerResults.txt").toPath();

    public static JFreeChart chart_zeit_messung;
    public static JFreeChart chart_ram_messung;
    public static JFreeChart chart_prozent;

    public static double[][] metrics = new double[8][5];

    public static String pattern = "%7s %1s %8s %3s %8s %3s %8s %4s %8s %4s %8s %3s %8s %3s %8s %4s %8s %2s";

    public static void main(String[] args) throws Exception {
        GeneralConfig.isComparing = true;

        if (args.length != 2) {
            System.out.println("Specify Skinner DB dir, " + "query directory");
            return;
        }

        // load database to ram on first run

        Random random = new Random();

        for (int i = 0; i < amountTesting; i++) {
            if (i == 0 || (forgetSkip > 0 && i % forgetSkip == 0)) initDB(args);
            if (random.nextBoolean()) test(args);
            else testRev(args);

            updateDiagram();
        }

        // at the end print summary
        System.out.println("End");
        System.out.println("Same: " + yes);
        System.out.println("Not Same: " + no);
        System.out.println();

        for (int i = 0; i < 5; i++) {
            metrics[1][i] = calcPer(metrics[2][i], metrics[3][i]);
            metrics[5][i] = calcPer(metrics[6][i], metrics[7][i]);
        }

        // tabellarische datstellung
        System.out.println(String.format(pattern, "Metrik", "|", "RAM % X", "|", "RAM % N", "|", "RAM R", "|", "RAM NR", "|", "Zeit % X", "|", "Zeit % N", "|", "Zeit R", "|", "Zeit NR", ""));
        System.out.println(String.format("%s", "--------------------------------------------------------------------------------------------------------------------"));
        System.out.println(formatTableLine("Sum", 0));
        System.out.println(formatTableLine("Pre", 1));
        System.out.println(formatTableLine("Join", 2));
        System.out.println(formatTableLine("Post", 3));
        System.out.println(formatTableLine("Index", 4));
    }

    private static void updateDiagram() {
        final String s1 = "RIPPLE";
        final String s2 = "NO RIPPLE";
        final String s3 = "RAM";
        final String s4 = "TIME";
        final DefaultCategoryDataset CategoryDataset_Time = new DefaultCategoryDataset();
        final DefaultCategoryDataset CategoryDataset_Ram = new DefaultCategoryDataset();
        final DefaultCategoryDataset CategoryDataset_Prozent = new DefaultCategoryDataset();

        CategoryDataset_Ram.addValue(metrics[2][0] / 1024 / 1024 / sum, s3, s1);
        CategoryDataset_Ram.addValue(metrics[3][0] / 1024 / 1024 /sum, s3, s2);

        CategoryDataset_Time.addValue(metrics[6][0] / sum, s4, s1);
        CategoryDataset_Time.addValue(metrics[7][0] / sum, s4, s2);

        CategoryDataset_Prozent.addValue(-metrics[4][0] / sum, "%", "Time");
        CategoryDataset_Prozent.addValue(-metrics[0][0]  / sum, "%", "Ram");

        if (chart_zeit_messung != null) {
            ((CategoryPlot) chart_zeit_messung.getPlot()).setDataset(CategoryDataset_Time);
        }
        else {
            final JFreeChart barChart = ChartFactory.createBarChart("SkinnerDB - Ripple-Join-Extension-Time-Benchmark", "Metrik", "Zeit in ms", CategoryDataset_Time);
            final CategoryPlot categoryPlot = (CategoryPlot) barChart.getPlot();
            final NumberAxis numberAxis = (NumberAxis) categoryPlot.getRangeAxis();
            numberAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
            numberAxis.setUpperMargin(0.15);
            final CategoryItemRenderer renderer = categoryPlot.getRenderer();
            renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
            renderer.setSeriesItemLabelsVisible(0, Boolean.TRUE);
            categoryPlot.getDomainAxis().setCategoryLabelPositions(CategoryLabelPositions.UP_45);

            chart_zeit_messung = barChart;

            final ChartPanel contentPane = new ChartPanel(barChart);
            contentPane.setPreferredSize(new Dimension(500, 400));
            JFrame frame = new JFrame();
            frame.setContentPane(contentPane);
            frame.pack();
            frame.setVisible(true);
        }

        if (chart_ram_messung != null) {
            ((CategoryPlot) chart_ram_messung.getPlot()).setDataset(CategoryDataset_Ram);
        }
        else {
            final JFreeChart barChart = ChartFactory.createBarChart("SkinnerDB - Ripple-Join-Extension-RAM-Benchmark", "Metrik", "Ram-Verbrauch in MB", CategoryDataset_Ram);
            final CategoryPlot categoryPlot = (CategoryPlot) barChart.getPlot();
            final NumberAxis numberAxis = (NumberAxis) categoryPlot.getRangeAxis();
            numberAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
            numberAxis.setUpperMargin(0.15);
            final CategoryItemRenderer renderer = categoryPlot.getRenderer();
            renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
            renderer.setSeriesItemLabelsVisible(0, Boolean.TRUE);
            categoryPlot.getDomainAxis().setCategoryLabelPositions(CategoryLabelPositions.UP_45);

            chart_ram_messung = barChart;

            final ChartPanel contentPane = new ChartPanel(barChart);
            contentPane.setPreferredSize(new Dimension(500, 400));
            JFrame frame = new JFrame();
            frame.setContentPane(contentPane);
            frame.pack();
            frame.setVisible(true);
        }

        if (chart_prozent != null) {
            ((CategoryPlot) chart_prozent.getPlot()).setDataset(CategoryDataset_Prozent);
        }
        else {
            final JFreeChart barChart = ChartFactory.createBarChart("SkinnerDB - Ripple-Join-Extension-Benchmark-Conclusion", "Metrik", "Prozentual", CategoryDataset_Prozent);
            final CategoryPlot categoryPlot = (CategoryPlot) barChart.getPlot();
            final NumberAxis numberAxis = (NumberAxis) categoryPlot.getRangeAxis();
            numberAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
            numberAxis.setUpperMargin(0.15);
            final CategoryItemRenderer renderer = categoryPlot.getRenderer();
            renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
            renderer.setSeriesItemLabelsVisible(0, Boolean.TRUE);
            categoryPlot.getDomainAxis().setCategoryLabelPositions(CategoryLabelPositions.UP_45);

            chart_prozent = barChart;

            final ChartPanel contentPane = new ChartPanel(barChart);
            contentPane.setPreferredSize(new Dimension(500, 400));
            JFrame frame = new JFrame();
            frame.setContentPane(contentPane);
            frame.pack();
            frame.setVisible(true);
        }
    }

    private static void initDB(String[] args) throws Exception {
        String SkinnerDbDir = args[0];
        GeneralConfig.inMemory = true;
        PathUtil.initSchemaPaths(SkinnerDbDir);
        CatalogManager.loadDB(PathUtil.schemaPath);
        PathUtil.initDataPaths(CatalogManager.currentDB);
        BufferManager.loadDB();
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
            System.out.println("Time-Diff: " + formatDouble((rippleTime - noRippleTime), false, true) + " ms; " + formatDouble(calcPer(rippleTime, noRippleTime), false, true) + " %; NoR: " + formatDouble(noRippleTime, false, false) + " ms; R: " + formatDouble(rippleTime, false, false) + " ms");
        if (printDebug)
            System.out.println("Ram-Diff: " + formatDouble((rippleRam - noRippleRam), true, true) + " MB; " + formatDouble(calcPer(rippleRam, noRippleRam), false, true) + " %; NoR: " + formatDouble(noRippleRam, true, false) + " MB; R: " + formatDouble(rippleRam, true, false) + " MB");

        metrics[0][0] = addVal(metrics[0][0], sum, calcPer(rippleRam, noRippleRam));
        metrics[0][1] = addVal(metrics[0][1], sum, calcPer(rippleRamPre, noRippleRamPre));
        metrics[0][2] = addVal(metrics[0][2], sum, calcPer(rippleRamJoin, noRippleRamJoin));
        metrics[0][3] = addVal(metrics[0][3], sum, calcPer(rippleRamPost, noRippleRamPost));
        metrics[0][4] = addVal(metrics[0][4], sum, calcPer(rippleRamIndex, noRippleRamIndex));

        metrics[2][0] = addVal(metrics[2][0], sum, rippleRam);
        metrics[2][1] = addVal(metrics[2][1], sum, rippleRamPre);
        metrics[2][2] = addVal(metrics[2][2], sum, rippleRamJoin);
        metrics[2][3] = addVal(metrics[2][3], sum, rippleRamPost);
        metrics[2][4] = addVal(metrics[2][4], sum, rippleRamIndex);

        metrics[3][0] = addVal(metrics[3][0], sum, noRippleRam);
        metrics[3][1] = addVal(metrics[3][1], sum, noRippleRamPre);
        metrics[3][2] = addVal(metrics[3][2], sum, noRippleRamJoin);
        metrics[3][3] = addVal(metrics[3][3], sum, noRippleRamPost);
        metrics[3][4] = addVal(metrics[3][4], sum, noRippleRamIndex);

        metrics[4][0] = addVal(metrics[4][0], sum, calcPer(rippleTime, noRippleTime));
        metrics[4][1] = addVal(metrics[4][1], sum, calcPer(rippleTimePre, noRippleTimePre));
        metrics[4][2] = addVal(metrics[4][2], sum, calcPer(rippleTimeJoin, noRippleTimeJoin));
        metrics[4][3] = addVal(metrics[4][3], sum, calcPer(rippleTimePost, noRippleTimePost));

        metrics[6][0] = addVal(metrics[6][0], sum, rippleTime);
        metrics[6][1] = addVal(metrics[6][1], sum, rippleTimePre);
        metrics[6][2] = addVal(metrics[6][2], sum, rippleTimeJoin);
        metrics[6][3] = addVal(metrics[6][3], sum, rippleTimePost);

        metrics[7][0] = addVal(metrics[7][0], sum, noRippleTime);
        metrics[7][1] = addVal(metrics[7][1], sum, noRippleTimePre);
        metrics[7][2] = addVal(metrics[7][2], sum, noRippleTimeJoin);
        metrics[7][3] = addVal(metrics[7][3], sum, noRippleTimePost);

        System.out.println("-----------------------------------");
    }

    public static void testRev(String[] args) throws Exception {
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
            System.out.println("Time-Diff: " + formatDouble((rippleTime - noRippleTime), false, true) + " ms; " + formatDouble(calcPer(rippleTime, noRippleTime), false, true) + " %; NoR: " + formatDouble(noRippleTime, false, false) + " ms; R: " + formatDouble(rippleTime, false, false) + " ms");
        if (printDebug)
            System.out.println("Ram-Diff: " + formatDouble((rippleRam - noRippleRam), true, true) + " MB; " + formatDouble(calcPer(rippleRam, noRippleRam), false, true) + " %; NoR: " + formatDouble(noRippleRam, true, false) + " MB; R: " + formatDouble(rippleRam, true, false) + " MB");

        metrics[0][0] = addVal(metrics[0][0], sum, calcPer(rippleRam, noRippleRam));
        metrics[0][1] = addVal(metrics[0][1], sum, calcPer(rippleRamPre, noRippleRamPre));
        metrics[0][2] = addVal(metrics[0][2], sum, calcPer(rippleRamJoin, noRippleRamJoin));
        metrics[0][3] = addVal(metrics[0][3], sum, calcPer(rippleRamPost, noRippleRamPost));
        metrics[0][4] = addVal(metrics[0][4], sum, calcPer(rippleRamIndex, noRippleRamIndex));

        metrics[2][0] = addVal(metrics[2][0], sum, rippleRam);
        metrics[2][1] = addVal(metrics[2][1], sum, rippleRamPre);
        metrics[2][2] = addVal(metrics[2][2], sum, rippleRamJoin);
        metrics[2][3] = addVal(metrics[2][3], sum, rippleRamPost);
        metrics[2][4] = addVal(metrics[2][4], sum, rippleRamIndex);

        metrics[3][0] = addVal(metrics[3][0], sum, noRippleRam);
        metrics[3][1] = addVal(metrics[3][1], sum, noRippleRamPre);
        metrics[3][2] = addVal(metrics[3][2], sum, noRippleRamJoin);
        metrics[3][3] = addVal(metrics[3][3], sum, noRippleRamPost);
        metrics[3][4] = addVal(metrics[3][4], sum, noRippleRamIndex);

        metrics[4][0] = addVal(metrics[4][0], sum, calcPer(rippleTime, noRippleTime));
        metrics[4][1] = addVal(metrics[4][1], sum, calcPer(rippleTimePre, noRippleTimePre));
        metrics[4][2] = addVal(metrics[4][2], sum, calcPer(rippleTimeJoin, noRippleTimeJoin));
        metrics[4][3] = addVal(metrics[4][3], sum, calcPer(rippleTimePost, noRippleTimePost));

        metrics[6][0] = addVal(metrics[6][0], sum, rippleTime);
        metrics[6][1] = addVal(metrics[6][1], sum, rippleTimePre);
        metrics[6][2] = addVal(metrics[6][2], sum, rippleTimeJoin);
        metrics[6][3] = addVal(metrics[6][3], sum, rippleTimePost);

        metrics[7][0] = addVal(metrics[7][0], sum, noRippleTime);
        metrics[7][1] = addVal(metrics[7][1], sum, noRippleTimePre);
        metrics[7][2] = addVal(metrics[7][2], sum, noRippleTimeJoin);
        metrics[7][3] = addVal(metrics[7][3], sum, noRippleTimePost);

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

    public static double round2(double d) {
        return Math.round(d * 100.0) / 100.0;
    }

    public static double addVal(double pre, int sum, double addVal) {
        //return (pre * (sum - 1) + addVal) / sum;
        return (addVal > 0) ? pre + addVal : pre;
    }

    public static double calcPer(double ripple, double noRipple) {
        return 100 * (ripple - noRipple) / noRipple;
    }

    public static String formatDouble(double d, boolean isRAM, boolean addPlus) {
        if (isRAM) d = (d / 1024) / 1024;
        d = round2(d);
        if (!addPlus) return "" + d;
        return (d > 0) ? "+" + d : "" + d;
    }

    public static String formatTableLine(String name, int metricIndex) {
        return String.format(
                pattern,
                name,
                "|",
                formatDouble(metrics[0][metricIndex] / amountTesting, false, true),
                "% |",
                formatDouble(metrics[1][metricIndex], false, true),
                "% |",
                formatDouble(metrics[2][metricIndex] / amountTesting, true, false),
                "MB |",
                formatDouble(metrics[3][metricIndex] / amountTesting, true, false),
                "MB |",
                formatDouble(metrics[4][metricIndex] / amountTesting, false, true),
                "% |",
                formatDouble(metrics[5][metricIndex], false, true),
                "% |",
                formatDouble(metrics[6][metricIndex] / amountTesting, false, false),
                "ms |",
                formatDouble(metrics[7][metricIndex] / amountTesting, false, false),
                "ms"
        );
    }
}