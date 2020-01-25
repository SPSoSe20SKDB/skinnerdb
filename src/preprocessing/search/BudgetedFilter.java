package preprocessing.search;

import catalog.CatalogManager;
import expressions.compilation.UnaryBoolEval;

import java.util.ArrayList;
import java.util.List;

public class BudgetedFilter {
    private final int cardinality;
    private int index;
    private int completedTuples;
    private List<Integer> result;
    private List<UnaryBoolEval> compiled;

    public BudgetedFilter(String tableName, List<UnaryBoolEval> compiled) {
        this.result = new ArrayList<>();
        this.compiled = compiled;
        this.cardinality = CatalogManager.getCardinality(tableName);
        this.completedTuples = 0;
    }

    private void executeWithBudget(int budget, int[] order) {
        int remainingBudget = budget;
        // last completed row index
        int currentIndex = index;

        ROW_LOOP:
        while (remainingBudget > 0 && currentIndex + 1 < cardinality) {
            currentIndex++;
            for (int predIndex : order) {
                --remainingBudget;
                if (compiled.get(predIndex).evaluate(currentIndex) <= 0) {
                    continue ROW_LOOP;
                }
            }
            result.add(currentIndex);
        }

        index = currentIndex;
        completedTuples = currentIndex - index;
    }

    public boolean isFinished() {
        return index == cardinality - 1;
    }

    public int getCompletedTuples() {
        return completedTuples;
    }
}