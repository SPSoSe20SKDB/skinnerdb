package joining.join;

import buffer.BufferManager;
import data.DoubleData;
import data.IntData;
import indexing.LiveIndex;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import types.JavaType;

import java.util.Set;

public class JoinNoIndexWrapper extends JoinIndexWrapper {
    private final JavaType type;

    private LiveIndex liveIndex;

    /**
     * Initialize index wrapper for
     * given query and join order.
     *
     * @param queryInfo  query meta-data
     * @param preSummary maps query columns to intermediate result columns
     * @param joinCols   pair of columns in equi-join predicate
     * @param order      join order
     * @param javaType
     * @throws Exception
     */
    public JoinNoIndexWrapper(QueryInfo queryInfo, Context preSummary, Set<ColumnRef> joinCols, int[] order, JavaType javaType) throws Exception {
        super(queryInfo, preSummary, joinCols, order);
        type = javaType;

        if (nextIndex == null) {
            nextIndex = new LiveIndex(nextData.cardinality, javaType);
            nextIndex.data = nextData;
            BufferManager.colToIndex.put(nextRef, nextIndex);
        }

        liveIndex = (LiveIndex) nextIndex;
    }

    @Override
    public int nextIndex(int[] tupleIndices) {
        // TODO: Die nächste Zeile als "Zeilennummer" zurückgeben
        // Hier eine Zeile zufällig auswählen und hashen
        // dazu: Die Daten der Spalte befinden sich in this.nextData
        // Spalte selbst ist als referenz gegeben aus int[]
        // Die zu füllende Hash-Tabelle ist in this.liveIndex
        // this.liveIndex ist vom Typ "LiveIndex extends Index". Die Funktionalität ist noch offen.

        int n = liveIndex.getRandomNotHashed();
        if (n >= liveIndex.cardinality) return n;
        Object data = null;
        switch (nextData.getClass().getSimpleName()) {
            case "IntData":
                data = ((IntData) nextData).data[n];
                break;
            case "DoubleData":
                data = ((DoubleData) nextData).data[n];
                break;
        }
        liveIndex.addHash(n, data);

        return n; // zeilennummer = zufallszahl
    }

    @Override
    public int nrIndexed(int[] tupleIndices) {
        return liveIndex.nrIndexed;
    }

    public void resetRandomList() {
        liveIndex.resetRandomList();
    }
}
