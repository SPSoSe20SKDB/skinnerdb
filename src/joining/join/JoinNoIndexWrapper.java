package joining.join;

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

        nextIndex = new LiveIndex(nextData.cardinality);
        nextIndex.data = nextData;

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
        int zufallsZahl = (int)(Math.random()*tupleIndices.length); //wertebereich geht bis zur länge des Arrays
        this.liveIndex.set(tupleIndices[zufallsZahl]);
        return zufallsZahl; // zeilennummer = zufallszahl
    }

    @Override
    public int nrIndexed(int[] tupleIndices) {
        return liveIndex.nrIndexed;
    }
}
