package joining.join;

import buffer.BufferManager;
import data.ColumnData;
import data.DoubleData;
import data.IntData;
import indexing.LiveIndex;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import types.JavaType;

import java.util.Set;

public class JoinNoIndexWrapper<T> extends JoinIndexWrapper {
    private final JavaType type;

    public static int nextIndexCalled = 0;
    private LiveIndex<T> liveIndex;

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
            nextIndex = new LiveIndex<T>(nextData.cardinality, javaType);
            nextIndex.data = nextData;
            BufferManager.colToIndex.put(nextRef, nextIndex);
        }

        liveIndex = (LiveIndex) nextIndex;
    }

    @Override
    public int nextIndex(int[] tupleIndices) {
        // TODO: Hashtabelle aufbauen und danach dann auf den fertigen hashtabellen arbeiten
        // Hier eine Zeile "zufällig" auswählen und hashen
        // dazu: Die Daten der Spalte befinden sich in this.nextData
        // Die zu füllende Hash-Tabelle ist in this.liveIndex
        // this.liveIndex ist vom Typ "LiveIndex extends Index"

        nextIndexCalled++;

        //hashtabelle bereits fertig vorhanden
        if(liveIndex.isReady() == true){
            Object data = null;
            int tab1_zeile = tupleIndices[priorTable];
            //System.out.println(tab1_zeile);

            //aktuelle daten finden
            switch (priorData.getClass().getSimpleName()) {
                case "IntData":
                    data = ((IntData) priorData).data[tab1_zeile];
                    break;
                case "DoubleData":
                    data = ((DoubleData) priorData).data[tab1_zeile];
                    break;
            }
            //daten in tab2 finden
            int ausgabe = liveIndex.getNextHashLine((T)data);
            return ausgabe;
        }

        // hashtabelle ist nicht fertig aufgebaut
        else{
            int n = liveIndex.getNextNotHashed();
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
            liveIndex.addHash(n, (T) data);

            return n;
        }

    }

    @Override
    public int nrIndexed(int[] tupleIndices) {
        return liveIndex.nrIndexed;
    }
}
