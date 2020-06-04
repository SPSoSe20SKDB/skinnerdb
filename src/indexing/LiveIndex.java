package indexing;

public abstract class LiveIndex extends Index {
    /**
     * indexed lines
     */
    public int nrIndexed = 0;
    /**
     * state if index is fully build
     */
    private boolean isReady = false;

    /**
     * current row when indexing
     */
    private int listIndex = 0;

    /**
     * Initialize for given cardinality of indexed table.
     *
     * @param cardinality number of rows to index
     */
    public LiveIndex(int cardinality) {
        super(cardinality);
    }

    /**
     * Nächsten Tabelleninces (Iterator-mäßig) zurückgeben
     *
     * @return Zeilennummer innerhalb der Spalte
     */
    public int getNextNotHashed() {
        if (listIndex == cardinality) {
            listIndex = 0;
            isReady = true;
            return cardinality;
        }
        int returnIndex = listIndex;
        listIndex++;
        return returnIndex;
    }

    /**
     * Ist Hash-Tabelle fertig gebaut
     *
     * @return Hash-Tabelle fertig
     */
    public boolean isReady() {
        return isReady;
    }
}
