package indexing;

import config.JoinConfig;

public abstract class LiveIndex extends Index {
    /**
     * indexed lines
     */
    public int nrIndexed = 0;
    /**
     * state if index is fully build
     */
    protected boolean isReady = false;

    /**
     * current row when indexing
     */
    private int listIndex = 0;

    /**
     * Thread for solidify
     */
    protected Thread solidifyThread;

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
            if (JoinConfig.USE_RIPPLE_SOLIDIFY) {
                solidifyThread = new Thread(this::solidify);
                solidifyThread.start();
            }
            return cardinality;
        }
        int returnIndex = listIndex;
        listIndex++;
        return returnIndex;
    }

    abstract void solidify();

    /**
     * Ist Hash-Tabelle fertig gebaut
     *
     * @return Hash-Tabelle fertig
     */
    public boolean isReady() {
        return isReady;
    }
}
