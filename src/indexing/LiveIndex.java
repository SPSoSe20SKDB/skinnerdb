package indexing;

public class LiveIndex extends Index {

    public int nrIndexed;

    /**
     * Initialize for given cardinality of indexed table.
     *
     * @param cardinality number of rows to index
     */
    public LiveIndex(int cardinality) {
        super(cardinality);
        nrIndexed = 0;

        // TODO: Struktur für Hash-Tabelle ausdenken + setter und getter
    }

    public void set(int Referenz){

    }

    public int get(){
        return -1;
    }

}
