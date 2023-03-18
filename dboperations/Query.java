package dboperations;

import bufmgr.BufMgr;
import diskmgr.PCounter;
import global.SystemDefs;

public class Query {
    BigT bigtable;
    Stream stream;
    PCounter pCounter = new PCounter();

    public Query(String BIGTABLENAME, int TYPE, int ORDERTYPE, String ROWFILTER, String COLUMNFILTER, String VALUEFILTER, int NUMBUF) {

        // check if this is safe
        SystemDefs.JavabaseBM = new BufMgr(NUMBUF, "Clock");

        bigtable = new BigT(BIGTABLENAME, TYPE);
        stream = bigtable.openStream(ORDERTYPE, ROWFILTER, COLUMNFILTER, VALUEFILTER);

        pCounter.initialize();
    }

    public void run() {
        while(true) {
            Map map = stream.getNext();
            if(map) map.print(); // do we print the whole map or just the value ?
            else break;
        }
        stream.close();

        // print num of db pages read/written
        pCounter.print();
    }
}
