package dboperations;

import BigT.BigT;
import BigT.Map;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.PinPageException;
import bufmgr.BufMgr;
import diskmgr.PCounter;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;

import java.io.IOException;

public class Query {
    BigT bigtable;
//    Stream stream;
    PCounter pCounter = new PCounter();

    public Query(String BIGTABLENAME, int TYPE, int ORDERTYPE, String ROWFILTER, String COLUMNFILTER, String VALUEFILTER, int NUMBUF) throws ConstructPageException, HFDiskMgrException, HFException, GetFileEntryException, HFBufMgrException, PinPageException, IOException {

        // check if this is safe
        SystemDefs.JavabaseBM = new BufMgr(NUMBUF, "Clock");

        bigtable = new BigT(BIGTABLENAME, TYPE);
//        stream = bigtable.openStream(ORDERTYPE, ROWFILTER, COLUMNFILTER, VALUEFILTER);

        pCounter.initialize();
    }

    public void run() {
        while(true) {
//            Map map = stream.getNext();
//            if(map) map.print(); // do we print the whole map or just the value ?
//            else break;
        }
//        stream.close();

        // print num of db pages read/written
//        pCounter.print();
    }
}
