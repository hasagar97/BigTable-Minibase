package dboperations;

import BigT.BigT;
import BigT.Map;
import BigT.Stream;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.PinPageException;
import bufmgr.BufMgr;
import diskmgr.PCounter;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidMapSizeException;

import java.io.IOException;

public class Query {
    BigT bigtable;
    Stream stream;
    PCounter pCounter = new PCounter();

    public Query(String BIGTABLENAME, int TYPE, int ORDERTYPE, String ROWFILTER, String COLUMNFILTER, String VALUEFILTER, int NUMBUF) throws ConstructPageException, HFDiskMgrException, HFException, GetFileEntryException, HFBufMgrException, PinPageException, IOException, InvalidMapSizeException {

        // check if this is safe
        SystemDefs.JavabaseBM = new BufMgr(NUMBUF, "Clock");

        bigtable = new BigT(BIGTABLENAME, TYPE);
        stream = new Stream(bigtable, 1, ROWFILTER, COLUMNFILTER, VALUEFILTER);
//        stream = bigtable.openStream(ORDERTYPE, ROWFILTER, COLUMNFILTER, VALUEFILTER);

        pCounter.initialize();
    }

    public void run() throws IOException, InvalidMapSizeException {
        while(true) {
            Map map = stream.getNext(new RID(new PageId(5), 0));
            if(map != null) map.print(); // do we print the whole map or just the value ?
            else break;
        }
        stream.close();

        // print num of db pages read/written
        pCounter.print();
    }
}
