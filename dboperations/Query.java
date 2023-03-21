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
    Stream stream;
    PCounter pCounter = new PCounter();

    public Query(BigT bigtable, int TYPE, int ORDERTYPE, String ROWFILTER, String COLUMNFILTER, String VALUEFILTER, int NUMBUF) throws ConstructPageException, HFDiskMgrException, HFException, GetFileEntryException, HFBufMgrException, PinPageException, IOException, InvalidMapSizeException {

        // check if this is safe
//        SystemDefs.JavabaseBM = new BufMgr(NUMBUF, "Clock");

        stream = new Stream(bigtable, ORDERTYPE, ROWFILTER, COLUMNFILTER, VALUEFILTER);
//        stream = bigtable.openStream(ORDERTYPE, ROWFILTER, COLUMNFILTER, VALUEFILTER);

        pCounter.initialize();
    }

    public void run() throws IOException, InvalidMapSizeException {
        RID id = new RID();
        while(true) {
            Map map = stream.getNext(id);
            if(map != null) {
                System.out.println();
                map.print(); // do we print the whole map or just the value ?
            }
            else break;
        }
        stream.close();

        // print num of db pages read/written
        pCounter.print();
    }
}
