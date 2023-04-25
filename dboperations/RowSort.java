package dboperations;

import BigT.BigT;
import BigT.Stream;
import BigT.Map;
import bufmgr.BufMgr;
import bufmgr.BufMgrException;
import global.RID;
import global.SystemDefs;
import heap.*;

import java.io.IOException;

public class RowSort {
    Stream stream = null;
    BigT in, out;
    public RowSort(BigT in, BigT out, String columnName, int n_pages) throws InvalidMapSizeException, IOException {
        this.in = in;
        this.out = out;

//        SystemDefs.JavabaseBM = new BufMgr(n_pages, "Clock");

        stream = new Stream(in, 1, "*", "*", "*", columnName);
    }

    public void run() throws InvalidMapSizeException, IOException, SpaceNotAvailableException, HFDiskMgrException, HFException, InvalidSlotNumberException, HFBufMgrException, BufMgrException {
        while(true) {
            Map map = stream.getNext(new RID());
            if(map == null) break;

            out.insertMap(map.getMapByteArray(), 1);
        }
        SystemDefs.JavabaseBM.softFlushAll();
    }
}
