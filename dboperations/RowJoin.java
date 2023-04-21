package dboperations;

import BigT.BigT;
import iterator.*;
import BigT.Map;
import BigT.Stream;
import bufmgr.BufMgrException;
import global.RID;
import global.SystemDefs;
import heap.*;

import java.io.IOException;

public class RowJoin {
    SortMergeJoin sortMergeJoinStream = null;
    SortMergeJoin nestedJoinStream = null;
    BigT lefT, rightT;
    String joinType;

    public RowJoin(BigT lefT, BigT rightT, String outputTable, String columnFilter, String joinType, int numbuf) throws InvalidMapSizeException, IOException {
        this.lefT = lefT;
        this.rightT = rightT;
        this.joinType = joinType;
        if (joinType == "sortmerge") {
            sortMergeJoinStream = new SortMergeJoin(lefT, rightT, columnFilter);
        }
    }

    public void run() throws InvalidMapSizeException, IOException, SpaceNotAvailableException, HFDiskMgrException, HFException, InvalidSlotNumberException, HFBufMgrException, BufMgrException {
        while(true) {
            Map map = null;
            if (joinType == "sortmerge")
                map = sortMergeJoinStream.getNext();
            if (joinType == "nested")
                map = sortMergeJoinStream.getNext();
            if(map == null) break;
            map.print();
        }
        SystemDefs.JavabaseBM.softFlushAll();
    }
}
