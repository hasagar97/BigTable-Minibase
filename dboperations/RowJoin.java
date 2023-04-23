package dboperations;

import BigT.BigT;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.PinPageException;
import global.AttrType;
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
    NestedLoopJoinMap nestedLoopJoinMapStream = null;

    BigT lefT, rightT;
    String joinType;

    AttrType[] Stypes2 = {
            new AttrType(AttrType.attrString),
            new AttrType(AttrType.attrString),
            new AttrType(AttrType.attrString),
            new AttrType(AttrType.attrInteger)
    };

    short []   Ssizes = new short[4];

    public RowJoin(BigT lefT, BigT rightT, String outputTable, String columnFilter, String joinType, int numbuf) throws InvalidMapSizeException, IOException, InvalidFieldSize, ConstructPageException, HFDiskMgrException, HFException, GetFileEntryException, HFBufMgrException, PinPageException, SpaceNotAvailableException, InvalidSlotNumberException {
        this.lefT = lefT;
        this.rightT = rightT;
        this.joinType = joinType;
        if (joinType.equals("sortmerge")) {
            sortMergeJoinStream = new SortMergeJoin(lefT, rightT, columnFilter);
        } else {
            // Add nested join code here
            RetrieveRecentMaps r = new RetrieveRecentMaps();
            Stream left = r.getRecentMaps(new Stream(lefT,6, "*", "*","*"));
            Stream right = r.getRecentMaps(new Stream(rightT,6, "*", "*","*"));
            Stream output = nestedLoopJoinMapStream.nestedRowJoin(left, right);
        }
    }

    public void run() throws Exception {
        while(true) {
            Map map = null;
            if (joinType == "sortmerge")
                map = sortMergeJoinStream.getNext();
            else if (joinType == "nested")
                map = nestedLoopJoinMapStream.get_next();
            if(map == null) break;
            map.print();
        }
        SystemDefs.JavabaseBM.softFlushAll();
    }
}
