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
    NestedLoopJoinMap nestedLoopJoinMapStreamLeft = null;
    NestedLoopJoinMap nestedLoopJoinMapStreamRight = null;

    BigT lefT, rightT;
    String joinType;
    Stream left = null;
    Stream right = null;
    Stream nestedJoinOutputStream = null;

    AttrType[] in1 = {
            new AttrType(AttrType.attrString),
            new AttrType(AttrType.attrString),
            new AttrType(AttrType.attrString),
            new AttrType(AttrType.attrInteger)
    };
    CondExpr [] outFilter  = new CondExpr[3];

    FldSpec []  proj1 = {
            new FldSpec(new RelSpec(RelSpec.outer), 2),
            new FldSpec(new RelSpec(RelSpec.innerRel), 2)
    };

    public RowJoin(BigT lefT, BigT rightT, String outputTable, String columnFilter, String joinType, int numbuf) throws InvalidMapSizeException, IOException, InvalidFieldSize, ConstructPageException, HFDiskMgrException, HFException, GetFileEntryException, HFBufMgrException, PinPageException, SpaceNotAvailableException, InvalidSlotNumberException, NestedLoopException {
        this.lefT = lefT;
        this.rightT = rightT;
        this.joinType = joinType;
        if (joinType.equals("sortmerge")) {
            sortMergeJoinStream = new SortMergeJoin(lefT, rightT, columnFilter);
        } else {
            // Add nested join code here
            RetrieveRecentMaps r = new RetrieveRecentMaps();
            left = r.getRecentMaps(new Stream(lefT,6, "*", "*","*"));
            right = r.getRecentMaps(new Stream(rightT,6, "*", "*","*"));
            nestedLoopJoinMapStreamLeft = new NestedLoopJoinMap(in1, 4, new short[4], in1,4, new short[4], 10, left, lefT.getName()+".in",
                    outFilter, null, proj1, 2);
//            nestedLoopJoinMapStreamRight = new NestedLoopJoinMap(in1, 4, new short[4], in1,4, new short[4], 10, right, rightT.toString(),
//                    outFilter, null, proj1, 2);
            nestedJoinOutputStream = nestedLoopJoinMapStreamLeft.nestedRowJoin(left, right);

        }
    }

    public void run() throws Exception {
        while(true) {
            Map map = null;
            if (joinType == "sortmerge")
                map = sortMergeJoinStream.getNext();
            else if (joinType == "nested") {
                map = nestedJoinOutputStream.getNext(new RID());
            }
            if(map == null) break;
            map.print();
        }
        SystemDefs.JavabaseBM.softFlushAll();
    }
}
