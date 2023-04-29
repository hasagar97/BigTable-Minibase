package dboperations;

import BigT.BigT;
import BigT.BigTScan;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.PinPageException;
import global.AttrOperator;
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
    BigTScan nestedJoinOutputStream = null;
    Integer counter = 1;

    AttrType[] in1 = {
            new AttrType(AttrType.attrString),
            new AttrType(AttrType.attrString),
            new AttrType(AttrType.attrString),
            new AttrType(AttrType.attrInteger)
    };
    CondExpr [] outFilter  = new CondExpr[3];
    CondExpr [] outFilter2 = new CondExpr[3];

    FldSpec []  proj1 = {
            new FldSpec(new RelSpec(RelSpec.outer), 2),
            new FldSpec(new RelSpec(RelSpec.innerRel), 2)
    };

    short[] sizes = new short[4];


    public RowJoin(BigT lefT, BigT rightT, String outputTable, String columnFilter, String joinType, int numbuf) throws Exception {
        this.lefT = lefT;
        this.rightT = rightT;
        this.joinType = joinType;
        sizes[0] = 30;
        sizes[1] = 30;
        sizes[2] = 30;
        sizes[3] = 30;
        outFilter[0] = new CondExpr();
        outFilter[1] = new CondExpr();
        outFilter[2] = new CondExpr();
        outFilter2[0] = new CondExpr();
        outFilter2[1] = new CondExpr();
        outFilter2[2] = new CondExpr();
        Query7_CondExpr(outFilter, outFilter2);
        if (joinType.equals("sortmerge")) {
            sortMergeJoinStream = new SortMergeJoin(lefT, rightT, columnFilter, outputTable);
        } else {
            // Add nested join code here
            counter++;
            RetrieveRecentMaps r = new RetrieveRecentMaps(lefT.getName()+"_ll_"+counter.toString());
            BigT uniqueLeftT = new BigT(lefT.getName()+"_ll_"+counter.toString());
            BigT uniqueRightT = new BigT(rightT.getName()+"_ll_"+counter.toString());

            r.getRecentMaps(lefT,lefT.getName()+"_ll_"+counter.toString());
            r.getRecentMaps(rightT,rightT.getName()+"_ll_"+counter.toString());
            nestedLoopJoinMapStreamRight = new NestedLoopJoinMap(in1, 4, sizes, in1,4, sizes, 20, right, rightT.getName()+".in",
                    outFilter, null, proj1, 2);

            
            BigTScan lt = uniqueLeftT.openScan();
            BigTScan rt = uniqueRightT.openScan();
            nestedJoinOutputStream = nestedLoopJoinMapStreamRight.nestedRowJoin(lt, rt, outputTable);

            
            // nestedJoinOutputStream = nestedLoopJoinMapStreamRight.nestedRowJoinCross(lt,rt,outputTable);

            Map op = new Map();
//            while((op =  nestedJoinOutputStream.getNext(new RID()))!=null){
//                System.out.println("Resultant output Join records:: Row:"+op.getRowLabel() +" Col:"+ op.getColumnLabel()+ " #TS: "+ op.getTimeStamp()+ " val: "+ op.getValue());
//            }
        }
    }

    private void Query7_CondExpr(CondExpr[] expr, CondExpr[] expr2) {

        expr[0].next  = null;
        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),2);

        expr[1].next  = null;
        expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrSymbol);
        expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),4);
        expr[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),4);
        expr[2] = null;
        expr2[0] = null;
        expr2[1] = null;
        expr2[2] = null;
    }

    public void run() throws Exception {
        while(true) {
            Map map = null;
            if (joinType == "sortmerge")
                map = sortMergeJoinStream.getNext();
            else if (joinType == "nested") {
                // System.out.println("requexsting next joined map ");
                map = nestedJoinOutputStream.getNext(new RID());
            }
            if(map == null) break;
            map.print();
        }
        SystemDefs.JavabaseBM.softFlushAll();
    }
}
