package tests;

import BigT.BigT;
import BigT.Map;
import BigT.Stream;
import btree.*;
import bufmgr.*;
import dboperations.Query;
import dboperations.Shell;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.*;
import iterator.InvalidFieldSize;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Phase2Test {
    static void printRID(RID rid) {
        System.out.println("Printing RID: " + rid);
        System.out.println("Slot No = " + rid.slotNo + " Page No = " + rid.pageNo.pid);
    }

    public static void main(String[] args) throws HFDiskMgrException, HFException, HFBufMgrException, IOException, SpaceNotAvailableException, InvalidSlotNumberException, InvalidMapSizeException, ConstructPageException, AddFileEntryException, GetFileEntryException, IteratorException, ConvertException, InsertException, IndexInsertRecException, LeafDeleteException, NodeNotMatchException, LeafInsertRecException, PinPageException, UnpinPageException, DeleteRecException, KeyTooLongException, KeyNotMatchException, IndexSearchException, ScanIteratorException, BufMgrException, InvalidFieldSize, PageNotFoundException, HashOperationException, PagePinnedException, PageUnpinnedException {
        try {
            Shell shell = new Shell();
            shell.run();
        } catch (Exception e) {
            System.out.print(e);
        }

//        String dbpath = "phase2.test.db";
//        SystemDefs sysdef = new SystemDefs( dbpath, 5000 ,5000,"Clock");
////
//        BigT f = new BigT("test2db_3", 3);
////////        Heapfile f1 = new Heapfile("test2db_3");
//        Map map = new Map();
//        map.setRowLabel("mango");
//        map.setColumnLabel("mango");
//        map.setTimeStamp(1);
//        map.setValue("101");
//        Map map2 = new Map();
//        map2.setRowLabel("zebra");
//        map2.setColumnLabel("zebra");
//        map2.setTimeStamp(2);
//        map2.setValue("201");
//        Map map3 = new Map();
//        map3.setRowLabel("apple");
//        map3.setColumnLabel("aple");
//        map3.setTimeStamp(3);
//        map3.setValue("203");
//        RID mid = f.insertMap(map2.getMapByteArray());
//        RID mid3 = f.insertMap(map3.getMapByteArray());
//        RID mid2 = f.insertMap(map.getMapByteArray());
//        SystemDefs.JavabaseBM.softFlushAll();
//
//        Query query = new Query(f, 3, 1, "*", "*", "*", 200);
//        query.run();




//        BTreeFile bTreeFile = new BTreeFile("test2db_3_row_col_index", 0, 20, 1);
//        BTFileScan scan = bTreeFile.new_scan(new StringKey("NetherlanJaguar"), new StringKey("NetherlanJaguar"));
//        while(true) {
//            KeyDataEntry ret = scan.get_next();
//            if(ret == null) {
//                System.out.println("NULL");
//                break;
//            }
//            System.out.println("In Scan");
//            System.out.println(ret.key);
//            System.out.println(ret.data);
//            LeafData leafData = (LeafData) ret.data;
//            RID id = leafData.getData();
//            try {
////                printRID(id);
//                Map out = f.getMap(id);
////                System.out.println(out);
//                System.out.println(out.getValue());
////                System.out.println(new String(out.getMapByteArray(), StandardCharsets.UTF_8));
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//        }
    }
}
