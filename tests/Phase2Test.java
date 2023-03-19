package tests;

import BigT.BigT;
import BigT.Map;
import btree.*;
import bufmgr.BufMgrException;
import dboperations.Shell;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Phase2Test {
    static void printRID(RID rid) {
        System.out.println("Printing RID: " + rid);
        System.out.println("Slot No = " + rid.slotNo + " Page No = " + rid.pageNo.pid);
    }

    public static void main(String[] args) throws HFDiskMgrException, HFException, HFBufMgrException, IOException, SpaceNotAvailableException, InvalidSlotNumberException, InvalidMapSizeException, ConstructPageException, AddFileEntryException, GetFileEntryException, IteratorException, ConvertException, InsertException, IndexInsertRecException, LeafDeleteException, NodeNotMatchException, LeafInsertRecException, PinPageException, UnpinPageException, DeleteRecException, KeyTooLongException, KeyNotMatchException, IndexSearchException, ScanIteratorException, BufMgrException {

//        Shell shell = new Shell();
//        shell.run();

        String dbpath = "phase2.test.db";
        SystemDefs sysdef = new SystemDefs( dbpath, 0 ,5000,"Clock");

        Heapfile f = new Heapfile("test2db_3");

        BTreeFile bTreeFile = new BTreeFile("test2db_3_row_col_index", 0, 20, 1);
        BTFileScan scan = bTreeFile.new_scan(new StringKey("NetherlanJaguar"), new StringKey("NetherlanJaguar"));
        while(true) {
            KeyDataEntry ret = scan.get_next();
            if(ret == null) {
                System.out.println("NULL");
                break;
            }
            System.out.println("In Scan");
            System.out.println(ret.key);
            System.out.println(ret.data);
            LeafData leafData = (LeafData) ret.data;
            RID id = leafData.getData();
            try {
//                printRID(id);
                Map out = f.getMap(id);
//                System.out.println(out);
                System.out.println(out.getValue());
//                System.out.println(new String(out.getMapByteArray(), StandardCharsets.UTF_8));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
