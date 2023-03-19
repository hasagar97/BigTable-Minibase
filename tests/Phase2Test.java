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

    private static void update_test(BigT bigtable) throws Exception {
        Map map = new Map();
        map.setRowLabel("Netherlan");
        map.setColumnLabel("Jaguar");
        map.setTimeStamp(5);
        map.setValue("501");

        RID rid = new RID(new PageId(5), 6);

        bigtable.updateMap(rid, map);
    }

    public static void main(String[] args) throws Exception {

        //Shell shell = new Shell();
        //shell.run();

        String dbpath = "phase2.test.db";
        SystemDefs sysdef = new SystemDefs( dbpath, 0 ,5000,"Clock");

        
        BigT bigtable = new BigT("test2db_3", 3);
	bigtable.deleteBigt();
        update_test(bigtable);
//
        System.out.println("Printing BTree Index");
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
            System.out.println("Entry " + id);
//                printRID(id);
                //Map out = f.getMap(id);
                //out.print();
                //System.out.println(out.getValue());
//                System.out.println(new String(out.getMapByteArray(), StandardCharsets.UTF_8));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /*System.out.println("Printing HeapFile");
        Scan heapScan = f.openScan();
        RID id = new RID(new PageId(5), 0);
        while (true) {
            Map temp = heapScan.getNext(id);
            if(temp == null) break;
            temp.print();
        }
        heapScan.closescan();*/
    }
}
