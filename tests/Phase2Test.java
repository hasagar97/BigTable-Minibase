package tests;

import BigT.Map;
import btree.*;
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

    public static void main(String[] args) throws HFDiskMgrException, HFException, HFBufMgrException, IOException, SpaceNotAvailableException, InvalidSlotNumberException, InvalidMapSizeException, ConstructPageException, AddFileEntryException, GetFileEntryException, IteratorException, ConvertException, InsertException, IndexInsertRecException, LeafDeleteException, NodeNotMatchException, LeafInsertRecException, PinPageException, UnpinPageException, DeleteRecException, KeyTooLongException, KeyNotMatchException, IndexSearchException, ScanIteratorException {
        Map mp = new Map();
        mp.setRowLabel("Test String");
        System.out.println(mp.getRowLabel());
        mp.setColumnLabel("cols steinfg");
        System.out.println(mp.getColumnLabel());
        mp.setTimeStamp(55);
        System.out.println(mp.getTimeStamp());
        mp.setValue("value tsint");
        System.out.println(mp.getValue());
//        String dbpath = "phase2.test3.db";
//        SystemDefs sysdef = new SystemDefs( dbpath, 5000 ,5000,"Clock");
//        Heapfile f = new Heapfile("first_table3");
//        byte[] data = "Hello World!".getBytes();
//        byte[] data1 = "Hello World 1!".getBytes();
//        byte[] data2 = "Hello World 2!".getBytes();
//        byte[] data3 = "Hello World 3!".getBytes();
//        byte[] data4 = "Hello World 4!".getBytes();
//        byte[] data5 = "Hello World 5!".getBytes();
//        byte[] data6 = "Hello World 6!".getBytes();
//        RID rid = f.insertRecord(data);
//        RID rid1 = f.insertRecord(data1);
//        RID rid2 = f.insertRecord(data2);
//        RID rid3 = f.insertRecord(data3);
//        RID rid4 = f.insertRecord(data4);
//        RID rid5 = f.insertRecord(data5);
//        RID rid6 = f.insertRecord(data6);
//        printRID(rid1);
//        printRID(rid2);
//        printRID(rid3);
//        try {
//            BigT.Map out2 = f.getRecord(rid);
//            System.out.println(new String(out2.getMapByteArray(), StandardCharsets.UTF_8));
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//
//        BTreeFile bTreeFile = new BTreeFile("btree_file1", 0, 10, 0);
//        bTreeFile.insert(new StringKey("key"), rid);
//        bTreeFile.insert(new StringKey("key"), rid1);
//        bTreeFile.insert(new StringKey("key2"), rid2);
//        bTreeFile.insert(new StringKey("key3"), rid3);
//        bTreeFile.insert(new StringKey("key4"), rid4);
//        bTreeFile.insert(new StringKey("key5"), rid5);
//        bTreeFile.insert(new StringKey("key6"), rid6);
//        BTFileScan scan = bTreeFile.new_scan(new StringKey("key"), new StringKey("key"));
//        while(true) {
//            KeyDataEntry ret = scan.get_next();
//            if(ret == null) break;
//            System.out.println("In Scan");
//            System.out.println(ret.key);
//            System.out.println(ret.data);
//            LeafData leafData = (LeafData) ret.data;
//            RID id = leafData.getData();
//            try {
//                printRID(id);
//                BigT.Map out = f.getRecord(id);
//                System.out.println(out);
//                System.out.println(new String(out.getMapByteArray(), StandardCharsets.UTF_8));
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//        }
    }
}
