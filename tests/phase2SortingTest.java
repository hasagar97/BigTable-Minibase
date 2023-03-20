package tests;

import BigT.*;
import global.RID;
import global.SystemDefs;
import heap.*;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class phase2SortingTest {
    static void printRID(RID rid) {
        System.out.println("Printing RID: " + rid);
        System.out.println("Slot No = " + rid.slotNo + " Page No = " + rid.pageNo.pid);
    }

    public static void main(String[] args) throws HFDiskMgrException, HFException, HFBufMgrException, IOException, SpaceNotAvailableException, InvalidSlotNumberException {
        String dbpath = "phase2.test3.db";
        SystemDefs sysdef = new SystemDefs( dbpath, 5000 ,5000,"Clock");
        Heapfile f = new Heapfile("first_table5");
        byte[] data = "3".getBytes();
        byte[] data1 = "2".getBytes();
        byte[] data2 = "4".getBytes();
        byte[] data3 = "3".getBytes();
        byte[] data4 = "4".getBytes();
        byte[] data5 = "5".getBytes();
        byte[] data6 = "6".getBytes();
        try {
            f.insertMap(data);
            f.insertMap(data1);
            f.insertMap(data2);
            f.insertMap(data3);
            f.insertMap(data4);
            f.insertMap(data5);
            f.insertMap(data6);
        } catch (Exception e) {
            System.err.println(e);
        }
        

//        Stream s = new Stream((BigT) f, 1, "[0,3]", "*", "*");
//
//        while (true) {
//            try {
//                Map item = s.getNext();
//                if(item == null) break;
//                System.out.println("In Scan");
//                item.print();
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//        }
    }
}
