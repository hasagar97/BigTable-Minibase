package tests;

import BigT.*;
import global.RID;
import global.SystemDefs;
import heap.*;

import java.io.IOException;

public class phase2SortingTest {
    static void printRID(RID rid) {
        System.out.println("Printing RID: " + rid);
        System.out.println("Slot No = " + rid.slotNo + " Page No = " + rid.pageNo.pid);
    }

    public static void main(String[] args) throws HFDiskMgrException, HFException, HFBufMgrException, IOException, SpaceNotAvailableException, InvalidSlotNumberException, InvalidTupleSizeException {
        String dbpath = "phase2.test3.db";
        SystemDefs sysdef = new SystemDefs( dbpath, 5000 ,5000,"Clock");
        Heapfile f = new Heapfile("first_table5");
        byte[] data = "Hello World!".getBytes();
        byte[] data1 = "Hello World 1!".getBytes();
        byte[] data2 = "Hello World 2!".getBytes();
        byte[] data3 = "Hello World 3!".getBytes();
        byte[] data4 = "Hello World 4!".getBytes();
        byte[] data5 = "Hello World 5!".getBytes();
        byte[] data6 = "Hello World 6!".getBytes();

        f.insertRecord(data);
        f.insertRecord(data1);
        f.insertRecord(data2);
        f.insertRecord(data3);
        f.insertRecord(data4);
        f.insertRecord(data5);
        f.insertRecord(data6);

        BigT.Stream s = new BigT.Stream("first_table5", 1, "[0-5]", "*", "*");

        while (true) {
            try {
                BigT.Map item = s.getNext();
                if(item == null) break;
                System.out.println("In Scan");
                System.out.println(item);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
