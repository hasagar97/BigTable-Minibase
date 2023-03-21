package tests;

import BigT.Map;
import btree.*;
import global.RID;
import global.SystemDefs;
import heap.*;
import iterator.InvalidFieldSize;

import java.io.IOException;
import java.nio.charset.StandardCharsets;


public class Phase2Basic {
    public static void main(String[] args) throws HFDiskMgrException, HFException, HFBufMgrException, IOException, SpaceNotAvailableException, InvalidSlotNumberException, InvalidMapSizeException, ConstructPageException, AddFileEntryException, GetFileEntryException, IteratorException, ConvertException, InsertException, IndexInsertRecException, LeafDeleteException, NodeNotMatchException, LeafInsertRecException, PinPageException, UnpinPageException, DeleteRecException, KeyTooLongException, KeyNotMatchException, IndexSearchException, ScanIteratorException, InvalidFieldSize, IOException, InvalidFieldSize {
        Map mp = new Map();
        mp.setRowLabel("Test String");
//        System.out.println(mp.getRowLabel());
        mp.print();
        mp.setColumnLabel("cols string");
        // System.out.println(mp.getColumnLabel());
//        mp.setTimeStamp(55);
//        System.out.println(mp.getTimeStamp());
//        mp.setValue("value string");
//        System.out.println(mp.getValue());
        mp.print();
    }
}
