package dboperations;

import BigT.BigT;
import BigT.Map;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.PinPageException;
import bufmgr.BufMgrException;
import diskmgr.PCounter;
import global.SystemDefs;
import heap.*;
import iterator.InvalidFieldSize;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Batchinsert {
    private String data_file_path;
    private BigT bigtable;
    PCounter pCounter = new PCounter();

    public Batchinsert(String DATAFILENAME, int TYPE, String BIGTABLENAME) throws ConstructPageException, HFDiskMgrException, HFException, GetFileEntryException, HFBufMgrException, PinPageException, IOException {
        this.data_file_path = DATAFILENAME;
        System.out.println("BigTable name = " + BIGTABLENAME);
        bigtable = new BigT(BIGTABLENAME, TYPE);

        pCounter.initialize();
    }

    public void run() throws IOException, SpaceNotAvailableException, InvalidMapSizeException, HFDiskMgrException, HFException, InvalidSlotNumberException, HFBufMgrException, BufMgrException, InvalidFieldSize {
        BufferedReader reader = new BufferedReader(new FileReader(data_file_path));
        String line;
        while((line = reader.readLine()) != null) {
            String[] mapAttributes = line.trim().split(",");

            // Create Map
            byte[] mapPtr = new byte[64];
            Map map = new Map(mapPtr, 0);
            map.setRowLabel(mapAttributes[0]);
            map.setColumnLabel(mapAttributes[1]);
            map.setTimeStamp(Integer.valueOf(mapAttributes[2]));
            map.setValue(mapAttributes[3]);
            System.out.println();
            map.print();

            // Insert Map into BigTable
            bigtable.insertMap(map.getMapByteArray());
        }
        reader.close();
        SystemDefs.JavabaseBM.softFlushAll();


        // print num of db pages read/written
        pCounter.print();
    }
}