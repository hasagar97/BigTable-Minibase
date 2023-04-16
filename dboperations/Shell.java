package dboperations;

import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.PinPageException;
import bufmgr.*;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import global.SystemDefs;
import heap.*;
import iterator.InvalidFieldSize;
import BigT.BigT;

import java.io.IOException;
import java.util.Scanner;

public class Shell {
    private static String dbpath = "phase2.test.db";
    private static int num_pages = 5000;
    private static int bufferpoolsize = 100;

    public static void run() throws BufMgrException, IOException, SpaceNotAvailableException, InvalidMapSizeException, HFDiskMgrException, HFException, InvalidSlotNumberException, HFBufMgrException, ConstructPageException, GetFileEntryException, PinPageException, InvalidFieldSize, PageNotFoundException, HashOperationException, PagePinnedException, PageUnpinnedException {
        new SystemDefs(dbpath, num_pages, bufferpoolsize, "Clock"); // creates a new db if num_pages > 0

        Scanner input = new Scanner(System.in);
        BigT bigtable = null;
        Boolean running = true;
        while(running) {
            System.out.println();
            System.out.println("Enter Command: (BatchInsert) / (Query) / (exit)");
            String command = input.nextLine();
            String[] words = command.trim().split(" ");

            // The ROW/COL/VALUE FILTERS, if provided as ranges, must have no spaces between "[" and "]"
            // eg:- [R1,R2] works, but [R1, R2] fails as the input command string is split on " "
            String DATAFILENAME, BIGTABLENAME, ROWFILTER, COLUMNFILTER, VALUEFILTER;
            int TYPE, ORDERTYPE, NUMBUF;
            switch(words[0].toLowerCase()) {
                case "batchinsert":
                    DATAFILENAME = words[1];
                    BIGTABLENAME = words[3];
                    TYPE = Integer.valueOf(words[2]);

                    BIGTABLENAME = BIGTABLENAME + "_" + String.valueOf(TYPE);
                    bigtable = new BigT(BIGTABLENAME);
                    Batchinsert batchinsert = new Batchinsert(DATAFILENAME, TYPE, bigtable);
                    batchinsert.run();

                    break;

                case "query":
                    BIGTABLENAME = words[1];
                    TYPE = Integer.valueOf(words[2]);
                    ORDERTYPE = Integer.valueOf(words[3]);
                    ROWFILTER = words[4];
                    COLUMNFILTER = words[5];
                    VALUEFILTER = words[6];
                    NUMBUF = Integer.valueOf(words[7]);

                    BIGTABLENAME = BIGTABLENAME + "_" + String.valueOf(TYPE);
                    if(bigtable == null) bigtable = new BigT(BIGTABLENAME);
                    Query query = new Query(bigtable, TYPE, ORDERTYPE, ROWFILTER, COLUMNFILTER, VALUEFILTER, NUMBUF);
                    query.run();

                    break;

                case "exit":
                    running = false;
                    break;

                default:
                    running = false;
                    break;
            }
        }

        // close the DB
        SystemDefs.JavabaseDB.closeDB();
    }

    public static void main(String[] args) throws IOException, BufMgrException, InvalidPageNumberException, FileIOException, DiskMgrException, ConstructPageException, HFDiskMgrException, HFException, GetFileEntryException, HFBufMgrException, PinPageException, SpaceNotAvailableException, InvalidMapSizeException, InvalidSlotNumberException, InvalidFieldSize, PageNotFoundException, HashOperationException, PagePinnedException, PageUnpinnedException {
        run();
    }
}
