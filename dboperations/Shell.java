package dboperations;

import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.PinPageException;
import bufmgr.*;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import global.RID;
import global.SystemDefs;
import heap.*;
import iterator.*;
import BigT.BigT;
import BigT.Map;
import BigT.BigTScan;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Scanner;

public class Shell {
    private static String dbpath = "phase2.test.db";
    private static int num_pages = 5000;
    private static int bufferpoolsize = 500;

    public static void run() throws Exception, BufMgrException, IOException, SpaceNotAvailableException, InvalidMapSizeException, HFDiskMgrException, HFException, InvalidSlotNumberException, HFBufMgrException, ConstructPageException, GetFileEntryException, PinPageException, InvalidFieldSize, PageNotFoundException, HashOperationException, PagePinnedException, PageUnpinnedException, InvalidRelation, FileScanException, TupleUtilsException, PageNotReadException, UnknowAttrType, FieldNumberOutOfBoundException, PredEvalException, WrongPermat, JoinsException, InvalidTypeException {
        new SystemDefs(dbpath, num_pages, bufferpoolsize, "Clock"); // creates a new db if num_pages > 0

        Scanner input = new Scanner(System.in);
        BigT bigtable = null;
        Boolean running = true;
        while(running) {
            System.out.println();
            System.out.println("Enter Command: (BatchInsert) / (Query) / (RowSort) / (MapInsert) / (CreateIndex) / (GetCounts) / (exit)");
            String command = input.nextLine();
            String[] words = command.trim().split(" ");

            // The ROW/COL/VALUE FILTERS, if provided as ranges, must have no spaces between "[" and "]"
            // eg:- [R1,R2] works, but [R1, R2] fails as the input command string is split on " "
            String DATAFILENAME, BIGTABLENAME, ROWFILTER, COLUMNFILTER, VALUEFILTER;
            String INTABLENAME, OUTTABLENAME, COLUMNNAME;
            int TYPE, ORDERTYPE, NUMBUF;
            switch(words[0].toLowerCase()) {
                case "batchinsert":
                    DATAFILENAME = words[1];
                    BIGTABLENAME = words[3];
                    TYPE = Integer.valueOf(words[2]);
                    NUMBUF = Integer.valueOf(words[4]);

                    SystemDefs.JavabaseBM = new BufMgr(NUMBUF, "Clock");

                    bigtable = new BigT(BIGTABLENAME);
                    Batchinsert batchinsert = new Batchinsert(DATAFILENAME, TYPE, bigtable);
                    batchinsert.run();

                    break;

                case "query":
                    BIGTABLENAME = words[1];
                    ORDERTYPE = Integer.valueOf(words[2]);
                    ROWFILTER = words[3];
                    COLUMNFILTER = words[4];
                    VALUEFILTER = words[5];
                    NUMBUF = Integer.valueOf(words[6]);

                    bigtable = new BigT(BIGTABLENAME);
                    Query query = new Query(bigtable, ORDERTYPE, ROWFILTER, COLUMNFILTER, VALUEFILTER, NUMBUF);
                    query.run();

                    break;

                case "rowsort":
                    INTABLENAME = words[1];
                    OUTTABLENAME = words[2];
                    COLUMNNAME = words[3];
                    NUMBUF = Integer.valueOf(words[4]);

                    BigT in = new BigT(INTABLENAME);
                    BigT out = new BigT(OUTTABLENAME);
                    RowSort rowSort = new RowSort(in, out, COLUMNNAME, NUMBUF);
                    rowSort.run();

                    System.out.println("Printing the contents of the Out table for debugging");
                    BigTScan scan = out.openScan();
                    while(true) {
                        Map map = scan.getNext(new RID());
                        if(map == null) break;
                        map.print();
                    }
                    break;

                case "rowjoin":
                    String LEFTTABLE = words[1];
                    String RIGHTTABLE = words[2];
                    String OUTPUTTABLE = words[3];
                    COLUMNFILTER = words[4];
                    String JOINTYPE = words[5];
                    NUMBUF = Integer.valueOf(words[6]);

                    BigT lefT = new BigT(LEFTTABLE);
                    BigT righT = new BigT(RIGHTTABLE);
                    System.out.println("Initializing rowjoin with jointype "+ JOINTYPE);
                    Instant start = Instant.now();
                    RowJoin rowJoin = new RowJoin(lefT, righT, OUTPUTTABLE, COLUMNFILTER, JOINTYPE, NUMBUF);
                    rowJoin.run();
                    Instant finish = Instant.now();
                    Integer timeElapsed = Math.round(Duration.between(start, finish).toMillis());
                    System.out.println("Total Run time " + Integer.toString(timeElapsed) + " ms");
                    break;

                case "mapinsert":
                    String row = words[1];
                    String col = words[2];
                    String val = words[3];
                    int timestamp = Integer.valueOf(words[4]);
                    int type = Integer.valueOf(words[5]);
                    BIGTABLENAME = words[6];
                    NUMBUF = Integer.valueOf(words[7]);

                    SystemDefs.JavabaseBM = new BufMgr(NUMBUF, "Clock");

                    bigtable = new BigT(BIGTABLENAME);
                    Map newMap = new Map();
                    newMap.setRowLabel(row);
                    newMap.setColumnLabel(col);
                    newMap.setTimeStamp(timestamp);
                    newMap.setValue(val);

                    bigtable.insertMap(newMap.getMapByteArray(), type);

                    break;

                case "createindex":
                    BIGTABLENAME = words[1];
                    int heapFileType = Integer.valueOf(words[2]);
                    int newIndexType = Integer.valueOf(words[3]);

                    bigtable = new BigT(BIGTABLENAME);
                    bigtable.createIndex(heapFileType, newIndexType);

                    break;

                case "getcounts":
                    BIGTABLENAME = words[1];
                    NUMBUF = Integer.valueOf(words[2]);

                    SystemDefs.JavabaseBM = new BufMgr(NUMBUF, "Clock");

                    bigtable = new BigT(BIGTABLENAME);

                    System.out.println("Number of Maps = " + bigtable.getMapCnt());
                    System.out.println("Distinct Row Labels = " + bigtable.getRowCnt());
                    System.out.println("Distinct Column Labels = " + bigtable.getColumnCnt());

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

    public static void main(String[] args) throws Exception {
        run();
    }
}
