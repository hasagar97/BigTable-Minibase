package iterator;
import global.AttrType;
import global.RID;
import global.TupleOrder;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidMapSizeException;
import heap.InvalidSlotNumberException;
import heap.Scan;
import heap.SpaceNotAvailableException;
import iterator.*;
import java.io.IOException;
import BigT.BigT;
import BigT.BigTScan;
import BigT.Map;
import BigT.Stream;
import btree.*;

public class SortMergeJoin {
    BigTScan leftStream = null;
    Stream tempStream = null;
    Stream leftResultStream = null;
    Stream rightResultStream = null;

    String joinMode = "inner";
    Stream resultStream = null;
    Integer counter = 0;
    public SortMergeJoin(BigT leftTable, BigT rightTable, String columnFilter, String outputTable) throws Exception {
        // Start stream with bigtable2
        RetrieveRecentMaps r1 = new RetrieveRecentMaps("LeftSortMergeJoinUniqueTable" + counter.toString());
        r1.getRecentMaps(leftTable, "LeftSortMergeJoinUniqueTable"  + counter.toString());
        leftTable = new BigT("LeftSortMergeJoinUniqueTable"  + counter.toString());
        leftStream = leftTable.openScan();

        RetrieveRecentMaps r2 = new RetrieveRecentMaps("RightSortMergeJoinUniqueTable"  + counter.toString());
        r2.getRecentMaps(rightTable, "RightSortMergeJoinUniqueTable"  + counter.toString());
        rightTable = new BigT("RightSortMergeJoinUniqueTable"  + counter.toString());
        
        BigT result = new BigT(outputTable);
        System.out.println("Stream created");

        // Create temp buffer with bigtable2 entries which are same
        if (joinMode == "inner") {
            // columns equal and value equal then print
            Map leftMap = leftStream.getNext(new RID());

            while (leftMap != null) {
                BTFileScan rightStream = rightTable.m_valueIndex.new_scan(new StringKey(leftMap.getValue()), new StringKey(leftMap.getValue()));
                while(true) {
                    KeyDataEntry entry = rightStream.get_next();
                    if(entry == null) break;

                    if(entry.data instanceof LeafData) {
                        RID mid = ((LeafData)entry.data).getData();

                        Map rightMap = rightTable.m_heap_files.get(mid.heapIndex).getMap(mid);
                        // If the join attribute values match, join the two tuples and output the result
                        Map leftJoinResult = new Map();
                        Map rightJoinResult = new Map();
                        // Check if column label is the same if it is then use some other condition
                        int columnCompare = leftMap.getColumnLabel().compareTo(rightMap.getColumnLabel());
                        leftJoinResult.setRowLabel(leftMap.getRowLabel() + ":" + rightMap.getRowLabel());
                        leftJoinResult.setValue(leftMap.getValue());
                        leftJoinResult.setTimeStamp(leftMap.getTimeStamp());

                        rightJoinResult.setRowLabel(leftMap.getRowLabel() + ":" + rightMap.getRowLabel());
                        rightJoinResult.setValue(rightMap.getValue());
                        rightJoinResult.setTimeStamp(rightMap.getTimeStamp());
                        
                        if (columnCompare == 0) {
                            leftJoinResult.setColumnLabel(leftMap.getColumnLabel() + "_left");
                            rightJoinResult.setColumnLabel(leftMap.getColumnLabel() + "_right");
                        } else {
                            leftJoinResult.setColumnLabel(leftMap.getColumnLabel());
                            rightJoinResult.setColumnLabel(rightMap.getColumnLabel());
                        }
                        leftJoinResult.print();
                        rightJoinResult.print();
                        System.out.println("----------------");
                        
                        result.insertMap(leftJoinResult.getMapByteArray(), 1);
                        result.insertMap(rightJoinResult.getMapByteArray(), 1);
                    }
                }
                leftMap = leftStream.getNext(new RID());
            }            
        }
        // If bigtable1 next entries don't match then flush buffer

        // Print out the result
        resultStream = new Stream(result, 3, "*", "*", "*", null);
    }

    public Map getNext() throws InvalidMapSizeException, IOException {
        return resultStream.getNext(null);
    }

}
