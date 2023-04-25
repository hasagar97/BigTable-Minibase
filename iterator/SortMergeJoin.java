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
import iterator.FileScanMap;
import iterator.Sort;
import java.io.IOException;
import BigT.BigT;
import BigT.Map;
import BigT.Stream;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.PinPageException;

public class SortMergeJoin {
    Stream leftStream = null;
    Stream rightStream = null;
    Stream leftResultStream = null;
    Stream rightResultStream = null;

    String joinMode = "inner";

    public SortMergeJoin(BigT leftTable, BigT rightTable, String columnFilter, String outputTable) throws InvalidMapSizeException, IOException, InvalidFieldSize, HFDiskMgrException, HFException, HFBufMgrException, ConstructPageException, GetFileEntryException, PinPageException, InvalidSlotNumberException, SpaceNotAvailableException {
        // Start stream with bigtable2
        leftStream = new Stream(leftTable, 1, "*", columnFilter, "*", null);

        BigT result = new BigT(outputTable);
        System.out.println("Stream created");

        // Create temp buffer with bigtable2 entries which are same
        if (joinMode == "inner") {
            // columns equal and value equal then print
            Map leftMap = leftStream.getNext(null);

            while (leftMap != null) {
                String valueFilter = "["+leftMap.getValue()+","+leftMap.getValue()+"]";
                rightStream = new Stream(rightTable, 1, "*", "*", valueFilter, null);
                Map rightMap = rightStream.getNext(null);
                
                while (rightMap != null) {
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
                    // Move pointers forward
                    rightMap = rightStream.getNext(null);
                }
                leftMap = leftStream.getNext(null);
            }            

        }
        // If bigtable1 next entries don't match then flush buffer

        // Print out the result
    }

    public Map getNext() {
        return null;
    }

}
