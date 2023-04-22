package iterator;
import global.AttrType;
import global.RID;
import global.TupleOrder;
import heap.InvalidMapSizeException;
import heap.Scan;
import iterator.FileScanMap;
import iterator.Sort;
import java.io.IOException;
import BigT.BigT;
import BigT.Map;
import BigT.Stream;

public class SortMergeJoin {
    Stream leftStream = null;
    Stream rightStream = null;
    String joinMode = "inner";

    public SortMergeJoin(BigT leftTable, BigT rightTable, String columnFilter) throws InvalidMapSizeException, IOException, InvalidFieldSize {
        // Start stream with bigtable2
        leftStream = new Stream(leftTable, 2, "*", columnFilter, "*");
        rightStream = new Stream(leftTable, 2, "*", columnFilter, "*");
        System.out.println("Stream created");

        // Create temp buffer with bigtable2 entries which are same
        if (joinMode == "inner") {
            // columns equal and value equal then print
            Map leftMap = leftStream.getNext(null);
            Map rightMap = rightStream.getNext(null);

            while (leftMap != null && rightMap != null) {
                int compareResult = leftMap.getColumnLabel().compareTo(rightMap.getColumnLabel());
            
                if (compareResult == 0) {
                    // If the join attribute values match, join the two tuples and output the result
                    Map joinResult = new Map();
                    joinResult.setColumnLabel(leftMap.getColumnLabel());
                    joinResult.setRowLabel(leftMap.getRowLabel() + ":" + rightMap.getRowLabel());
                    if (leftMap.getTimeStamp() < rightMap.getTimeStamp()) {
                        joinResult.setTimeStamp(leftMap.getTimeStamp());
                    } else {
                        joinResult.setTimeStamp(rightMap.getTimeStamp());
                    }
                    joinResult.setValue(leftMap.getValue());
                    joinResult.print();
                    // Move both pointers forward
                    leftMap = leftStream.getNext(null);
                    rightMap = rightStream.getNext(null);
                } else if (compareResult < 0) {
                    // If the join attribute value in the left relation is smaller, move the left pointer forward
                    leftMap = leftStream.getNext(null);
                } else {
                    // If the join attribute value in the right relation is smaller, move the right pointer forward
                    rightMap = rightStream.getNext(null);
                }
            }            
            
        }
        // If bigtable1 next entries don't match then flush buffer

        // Print out the result
    }

    public Map getNext() {
        return null;
    }

}
