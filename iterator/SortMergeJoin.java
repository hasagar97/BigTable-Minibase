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

    public SortMergeJoin(BigT leftTable, BigT rightTable, String columnFilter) throws InvalidMapSizeException, IOException {
        // Start stream with bigtable2
        leftStream = new Stream(leftTable, 2, "*", columnFilter, "*");
        rightStream = new Stream(leftTable, 2, "*", columnFilter, "*");
        System.out.println("Stream created");

        // Create temp buffer with bigtable2 entries which are same
        if (joinMode == "inner") {
            // columns equal and value equal then print
            Map leftMap = leftStream.getNext(null);
            while (leftMap != null) {
                System.out.println("HERE1"); 
                Map rightMap = rightStream.getNext(null);
                if (rightMap == null) {
                    // just process leftmap
                }
                while (leftMap.getColumnLabel() != rightMap.getColumnLabel()){
                    rightMap = rightStream.getNext(null);
                }
                leftMap.print();
                rightMap.print();
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
