package BigT;

import global.AttrType;
import global.RID;
import global.TupleOrder;
import heap.InvalidMapSizeException;
import heap.Scan;
import iterator.FileScanMap;
import iterator.Sort;
import java.io.IOException;

public class Stream {
    String[] rowFilter, colFilter, valFilter;
    Sort sortedStream;

    public Stream(BigT bigtable, int orderType, String ROWFILTER, String COLUMNFILTER, String VALUEFILTER) throws InvalidMapSizeException, IOException {
        rowFilter = processFilter(ROWFILTER);
        colFilter = processFilter(COLUMNFILTER);
        valFilter = processFilter(VALUEFILTER);
        try {
          FileScanMap iterator = new FileScanMap(bigtable, rowFilter, colFilter, valFilter);
          AttrType[] attrTypes = new AttrType[]{new AttrType(0), new AttrType(0), new AttrType(1), new AttrType(0)};
          short len_in = 4;
          short [] str_sizes = {(short)20, (short)20, (short)4, (short)20};
          this.sortedStream = new Sort(attrTypes, len_in, str_sizes, iterator, 4, new TupleOrder(TupleOrder.Ascending), 20, 10, orderType);
        } catch (Exception e) {
            e.printStackTrace();
          System.out.println(e);
        }

    }

    private static String[] processFilter(String filter) {
        String[] ret = new String[2];
        if(filter.charAt(0) == '[' && filter.charAt(filter.length()-1) == ']') {
            String temp = filter.substring(1, filter.length()-1);
            ret = temp.trim().split(",");
        } else if(filter.charAt(0) == '*'){
            ret = null;
        } else {
            ret[0] = filter;
            ret[1] = filter;
        }
        return ret;
    }

    public Map getNext(RID mid) throws InvalidMapSizeException, IOException {
        while(true) {
          Map currentMap = null;
          try {
            currentMap = this.sortedStream.get_next();            
          } catch (Exception e) {
              e.printStackTrace();
            System.out.println(e);
          }
            if(currentMap == null) return null;

            if(rowFilter != null) {
                String rowLabel = currentMap.getRowLabel();
                if(!(rowFilter[0].compareTo(rowLabel) <= 0 && rowFilter[1].compareTo(rowLabel) >= 0)) {
                    continue;
                }
            }
            if(colFilter != null) {
                String colLabel = currentMap.getColumnLabel();
                if(!(colFilter[0].compareTo(colLabel) <= 0 && colFilter[1].compareTo(colLabel) >= 0)) {
                    continue;
                }
            }
            if(valFilter != null) {
                String val = currentMap.getValue();
                if(!(valFilter[0].compareTo(val) <= 0 && valFilter[1].compareTo(val) >= 0)) {
                    continue;
                }
            }
            return currentMap;
        }
    }

//    public static void main(String[] args) {
//        String filter = "[Main1, Main2]";
//        String[] ret = processFilter(filter);
//        if(ret == null) System.out.println(ret);
//        else System.out.println(ret[0] + " " + ret[1]);
//        int temp = "123".compareTo("456");
//        System.out.println(temp);
//    }

    public void close() {
      try {
        this.sortedStream.close();
      } catch (Exception e) {
        System.out.println(e);
      }
    }
}
