package BigT;

import global.RID;
import heap.InvalidMapSizeException;
import heap.Scan;

import java.io.IOException;

public class Stream {
    Scan scan;
    String[] rowFilter, colFilter, valFilter;
    public Stream(BigT bigtable, int orderType, String ROWFILTER, String COLUMNFILTER, String VALUEFILTER) throws InvalidMapSizeException, IOException {
        scan = bigtable.openScan();
        rowFilter = processFilter(ROWFILTER);
        colFilter = processFilter(COLUMNFILTER);
        valFilter = processFilter(VALUEFILTER);
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
            Map currentMap = scan.getNext(mid);
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
        scan.closescan();
    }
}
