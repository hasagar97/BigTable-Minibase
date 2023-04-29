package iterator;

import BigT.BigT;
import BigT.Map;
import BigT.Stream;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.PinPageException;
import global.RID;
import heap.*;

import java.io.IOException;

public class RetrieveRecentMaps {

    Map curr_map;
    Map next_ptr;

    BigT recentValueTable = new BigT("recentValueTable.in");

    public RetrieveRecentMaps(String tableName) throws ConstructPageException, HFDiskMgrException, HFException, GetFileEntryException, HFBufMgrException, PinPageException, IOException {
        recentValueTable = new BigT(tableName);
        recentValueTable.deleteBigt();
        recentValueTable = new BigT(tableName);
    }
    public BigT getBigT() {
        return recentValueTable;
    }
    public Stream getRecentMaps(Stream stream,String TableName) throws InvalidMapSizeException, IOException, InvalidFieldSize, SpaceNotAvailableException, HFDiskMgrException, HFException, InvalidSlotNumberException, HFBufMgrException, ConstructPageException, GetFileEntryException, PinPageException {
        recentValueTable = new BigT(TableName);
        Map x = null;
        int recCount = 0;
        Map curr_map = null;
        while((x = stream.getNext(new RID()))!= null){
            if(curr_map== null) {
                Map curr_map1 = new Map();
                curr_map1.setRowLabel(x.getRowLabel());
                curr_map1.setColumnLabel(x.getColumnLabel());
                curr_map1.setTimeStamp(x.getTimeStamp());
                curr_map1.setValue(x.getValue());
                curr_map = curr_map1;
            }
            if(!curr_map.getColumnLabel().equalsIgnoreCase(x.getColumnLabel())){
                recentValueTable.insertMap(curr_map.returnMapByteArray(), 1);
            }
            curr_map.setRowLabel(x.getRowLabel());
            curr_map.setColumnLabel(x.getColumnLabel());
            curr_map.setTimeStamp(x.getTimeStamp());
            curr_map.setValue(x.getValue());
//            x.print();
            recCount++;
        }
        System.out.println("Total records in recentValueTable.in: "+ recCount);
        //order type 2 - based on columnlabel
//        Stream res = new Stream(recentValueTable, 2, "*","*","*");
//        Map m = null;
//        while((m = res.getNext(new RID()))!=null){
//            System.out.println("Resultant big table records: "+ m.getColumnLabel()+ " #TS: "+ m.getTimeStamp());
//        }
        return new Stream(recentValueTable, 6, "*","*","*", null);
    }

}

