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

    public RetrieveRecentMaps() throws ConstructPageException, HFDiskMgrException, HFException, GetFileEntryException, HFBufMgrException, PinPageException, IOException {
    }

    public Stream getRecentMaps(Stream stream) throws InvalidMapSizeException, IOException, InvalidFieldSize, SpaceNotAvailableException, HFDiskMgrException, HFException, InvalidSlotNumberException, HFBufMgrException {
        Map x = null;
        int recCount = 0;
        curr_map = null;
        while((x = stream.getNext(new RID()))!= null){
            if(curr_map== null) {
                curr_map = x;
            }
            else if(!curr_map.getColumnLabel().equals(x.getColumnLabel())){
                recentValueTable.insertMap(curr_map.returnMapByteArray(), 1);
                curr_map = x;
            }
            x.print();
            recCount++;
        }
        System.out.println("Total records in recentValueTable.in: "+ recCount);
        return new Stream(recentValueTable, 6, "*","*","*");
    }

}

