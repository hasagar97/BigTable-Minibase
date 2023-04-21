package iterator;

import BigT.Map;
import BigT.Stream;
import global.RID;
import heap.InvalidMapSizeException;

import java.io.IOException;

public class RetrieveRecentMaps {

    int curr_ptr;
    int temp_ptr;
    public RetrieveRecentMaps(){
        curr_ptr = 0;
        temp_ptr = curr_ptr+1;
    }

    Map recentMap = new Map();
    public Map getRecentRecord(Stream stream) throws InvalidMapSizeException, IOException {
        while(stream.getNext(new RID())!= null){
            stream.getNext(new RID());
        }
        return recentMap;
    }

    public static void main(String args[]){
        RetrieveRecentMaps r = new RetrieveRecentMaps();
    }
}

