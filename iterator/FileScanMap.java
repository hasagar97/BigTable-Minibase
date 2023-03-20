package iterator;


import BigT.BigT;
import BigT.Map;
import global.RID;
import heap.Scan;


public class FileScanMap extends Iterator {
    Scan scan;
    BigT bigtable;
    public FileScanMap(BigT bigtable) {
        try {
            this.bigtable = bigtable;
            scan = this.bigtable.openScan();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
    @Override
    public Map get_next() {
        Map currentMap = null;
        try {
        RID rid = new RID();
        currentMap = this.scan.getNext(rid);
            
        } catch (Exception e) {
            System.out.println(e);
        }
        return currentMap;
    }

    @Override
    public void close()  {
        try {
            this.scan.closescan();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
    
}
