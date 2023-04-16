package iterator;


import BigT.BigT;
import BigT.Map;
import BigT.BigTScan;
import btree.*;
import global.PageId;
import global.RID;
import heap.Scan;


public class FileScanMap extends Iterator {
    BigTScan scan = null;
    BTFileScan btScan = null;
    BigT bigtable;

    public FileScanMap(BigT bigtable, String[] rowFilter, String[] colFilter, String[] valFilter) {
        try {
            this.bigtable = bigtable;
            String lo, hi;

            if (rowFilter != null && colFilter != null) {
            	lo = rowFilter[0] + colFilter[0];
                hi = rowFilter[1] + colFilter[1];
                System.out.println("Filters = " + lo + " " + hi);
                btScan = this.bigtable.m_defaultindex.new_scan(new StringKey(lo), new StringKey(hi));
            }
            
            if (btScan == null)
            {
                scan = bigtable.openScan();
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    private Map keyDataEntryToMap(KeyDataEntry entry) {
        LeafData leafData = (LeafData) entry.data;
        RID id = leafData.getData();
        Map out;
        try {
            out = this.bigtable.getMap(id);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return out;
    }

    @Override
    public Map get_next() throws ScanIteratorException {
        if (btScan == null) {
            Map currentMap = null;
            try {
                RID rid = new RID();
                currentMap = this.scan.getNext(rid);
            } catch (Exception e) {
                System.out.println(e);
            }
            return currentMap;
        } else {
            KeyDataEntry entry = null;
            try {
                entry = this.btScan.get_next();
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (entry == null) return null; // should be redundant
            return keyDataEntryToMap(entry);
        }
    }

    @Override
    public void close() {
        try {
            this.scan.closescan();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

}
