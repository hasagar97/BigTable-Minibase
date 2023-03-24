package iterator;


import BigT.BigT;
import BigT.Map;
import btree.*;
import global.PageId;
import global.RID;
import heap.Scan;


public class FileScanMap extends Iterator {
    Scan scan = null;
    BTFileScan btScan = null;
    BigT bigtable;
    public FileScanMap(BigT bigtable, String[] rowFilter, String[] colFilter, String[] valFilter) {
        try {
            this.bigtable = bigtable;
            String lo, hi;
            if(rowFilter != null && colFilter != null) {
                lo = rowFilter[0] + colFilter[0];
                hi = rowFilter[1] + colFilter[1];
                System.out.println("Filters = " + lo + " " + hi);
                btScan = this.bigtable.m_defaultindex.new_scan(new StringKey(lo), new StringKey(hi));
            } else {
                switch(this.bigtable.m_strategy) {
                    case 1:
                        if(rowFilter != null) {
                            lo = new String(rowFilter[0]);
                            hi = new String(rowFilter[1]);
                            System.out.println("Filters = " + lo + " " + hi);
                            btScan = this.bigtable.m_indexfile1.new_scan(new StringKey(lo), new StringKey(hi));
                        }
                        break;

                    case 2:
                        if(colFilter != null) {
                            lo = new String(colFilter[0]);
                            hi = new String(colFilter[1]);
                            System.out.println("Filters = " + lo + " " + hi);
                            btScan = this.bigtable.m_indexfile1.new_scan(new StringKey(lo), new StringKey(hi));
                        }
                        break;

                    case 3:
                        break;

                    case 4:
                        if(rowFilter != null && valFilter != null) {
                            lo = new String(rowFilter[0] + valFilter[0]);
                            hi = new String(rowFilter[1] + valFilter[1]);
                            System.out.println("Filters = " + lo + " " + hi);
                            btScan = this.bigtable.m_indexfile1.new_scan(new StringKey(lo), new StringKey(hi));
                        }
                        break;

                    case 5:
                        if(colFilter != null && valFilter != null) {
                            lo = new String(colFilter[0] + valFilter[0]);
                            hi = new String(colFilter[1] + valFilter[1]);
                            System.out.println("Filters = " + lo + " " + hi);
                            btScan = this.bigtable.m_indexfile1.new_scan(new StringKey(lo), new StringKey(hi));
                        }
                }

                if(btScan == null) {
                    btScan = this.bigtable.m_defaultindex.new_scan(null, null);
//                    scan = this.bigtable.openScan();
                }
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
        if(btScan == null) {
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
            if(entry == null) return null; // should be redundant
            return keyDataEntryToMap(entry);
        }
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
