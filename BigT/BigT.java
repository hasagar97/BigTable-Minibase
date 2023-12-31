package BigT;

import java.io.*;
import java.lang.*;
import java.util.*;

import global.*;
import btree.*;
import heap.*;
import bufmgr.*;
import iterator.*;
import BigT.*;


public class BigT extends Heapfile 
{
  private java.lang.String m_name;
  public int m_strategy;
  public Vector<Heapfile> m_heap_files = new Vector<Heapfile>();
  public BTreeFile m_defaultindex = null;
  public Vector<BTreeFile> m_index_files = new Vector<BTreeFile>();
  
  /* 
    Initializes the big table
  	name - name of the table
  	type - the clustering/index strategy to use
  */
  public BigT(java.lang.String name, int type) throws HFDiskMgrException, HFException, HFBufMgrException, IOException, ConstructPageException, GetFileEntryException, PinPageException {
    super(name);
    m_name = name;
    m_strategy = type;
    
    try
    {
      // Default Row+Column index across all heap files
      m_defaultindex = new BTreeFile(name + "_default_index", AttrType.attrString, 2*MAXINDEXNAME, 1);
      // Create storage types aligned in vectors so that index file index and heap file index in vectors are the same index.
      // Type 1 No index
      m_index_files.add(null);
      m_heap_files.add(new Heapfile(name + "_1"));
      // Type 2 Row index
      m_index_files.add(new BTreeFile(name + "_row_index", AttrType.attrString, MAXINDEXNAME, 1));
      m_heap_files.add(new Heapfile(name + "_2"));
      // Type 3 Column index
      m_index_files.add(new BTreeFile(name + "_column_index", AttrType.attrString, MAXINDEXNAME, 1));
      m_heap_files.add(new Heapfile(name + "_3"));
      // Type 4 Row+Column index
      m_index_files.add(new BTreeFile(name + "_row_col_index", AttrType.attrString, 2*MAXINDEXNAME, 1));
      m_heap_files.add(new Heapfile(name + "_4"));
      // Type 5 Row+Value index
      m_index_files.add(new BTreeFile(name + "_row_val_index", AttrType.attrString, 2*MAXINDEXNAME, 1));
      m_heap_files.add(new Heapfile(name + "_5"));
    }
    catch (Exception e)
    {
      System.err.println("Failed to create index files\n");
    }
  }
  
  /*
    Deletes the big table from the database
  */
    public void deleteBigt ()
    {
      // Destroy the index files
      if (m_defaultindex != null)
      {
        try {
          m_defaultindex.destroyFile();
        }
        catch (Exception e) {
          System.err.println("Failed to destroy default index\n");
        }
      }

      // Destroy the heapfile
      try {
          super.deleteFile();
      }
      catch (Exception e) {
          System.err.println("Failed to delete heapfile\n");
      }
    }

  /*
    Returns the number of maps in the big table
  */
    public int getMapCnt()
    {
      try {
          return super.getMapCnt();
      }
      catch (Exception e) {
          System.err.println("Failed to get Map count\n");
          return -1;
      }
    }

  /*
    Returns the number of distinct labels in the big table
    type 1 - gets row labels count
    type 2 - gets column labels count
    default - gets row labels count
  */
    private int getCnt(int type) throws InvalidMapSizeException, IOException
    {
      int count = 0;
      Stream stream = null;
      RID next_MID = new RID();
      Map next = null;
      java.lang.String label = null;
      java.lang.String next_label = null;

      switch (type) {
            case 1:
              // Want to stream entries by ordered row label
              stream = new Stream(this, 3, "*", "*", "*");
              break;
            case 2:
              // Want to stream entries by ordered column label
              stream = new Stream(this, 4, "*", "*", "*");
              break;
            default:
              // Want to stream entries by ordered row label
              stream = new Stream(this, 3, "*", "*", "*");
              break;
      }

      // Scan all the entries in our default index
      next = stream.getNext(next_MID);

      while(next != null)
      {
        switch (type) {
          case 1:
            try {
              next_label = next.getRowLabel();
            }
            catch (Exception e) {
              System.err.println("Failed to get row label from map\n");
            }
            break;
          case 2:
            try {
              next_label = next.getColumnLabel();
            }
            catch (Exception e) {
              System.err.println("Failed to get column label from map\n");
            }
            break;
          default:
            try {
              next_label = next.getRowLabel();
            }
            catch (Exception e) {
              System.err.println("Failed to get row label from map\n");
            }
            break;
        }

        if (label != null)
        {
          // Label changed. Count and update checked label.
          if (next_label.equals(label) == false)
          {
            count += 1;
            label = next_label;
          }
        }
        else
        {
          // First unique label
          count += 1;
          label = next_label;
        }

        next = stream.getNext(next_MID);
      }

      return count;
    }

  /*
    Returns the number of distinct row labels in the big table
  */
    public int getRowCnt() throws InvalidMapSizeException, IOException
    {
      return getCnt(1);
    }

  /*
    Returns the number of distinct column labels in the big table
  */
    public int getColumnCnt() throws InvalidMapSizeException, IOException
    {
      return getCnt(2);
    }

//  private void printBTreeKey() throws IteratorException, ConstructPageException, KeyNotMatchException, PinPageException, IOException, UnpinPageException, ScanIteratorException, HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException {
//    BTFileScan scan = m_defaultindex.new_scan(new StringKey("NetherlanJaguar"), new StringKey("NetherlanJaguar"));
//        while(true) {
//            KeyDataEntry ret = scan.get_next();
//            if(ret == null) {
//                System.out.println("NULL");
//                break;
//            }
//            System.out.println("In Scan");
//            System.out.println(ret.key);
//            System.out.println(ret.data);
//            LeafData leafData = (LeafData) ret.data;
//            RID id = leafData.getData();
//            try {
////                printRID(id);
////                Map out = super.getMap(id);
////                System.out.println(out);
////                System.out.println(out.getValue());
////                System.out.println(new String(out.getMapByteArray(), StandardCharsets.UTF_8));
//            } catch (Exception e) {
//                scan.DestroyBTreeFileScan();
//                throw new RuntimeException(e);
//            }
//        }
//        scan.DestroyBTreeFileScan();
//  }

    private void updateIndexFiles(Map map, RID mid, int operation) throws IteratorException, ConstructPageException, InsertRecException, ConvertException, InsertException, IndexInsertRecException, LeafDeleteException, NodeNotMatchException, LeafInsertRecException, PinPageException, IOException, UnpinPageException, FreePageException, IndexFullDeleteException, DeleteRecException, LeafRedistributeException, KeyTooLongException, RecordNotFoundException, DeleteFashionException, KeyNotMatchException, RedistributeException, IndexSearchException {
      updateIndexFiles(map, mid, operation, false, false, false, false);
    }

    // operation: 0 -> insert, 1 -> delete, 2 -> inserts only the affected indexes, 3 -> deletes only the affected indexes
    private void updateIndexFiles (Map map, RID mid, int operation,
                                   Boolean isRowAffected,
                                   Boolean isColAffected,
                                   Boolean isTimestampAffected,
                                   Boolean isValueAffected)
            throws IteratorException, ConstructPageException,
            ConvertException, InsertException, IndexInsertRecException,
            LeafDeleteException, NodeNotMatchException, LeafInsertRecException,
            PinPageException, IOException, UnpinPageException, DeleteRecException,
            KeyTooLongException, KeyNotMatchException, IndexSearchException,
            LeafRedistributeException, RecordNotFoundException, InsertRecException,
            DeleteFashionException, RedistributeException, FreePageException,
            IndexFullDeleteException {
      StringKey key;
      switch (m_strategy) {
      	case 1:
          // no index
          key = new StringKey(map.getRowLabel() + map.getColumnLabel());
          if(operation == 0) m_defaultindex.insert(key, mid);
          else if(operation == 1) m_defaultindex.Delete(key, mid);
          else {
            if(isRowAffected || isColAffected) {
              if(operation == 2) m_defaultindex.insert(key, mid);
              else if(operation == 3) m_defaultindex.Delete(key, mid);
            }
          }
          break;
        case 2:
          // one btree to index row labels
          key = new StringKey(map.getRowLabel() + map.getColumnLabel());
          if(operation == 0) m_defaultindex.insert(key, mid);
          else if(operation == 1) m_defaultindex.Delete(key, mid);
          else {
            if(isRowAffected || isColAffected) {
              if(operation == 2) m_defaultindex.insert(key, mid);
              else if(operation == 3) m_defaultindex.Delete(key, mid);
            }
          }
          break;
        case 3:
          // one btree to index column labels
          key = new StringKey(map.getRowLabel() + map.getColumnLabel());
          if(operation == 0) m_defaultindex.insert(key, mid);
          else if(operation == 1) m_defaultindex.Delete(key, mid);
          else {
            if(isColAffected || isColAffected) {
              if(operation == 2) m_defaultindex.insert(key, mid);
              else if(operation == 3) m_defaultindex.Delete(key, mid);
            }
          }
          break;
        case 4:
          // one btree to index column label and row label (combined key) and
          key = new StringKey(map.getRowLabel() + map.getColumnLabel());
          if(operation == 0) {
//            System.out.println("Inserting key1 into row_col_index = " + key1);
            m_defaultindex.insert(key, mid);
          } else if(operation == 1) {
//            System.out.println("Deleting MID from (key1)row_col_index = " + mid.pageNo.pid + " " + mid.slotNo);
            Boolean didDeleteFromRowCol = m_defaultindex.Delete(key, mid);
          } else {
            if(isRowAffected || isColAffected) {
              if(operation == 2) m_defaultindex.insert(key, mid);
              else if(operation == 3) m_defaultindex.Delete(key, mid);
            }
          }
          break;
        case 5:
          // one btree to index column label and value (combined key) and
          key = new StringKey(map.getRowLabel() + map.getColumnLabel());
          if(operation == 0) {
            m_defaultindex.insert(key, mid);
          } else if(operation == 1){
            m_defaultindex.Delete(key, mid);
          } else {
            if(isRowAffected || isColAffected) {
              if(operation == 2) m_defaultindex.insert(key, mid);
              else if(operation == 3) m_defaultindex.Delete(key, mid);
            }
          }
          break;
      }
    }

    private RID checkDropMap ( Map map, RID mid) throws IOException, HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException {
      StringKey key = null;
      BTFileScan scan = null;
      Map current_map = null;
      Map oldest_map = null;
      RID current_mid = null;
      RID oldest = null;
      int timestamp_count = 0;
      KeyDataEntry current_entry = null;
      
      key = new StringKey(map.getRowLabel() + map.getColumnLabel());
      
      try {
        scan = m_defaultindex.new_scan(key, key);
      }
      catch (Exception e) {
        e.printStackTrace();
        System.err.println("Failed to create new Scan in checkDropMap\n");
      }
      
      try {
      	current_entry = scan.get_next();
      }
      catch (Exception e) {
        System.err.println("Failed to get next entry in BTFileScan in checkDropMap\n");
      }

      if (current_entry != null)
      {
        current_mid = ((LeafData) current_entry.data).getData();
        try {
          current_map = super.getMap(current_mid);
        }
        catch (Exception e) {
          System.err.println("Failed to getMap from heapfile in checkDropMap\n");
        }
        
        if ((current_map.getRowLabel().equals(map.getRowLabel()) == true) && (current_map.getColumnLabel().equals(map.getColumnLabel()) == true))
        {
          System.err.println("SETTING OLDEST\n");
          timestamp_count += 1;
          oldest = current_mid;
          oldest_map = current_map;
        }

        while(current_entry != null)
        {
          try {
            current_entry = scan.get_next();
            if(current_entry == null) break;

            current_mid = ((LeafData) current_entry.data).getData();
            current_map = super.getMap(current_mid);

            if ((current_map.getRowLabel().equals(map.getRowLabel()) == true) && (current_map.getColumnLabel().equals(map.getColumnLabel()) == true))
            {
              timestamp_count += 1;
              
              if ((oldest_map == null) || (current_map.getTimeStamp() < oldest_map.getTimeStamp()))
              {
                oldest = current_mid;
              }
            }
          }
          catch (Exception e) {
            e.printStackTrace();
             System.err.println("Failed to process next Map in checkDropMap\n");
          }
        }
      }

      if (timestamp_count <= 3)
      {
        oldest = null;
      }

      if (scan != null)
      {
        scan.DestroyBTreeFileScan();
      }
      
      return oldest;
    }
  
  /*
    Insert a map into the big table and returns an MID.
    This method ensure that there are at most 3 maps with the same 
    row and column label but different timestamps in the big table.
    When a fourth map is inserted it drops the map with the oldest
    label from the big table
  */
    public RID insertMap ( byte[] mapPtr)
            throws InvalidSlotNumberException,
		  InvalidMapSizeException,
	   SpaceNotAvailableException,
	   HFException,
	   HFBufMgrException,
	   HFDiskMgrException,
	   IOException {
      RID mid = null;
      
      try{
        Map map = new Map(mapPtr, 0);

        // Change insertRecord in heapfile to insertMap
        mid = super.insertMap(mapPtr);

        // Index the Map
        updateIndexFiles(map, mid, 0);

//        // Check whether to drop the map
        RID oldestMapID = checkDropMap(map, mid);

        if(oldestMapID != null) {
          System.out.println("4th MID = " + oldestMapID.pageNo.pid + " " + oldestMapID.slotNo);
          Map oldestMap = super.getMap(oldestMapID);

          updateIndexFiles(oldestMap, oldestMapID, 1);


          // Change deleteRecord in heapfile to deleteMap
          Boolean didDelete = super.deleteMap(oldestMapID);
          System.out.println("Map deleted in heap = " + didDelete);
          //SystemDefs.JavabaseBM.softFlushAll();
        }
//        System.out.println("Printing BTree");
//        printBTreeKey();
//        System.out.println("Done!");
      }
      catch (Exception e) {
        e.printStackTrace();
        System.err.println("Failed to insert Map in BigT class\n");
      }

      return mid;
    }

    public boolean deleteMap(RID mid) throws IteratorException, ConstructPageException, InsertRecException, ConvertException, InsertException, IndexInsertRecException, LeafDeleteException, NodeNotMatchException, LeafInsertRecException, PinPageException, IOException, UnpinPageException, FreePageException, IndexFullDeleteException, DeleteRecException, LeafRedistributeException, KeyTooLongException, RecordNotFoundException, DeleteFashionException, KeyNotMatchException, RedistributeException, IndexSearchException {
      boolean deleteSuccess = false;
      
      try {
        deleteSuccess = super.deleteMap(mid);
        if(deleteSuccess) {
          Map map = super.getMap(mid); // getRecord from heapfile
          updateIndexFiles(map, mid, 1);
        }
      }
      catch (Exception e) {
        System.err.println("Failed to delete Map in BigT class\n");
      }
      
      return deleteSuccess;
    }

    public boolean updateMap(RID mid, Map newmap) throws IteratorException, ConstructPageException, InsertRecException, ConvertException, InsertException, IndexInsertRecException, LeafDeleteException, NodeNotMatchException, LeafInsertRecException, PinPageException, IOException, UnpinPageException, FreePageException, IndexFullDeleteException, DeleteRecException, LeafRedistributeException, KeyTooLongException, RecordNotFoundException, DeleteFashionException, KeyNotMatchException, RedistributeException, IndexSearchException {
      boolean updateSuccess = false;
      
      try {
        // get current map
        Map currentmap = super.getMap(mid);

        // update map (heapfile)
        updateSuccess = super.updateMap(mid, newmap);

        if(updateSuccess) {
          // find affected attributes
          Boolean isRowAffected = false;
          Boolean isColAffected = false;
          Boolean isTimestampAffected = false;
          Boolean isValueAffected = false;
        
          if (MapUtils.CompareMapWithMap(currentmap, newmap, 0) != 0)
          {
       	    isRowAffected = true;
          }
          if (MapUtils.CompareMapWithMap(currentmap, newmap, 1) != 0)
          {
       	    isColAffected = true;
          }
          if (MapUtils.CompareMapWithMap(currentmap, newmap, 2) != 0)
          {
       	    isTimestampAffected = true;
          }
          if (MapUtils.CompareMapWithMap(currentmap, newmap, 3) != 0)
          {
       	    isValueAffected = true;
          }

          // delete mid from those indexes
          updateIndexFiles(currentmap, mid, 3, isRowAffected, isColAffected, isTimestampAffected, isValueAffected);

          // insert mid into the indexes which involve the affected keys
          updateIndexFiles(newmap, mid, 2, isRowAffected, isColAffected, isTimestampAffected, isValueAffected);
        }
      }
      catch (Exception e) {
        System.err.println("Failed to update Map in BigT class\n");
      }

      return updateSuccess;
    }

  
  /*
    Initialize a stream of maps where row label matching rowFilter, 
    column label matching columnFilter, and value label matching 
    valueFilter. If any of the filter are null strings, then that 
    filter is not considered (e.g., if rowFilter is null, then all 
    row labels are OK). If orderType is
	1, then results are first ordered in row label, then column label, then time stamp
	2, then results are first ordered in column label, then row label, then time stamp
	3, then results are first ordered in row label, then time stamp
	4, then results are first ordered in column label, then time stamp
	6, then results are ordered in time stamp
  */
    public Stream openStream ( int orderType,
    java.lang.String rowFilter,
    java.lang.String columnFilter,
    java.lang.String valueFilter) throws InvalidMapSizeException, IOException
    {
      return new Stream(this, orderType, rowFilter, columnFilter, valueFilter);
    }

    public int getStrategy ()
    {
      return m_strategy;
    }
    
    public java.lang.String getName ()
    {
      return m_name;
    }
} // end of bigt





