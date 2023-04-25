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


public class BigT 
{
  private java.lang.String m_name;
  public Vector<Heapfile> m_heap_files = new Vector<Heapfile>();
  public BTreeFile m_defaultindex = null;
  public static final int MAXINDEXNAME = 40;
  public HashMap<Integer, HashMap<Integer, BTreeFile>> m_index_files = new HashMap<Integer, HashMap<Integer, BTreeFile>>();

  private String getIndexName(int heapFileType, int indexType) {
    String name = getHeapfileName(heapFileType);
    switch (indexType) {
      case 1:
        name += "_no_index";
        break;

      case 2:
        name += "row_index";
        break;

      case 3:
        name += "col_index";
        break;

      case 4:
        name += "col_row_index";
        break;

      case 5:
        name += "row_val_index";
        break;

      default:
        break;
    }

    return name;
  }

  private String getHeapfileName(int heapFileType) {
    return m_name + "_" + String.valueOf(heapFileType);
  }

  /*
    Initializes the big table
  	name - name of the table
  	type - the clustering/index strategy to use
  */
  public BigT(java.lang.String name) throws HFDiskMgrException, HFException, HFBufMgrException, IOException, ConstructPageException, GetFileEntryException, PinPageException {
    m_name = name;
    Heapfile heapfile = null;

    // BigT already exists
//    if(m_defaultindex != null) return;

    try
    {
      // initialize m_index_files hashmap
      m_index_files.put(0, new HashMap<>());
      m_index_files.put(1, new HashMap<>());
      m_index_files.put(2, new HashMap<>());
      m_index_files.put(3, new HashMap<>());
      m_index_files.put(4, new HashMap<>());


      // Default Row+Column index across all heap files
      m_defaultindex = new BTreeFile(name + "_default_index", AttrType.attrString, 2*MAXINDEXNAME, DeleteFashion.NAIVE_DELETE);
      // Create storage types aligned in vectors so that index file index and heap file index in vectors are the same index.

      // Type 1 No index
      m_index_files.get(0).put(0, null);
      heapfile = new Heapfile(name + "_1");
      heapfile.setIndex(0);
      m_heap_files.add(heapfile);

      // Type 2 Row index
      m_index_files.get(1).put(1, new BTreeFile(getIndexName(2, 2), AttrType.attrString, MAXINDEXNAME, DeleteFashion.NAIVE_DELETE));
      heapfile = new Heapfile(getHeapfileName(2));
      heapfile.setIndex(1);
      m_heap_files.add(heapfile);

      // Type 3 Column index
      m_index_files.get(2).put(2, new BTreeFile(getIndexName(3, 3), AttrType.attrString, MAXINDEXNAME, DeleteFashion.NAIVE_DELETE));
      heapfile = new Heapfile(getHeapfileName(3));
      heapfile.setIndex(2);
      m_heap_files.add(heapfile);

      // Type 4 Column+Row index
      m_index_files.get(3).put(3, new BTreeFile(getIndexName(4, 4), AttrType.attrString, 2*MAXINDEXNAME, DeleteFashion.NAIVE_DELETE));
      heapfile = new Heapfile(getHeapfileName(4));
      heapfile.setIndex(3);
      m_heap_files.add(heapfile);

      // Type 5 Row+Value index
      m_index_files.get(4).put(4, new BTreeFile(getIndexName(5, 5), AttrType.attrString, 2*MAXINDEXNAME, DeleteFashion.NAIVE_DELETE));
      heapfile = new Heapfile(getHeapfileName(5));
      heapfile.setIndex(4);
      m_heap_files.add(heapfile);
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
      // Destroy the index and heap files
      if (m_defaultindex != null)
      {
        try {
          m_defaultindex.destroyFile();
        }
        catch (Exception e) {
          System.err.println("Failed to destroy default index\n");
        }
      }

      for(int heapIndex = 0; heapIndex < 5; heapIndex++) {
        for (int i = 0; i < m_index_files.get(heapIndex).size(); i++)
        {
          if (m_index_files.get(heapIndex).get(i) != null)
          {
            try {
              m_index_files.get(heapIndex).get(i).destroyFile();
            }
            catch (Exception e) {
              System.err.println("Failed to destroy index file\n");
            }
          }
          if (m_heap_files.get(heapIndex) != null)
          {
            try {
              m_heap_files.get(heapIndex).deleteFile();
            }
            catch (Exception e) {
              System.err.println("Failed to destroy heap file\n");
            }
          }
        }
      }


    }

  /*
    Returns the number of maps in the big table
  */
    public int getMapCnt()
    {
      try {
        int totalCount = 0;
        
        for (Integer i = 0; i < m_heap_files.size(); i++)
        {
          totalCount += m_heap_files.get(i).getMapCnt();
        }
        
        return totalCount;
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
              stream = new Stream(this, 3, "*", "*", "*", null);
              break;
            case 2:
              // Want to stream entries by ordered column label
              stream = new Stream(this, 4, "*", "*", "*", null);
              break;
            default:
              // Want to stream entries by ordered row label
              stream = new Stream(this, 3, "*", "*", "*", null);
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

    public void createIndex(int heapfileType, int indexType) throws Exception {
      if(indexType == 1) {
        System.out.println("No Index created (TYPE 1)");
        return;
      }

      if(m_index_files.get(heapfileType-1).get(indexType-1) != null) {
        System.out.println("Index already created on the heapfile");
        return;
      }

      // create new "indexType" index on heapfile "heapFileType"
      String newIndexName = getIndexName(heapfileType, indexType);
      m_index_files.get(heapfileType-1).put(indexType-1, new BTreeFile(newIndexName, AttrType.attrString, MAXINDEXNAME, DeleteFashion.NAIVE_DELETE));

      BTFileScan btFileScan = m_defaultindex.new_scan(null, null);
      while(true) {
        KeyDataEntry entry = btFileScan.get_next();
        if(entry == null) break;

        if(entry.data instanceof LeafData) {

          RID mid = ((LeafData)entry.data).getData();
          KeyClass key = entry.key;

          if(mid.heapIndex == heapfileType) {
            m_index_files.get(heapfileType-1).get(indexType-1).insert(new StringKey(key.toString()), mid);
          }

        }
      }
      System.out.println("Index created");
    }

    // type -> heapIndex
    private void indexInsertAll(RID mid, int type, StringKey[] keys) throws IOException, IteratorException, ConstructPageException, ConvertException, InsertException, IndexInsertRecException, LeafDeleteException, NodeNotMatchException, LeafInsertRecException, PinPageException, UnpinPageException, DeleteRecException, KeyTooLongException, KeyNotMatchException, IndexSearchException {
      for(int i = 1; i < 5; i++) {
        if(m_index_files.get(type - 1).get(i) != null) {
          m_index_files.get(type - 1).get(i).insert(keys[i], mid);
        }
      }
    }

    private void indexDeleteAll(RID mid, int type, StringKey[] keys) throws IOException, IteratorException, ConstructPageException, IndexInsertRecException, LeafDeleteException, PinPageException, UnpinPageException, DeleteRecException, KeyNotMatchException, IndexSearchException, LeafRedistributeException, RecordNotFoundException, InsertRecException, DeleteFashionException, RedistributeException, FreePageException, IndexFullDeleteException {
      for(int i = 1; i < 5; i++) {
        if(m_index_files.get(type - 1).get(i) != null) {
          m_index_files.get(type - 1).get(i).Delete(keys[i], mid);
        }
      }
    }

    private void indexInsertAffected(RID mid, int type, StringKey[] keys, boolean[] affectedIndexes) throws IteratorException, ConstructPageException, ConvertException, InsertException, IndexInsertRecException, LeafDeleteException, NodeNotMatchException, LeafInsertRecException, PinPageException, IOException, UnpinPageException, DeleteRecException, KeyTooLongException, KeyNotMatchException, IndexSearchException {
      for(int i = 1; i < 5; i++) {
        if(m_index_files.get(type - 1).get(i) != null && affectedIndexes[i] == true) {
          m_index_files.get(type - 1).get(i).insert(keys[i], mid);
        }
      }
    }

    private void indexDeleteAffected(RID mid, int type, StringKey[] keys, boolean[] affectedIndexes) throws IteratorException, ConstructPageException, ConvertException, InsertException, IndexInsertRecException, LeafDeleteException, NodeNotMatchException, LeafInsertRecException, PinPageException, IOException, UnpinPageException, DeleteRecException, KeyTooLongException, KeyNotMatchException, IndexSearchException, LeafRedistributeException, RecordNotFoundException, InsertRecException, DeleteFashionException, RedistributeException, FreePageException, IndexFullDeleteException {
      for(int i = 1; i < 5; i++) {
        if(m_index_files.get(type - 1).get(i) != null && affectedIndexes[i] == true) {
          m_index_files.get(type - 1).get(i).Delete(keys[i], mid);
        }
      }
    }

    private void updateIndexFiles(Map map, RID mid, int operation, int type) throws IteratorException, ConstructPageException, InsertRecException, ConvertException, InsertException, IndexInsertRecException, LeafDeleteException, NodeNotMatchException, LeafInsertRecException, PinPageException, IOException, UnpinPageException, FreePageException, IndexFullDeleteException, DeleteRecException, LeafRedistributeException, KeyTooLongException, RecordNotFoundException, DeleteFashionException, KeyNotMatchException, RedistributeException, IndexSearchException {
      updateIndexFiles(map, mid, operation, type, false, false, false, false);
    }

    // operation: 0 -> insert, 1 -> delete, 2 -> inserts only the affected indexes, 3 -> deletes only the affected indexes
    private void updateIndexFiles (Map map, RID mid, int operation, int type,
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
      StringKey defaultkey, key;
      
      defaultkey = new StringKey(map.getRowLabel() + map.getColumnLabel());

      StringKey[] keys = new StringKey[5];
      keys[0] = null;
      keys[1] = new StringKey(map.getRowLabel());
      keys[2] = new StringKey(map.getColumnLabel());
      keys[3] = new StringKey(map.getColumnLabel() + map.getRowLabel());
      keys[4] = new StringKey(map.getRowLabel() + map.getValue());

      if(operation == 0) {
        m_defaultindex.insert(defaultkey, mid);
        indexInsertAll(mid, type, keys);
      } else if(operation == 1) {
        m_defaultindex.Delete(defaultkey, mid);
        indexDeleteAll(mid, type, keys);
      } else {
        boolean[] affectedIndexes = new boolean[5];
        affectedIndexes[0] = false;
        affectedIndexes[1] = false;
        affectedIndexes[2] = false;
        affectedIndexes[3] = false;
        affectedIndexes[4] = false;

        if(isRowAffected) {
          affectedIndexes[1] = true;
          affectedIndexes[3] = true;
          affectedIndexes[4] = true;
        }
        if(isColAffected) {
          affectedIndexes[2] = true;
          affectedIndexes[3] = true;
        }
        if(isTimestampAffected) {
          // No indexes to update
        }
        if(isValueAffected) {
          affectedIndexes[4] = true;
        }

        if(operation == 3) {
          if(isRowAffected || isColAffected)
            m_defaultindex.insert(defaultkey, mid);
          indexInsertAffected(mid, type, keys, affectedIndexes);
        } else if(operation == 4) {
          if(isRowAffected || isColAffected)
            m_defaultindex.Delete(defaultkey, mid);
          indexDeleteAffected(mid, type, keys, affectedIndexes);
        }
      }
    }

    private RID checkDropMap (Map map, RID mid) throws Exception 
    {  
    	StringKey key = null;  
    	BTFileScan scan = null;  
    	Map current_map = null;  
    	Map oldest_map = null;  
    	RID current_mid = null;  
    	RID oldest = null;  
    	int timestamp_count = 0;  
    	KeyDataEntry current_entry = null;    
    	key = new StringKey(map.getRowLabel() + map.getColumnLabel());    
    	
    	try 
    	{    
    	  scan = m_defaultindex.new_scan(key, key);  
    	}  catch (Exception e) 
    	{    
    	  e.printStackTrace();    
    	  System.err.println("Failed to create new Scan in checkDropMap\n");  
    	}  
    	
    	Vector<Map> row_col_maps = new Vector<>();  
    	Vector<RID> row_col_mids = new Vector<>();  
    	
    	while(true) {    
    		KeyDataEntry nextMap = scan.get_next();    
    		if(nextMap == null) break;    
    		current_mid = ((LeafData) nextMap.data).getData();    
    		row_col_mids.add(current_mid);    
    		
    		if(current_mid == null)      
    			System.out.println("NULL MID");    
    		current_map = m_heap_files.get(current_mid.heapIndex).getMap(current_mid);    
    		row_col_maps.add(current_map);  
    	}  
    	
    	if(row_col_maps.size() < 4)      
    		return null;  int min_timestamp = 100000000, min_timestamp_index = -1;  
    		
    	for(int i = 0; i < row_col_maps.size(); i++) {    
    		if(row_col_maps.get(i).getTimeStamp() < min_timestamp) {      
    			min_timestamp = row_col_maps.get(i).getTimeStamp();      
    			min_timestamp_index = i;    
    		}  
    	}  
    	
    	return row_col_mids.get(min_timestamp_index);
    }
  
  /*
    Insert a map into the big table and returns an MID.
    This method ensure that there are at most 3 maps with the same 
    row and column label but different timestamps in the big table.
    When a fourth map is inserted it drops the map with the oldest
    label from the big table
  */
    public RID insertMap ( byte[] mapPtr, int type)
            throws InvalidSlotNumberException,
		  InvalidMapSizeException,
	   SpaceNotAvailableException,
	   HFException,
	   HFBufMgrException,
	   HFDiskMgrException,
	   IOException {
      RID mid = null;
      Heapfile heap_file = null;
      
      try{
        Map map = new Map(mapPtr, 0);
        heap_file = m_heap_files.get(type - 1);

        // Change insertRecord in heapfile to insertMap
        mid = heap_file.insertMap(mapPtr);

        // Index the Map
        updateIndexFiles(map, mid, 0, type);

//        // Check whether to drop the map
        RID oldestMapID = checkDropMap(map, mid);

        if(oldestMapID != null) {
          System.out.println("4th MID = " + oldestMapID.pageNo.pid + " " + oldestMapID.slotNo);
          heap_file = m_heap_files.get(oldestMapID.heapIndex);
          Map oldestMap = heap_file.getMap(oldestMapID);

          updateIndexFiles(oldestMap, oldestMapID, 1, oldestMapID.heapIndex + 1);

          // Change deleteRecord in heapfile to deleteMap
          Boolean didDelete = heap_file.deleteMap(oldestMapID);
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
        Heapfile heap_file = m_heap_files.get(mid.heapIndex);
        deleteSuccess = heap_file.deleteMap(mid);
        if(deleteSuccess) {
          Map map = heap_file.getMap(mid); // getRecord from heapfile
          updateIndexFiles(map, mid, 1, mid.heapIndex + 1);
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
        Heapfile heap_file = m_heap_files.get(mid.heapIndex);
        
        // get current map
        Map currentmap = heap_file.getMap(mid);

        // update map (heapfile)
        updateSuccess = heap_file.updateMap(mid, newmap);

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
          updateIndexFiles(currentmap, mid, 3, mid.heapIndex + 1, isRowAffected, isColAffected, isTimestampAffected, isValueAffected);

          // insert mid into the indexes which involve the affected keys
          updateIndexFiles(newmap, mid, 2, mid.heapIndex + 1, isRowAffected, isColAffected, isTimestampAffected, isValueAffected);
        }
      }
      catch (Exception e) {
        System.err.println("Failed to update Map in BigT class\n");
      }

      return updateSuccess;
    }
    
    public Map getMap(RID mid)
    throws InvalidSlotNumberException,
		  InvalidMapSizeException,
	   HFException, 
	   HFDiskMgrException,
	   HFBufMgrException,
	   Exception
    {
    	return m_heap_files.get(mid.heapIndex).getMap(mid);
    }
    
    public BigTScan openScan() 
    throws IOException,
	   FileScanException,
	   TupleUtilsException, 
	   InvalidRelation,
	   InvalidMapSizeException
    {
      BigTScan newscan = new BigTScan(this);
      return newscan;
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
      return new Stream(this, orderType, rowFilter, columnFilter, valueFilter, null);
    }
    
    public java.lang.String getName ()
    {
      return m_name;
    }
} // end of bigt





