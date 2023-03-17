package BigT;

import java.io.*;
import java.lang.*;
import java.util.*;

import com.sun.org.apache.xpath.internal.operations.Bool;
import global.*;
import BigT.*;
import btree.*;
import heap.*;
import global.*;


public class BigT extends HeapFile{
  private int m_strategy;
  private IndexFile m_indexfile1 = null;
  private IndexFile m_indexfile2 = null;

  /* 
    Initializes the big table
  	name - name of the table
  	type - the clustering/index strategy to use
  */
  public BigT(java.lang.String name, int type) throws HFDiskMgrException, HFException, HFBufMgrException, IOException, ConstructPageException, GetFileEntryException, PinPageException {
    super(name);
    m_strategy = type;

    switch (m_strategy) {
      case 1:
        // one btree to index row labels
        m_indexfile1 = new BTreeFile(name + "_row_index");
        break;
      case 2:
        // one btree to index column labels
        m_indexfile1 = new BTreeFile(name + "_column_index");
        break;
      case 3:
        // one btree to index column label and row label (combined key) and
        // one btree to index timestamps
        m_indexfile1 = new BTreeFile(name + "_row_col_index");
        m_indexfile2 = new BTreeFile(name + "_timestamp_index");
        break;
      case 4:
        // one btree to index row label and value (combined key) and
        // one btree to index timestamps
        m_indexfile1 = new BTreeFile(name + "_row_val_index");
        m_indexfile2 = new BTreeFile(name + "_timestamp_index");
        break;
      case 5:
        // Our new index strategy
        break;
    }
  }
  
  /*
    Deletes the big table from the database
  */
    public void deleteBigt ()
    {
      m_hfile.deleteFile();
    }

  /*
    Returns the number of maps in the big table
  */
    public int getMapCnt ()
    {
      return m_hfile.getMapCnt();
    }

  /*
    Returns the number of distinct row labels in the big table
  */
    public int getRowCnt ()
    {
      return m_row_set.size();
    }

  /*
    Returns the number of distinct column labels in the big table
  */
    public int getColumnCnt ()
    {
      return m_col_set.size();
    }

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
      StringKey key, key1, key2;
      switch (m_strategy) {
        case 1:
          // one btree to index row labels
          key = new StringKey(map.getRowLabel());
          if(operation == 0) m_indexfile1.insert(key, mid);
          else if(operation == 1) m_indexfile1.Delete(key, mid);
          else {
            if(isRowAffected) {
              if(operation == 2) m_indexfile1.insert(key, mid);
              else if(operation == 3) m_indexfile1.Delete(key, mid);
            }
          }
          break;
        case 2:
          // one btree to index column labels
          key = new StringKey(map.getColumnLabel());
          if(operation == 0) m_indexfile1.insert(key, mid);
          else if(operation == 1) m_indexfile1.Delete(key, mid);
          else {
            if(isColAffected) {
              if(operation == 2) m_indexfile1.insert(key, mid);
              else if(operation == 3) m_indexfile1.Delete(key, mid);
            }
          }
          break;
        case 3:
          // one btree to index column label and row label (combined key) and
          // one btree to index timestamps
          key1 = new StringKey(map.getColumnLabel() + map.getRowLabel());
          key2 = new StringKey(map.getTimeStamp());
          if(operation == 0) {
            m_indexfile1.insert(key1, mid);
            m_indexfile2.insert(key2, mid);
          } else if(operation == 1) {
            m_indexfile1.Delete(key1, mid);
            m_indexfile2.Delete(key2, mid);
          } else {
            if(isRowAffected || isColAffected) {
              if(operation == 2) m_indexfile1.insert(key1, mid);
              else if(operation == 3) m_indexfile1.Delete(key1, mid);
            }

            if(isTimestampAffected) {
              if(operation == 2) m_indexfile2.insert(key2, mid);
              else if(operation == 3) m_indexfile2.Delete(key2, mid);
            }
          }
          break;
        case 4:
          // one btree to index row label and value (combined key) and
          // one btree to index timestamps
          key1 = new StringKey(map.getRowLabel() + map.getValue());
          key2 = new StringKey(map.getTimeStamp());
          if(operation == 0) {
            m_indexfile1.insert(key1, mid);
            m_indexfile2.insert(key2, mid);
          } else if(operation == 1){
            m_indexfile1.Delete(key1, mid);
            m_indexfile2.Delete(key2, mid);
          } else {
            if(isRowAffected || isValueAffected) {
              if(operation == 2) m_indexfile1.insert(key1, mid);
              else if(operation == 3) m_indexfile1.Delete(key1, mid);
            }

            if(isTimestampAffected) {
              if(operation == 2) m_indexfile2.insert(key2, mid);
              else if(operation == 3) m_indexfile2.Delete(key2, mid);
            }
          }
          break;
        case 5:
          // Our new index strategy
          break;
      }
    }

    private boolean checkDropMap ( int row_hash, int column_hash, int timestamp)
    {
      boolean drop = false;

      if (timestamp_map.containsKey(row_hash)) {
        Map<Integer, Object> col_map = timestamp_map.get(row_hash);

        if (col_map.containsKey(column_hash)) {
          Set<Integer> timestamp_set = col_map.get(column_hash);

          if (timestamp_set.size() >= 3) {
            drop = true;
            Integer lowest = timestamp;
            timestamp_set.add(timestamp);

            Iterator<Integer> it = timestamp_set.iterator();

            while (it.hasNext()) {
              if (it.next() < lowest) {
                lowest = it.next();
              }
            }

            timestamp_set.remove(lowest);
          }
        } else {
          Set<Integer> timestamp_set = new HashSet<Integer>();
          Map<Integer, Object> col_map = new HashMap<Integer, Object>();

          timestamp_set.add(timestamp);
          col_map.put(column_hash, timestamp_set);
          timestamp_map.put(row_hash, col_map);
        }
      } else {
        Set<Integer> timestamp_set = new HashSet<Integer>();
        Map<Integer, Object> col_map = new HashMap<Integer, Object>();

        timestamp_set.add(timestamp);
        col_map.put(column_hash, timestamp_set);
        timestamp_map.put(row_hash, col_map);
      }

      return drop;
    }

  private MID getOldest()
  {
    // TODO
    return new MID();
  }
  
  /*
    Insert a map into the big table and returns an MID.
    This method ensure that there are at most 3 maps with the same 
    row and column label but different timestamps in the big table.
    When a fourth map is inserted it drops the map with the oldest
    label from the big table
  */
    public RID insertMap ( byte[] mapPtr)
            throws IteratorException, ConstructPageException,
            InsertRecException, ConvertException, InsertException,
            IndexInsertRecException, LeafDeleteException,
            NodeNotMatchException, LeafInsertRecException,
            PinPageException, IOException, UnpinPageException,
            FreePageException, IndexFullDeleteException, DeleteRecException,
            LeafRedistributeException, KeyTooLongException, RecordNotFoundException,
            DeleteFashionException, KeyNotMatchException, RedistributeException,
            IndexSearchException {
      Map map = Map(mapPtr, 0);

      // Change insertRecord in heapfile to insertMap
      RID mid = insertMap(mapPtr);

      // Index the Map
      updateIndexFiles(map, mid, 1);

      // Check whether to drop the map
      RID oldestMapID = checkDropMap(map, mid);

      if(oldestMapID != null) {
        Map oldestMap = getMap(oldestMapID);

        updateIndexFiles(oldestMap, oldestMapID, 1);

        // Change deleteRecord in heapfile to deleteMap
        deleteMap(oldestMapID);
      }

      return mid;
    }

    public Boolean deleteMap(RID mid) throws IteratorException, ConstructPageException, InsertRecException, ConvertException, InsertException, IndexInsertRecException, LeafDeleteException, NodeNotMatchException, LeafInsertRecException, PinPageException, IOException, UnpinPageException, FreePageException, IndexFullDeleteException, DeleteRecException, LeafRedistributeException, KeyTooLongException, RecordNotFoundException, DeleteFashionException, KeyNotMatchException, RedistributeException, IndexSearchException {
      Boolean deleteSuccess = deleteMap(mid); // can methods in higher class, override methods in base class and still call them ?
      if(deleteSuccess) {
        Map map = getMap(mid) // getRecord from heapfile
        updateIndexFiles(map, mid, 1);
      }
      return deleteSuccess;
    }

    public Boolean updateMap(RID mid, Map newmap) throws IteratorException, ConstructPageException, InsertRecException, ConvertException, InsertException, IndexInsertRecException, LeafDeleteException, NodeNotMatchException, LeafInsertRecException, PinPageException, IOException, UnpinPageException, FreePageException, IndexFullDeleteException, DeleteRecException, LeafRedistributeException, KeyTooLongException, RecordNotFoundException, DeleteFashionException, KeyNotMatchException, RedistributeException, IndexSearchException {
      // get current map
      Map currentmap = getMap(mid);

      // update map (heapfile)
      Boolean updateSuccess = updateMap(mid, newmap);

      if(updateSuccess) {
        // find affected attributes
        Boolean isRowAffected = MapUtils.compareMapWithMap(currentmap, newmap, 0);
        Boolean isColAffected = MapUtils.compareMapWithMap(currentmap, newmap, 1);
        Boolean isTimestampAffected = MapUtils.compareMapWithMap(currentmap, newmap, 2);
        Boolean isValueAffected = MapUtils.compareMapWithMap(currentmap, newmap, 3);

        // delete mid from those indexes
        updateIndexFiles(currentmap, mid, 3, isRowAffected, isColAffected, isTimestampAffected, isValueAffected);

        // insert mid into the indexes which involve the affected keys
        updateIndexFiles(newmap, mid, 2, isRowAffected, isColAffected, isTimestampAffected, isValueAffected);
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
    java.lang.String valueFilter)
    {
      return new Stream(this, orderType, rowFilter, columnFilter, valueFilter);
    }

    public int getStrategy ()
    {
      return m_strategy;
    }

    public int getIndexFile ()
    {
      return m_indexfile;
    }
  } // end of bigt
}




