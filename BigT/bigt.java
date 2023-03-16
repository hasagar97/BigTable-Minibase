package BigT;

import java.io.*;
import java.lang.*;
import java.util.*;
import global.*;
import BigT.*;
import btree.*;
import heap.*;


public class bigt extends HeapFile{
  private int m_strategy;
  private IndexFile m_indexfile1 = null;
  private IndexFile m_indexfile2 = null;

  /* 
    Initializes the big table
  	name - name of the table
  	type - the clustering/index strategy to use
  */
  public bigt(java.lang.String name, int type)
  {
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
  
  /*
    Deletes the big table from the database
  */
  public void deleteBigt()
  {
    m_hfile.deleteFile();
  }

  /*
    Returns the number of maps in the big table
  */
  public int getMapCnt()
  {
    return m_hfile.getMapCnt();
  }

  /*
    Returns the number of distinct row labels in the big table
  */ 
  public int getRowCnt()
  {
    return m_row_set.size();
  }

  /*
    Returns the number of distinct column labels in the big table
  */ 
  public int getColumnCnt()
  {
    return m_col_set.size();
  }

  private void indexMap(Map map, MID mid)
  {
    switch (m_strategy) {
      case 1:
        // one btree to index row labels
        StringKey key = new StringKey(map.getRowLabel());
        m_indexfile1.insert(key, mid);
        break;
      case 2:
        // one btree to index column labels
        StringKey key = new StringKey(map.getColumnLabel());
        m_indexfile1.insert(key, mid);
        break;
      case 3:
        // one btree to index column label and row label (combined key) and
        // one btree to index timestamps
        StringKey key1 = new StringKey(map.getColumnLabel() + map.getRowLabel());
        StringKey key2 = new StringKey(map.getTimeStamp());
        m_indexfile1.insert(key1, mid);
        m_indexfile2.insert(key2, mid);
        break;
      case 4:
        // one btree to index row label and value (combined key) and
        // one btree to index timestamps
        StringKey key1 = new StringKey(map.getRowLabel() + map.getValue());
        StringKey key2 = new StringKey(map.getTimeStamp());
        m_indexfile1.insert(key1, mid);
        m_indexfile2.insert(key2, mid);
        break;
      case 5:
        // Our new index strategy
        break;
    }
  }

  private boolean checkDropMap(int row_hash, int column_hash, int timestamp)
  {
    boolean drop = false;

    if (timestamp_map.containsKey(row_hash))
    {
      Map<Integer, Object> col_map = timestamp_map.get(row_hash);

      if (col_map.containsKey(column_hash))
      {
        Set<Integer> timestamp_set = col_map.get(column_hash);

        if (timestamp_set.size() >= 3)
        {
          drop = true;
          Integer lowest = timestamp;
          timestamp_set.add(timestamp);

          Iterator<Integer> it = timestamp_set.iterator();
          
          while(it.hasNext())
          {
            if (it.next() < lowest)
            {
              lowest = it.next();
            }
          }

          timestamp_set.remove(lowest);
        }
      }
      else
      {
        Set<Integer> timestamp_set = new HashSet<Integer> ();
        Map<Integer, Object> col_map = new HashMap<Integer, Object>();

        timestamp_set.add(timestamp);
        col_map.put(column_hash, timestamp_set);
        timestamp_map.put(row_hash, col_map);
      }
    }
    else
    {
      Set<Integer> timestamp_set = new HashSet<Integer> ();
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
  public MID insertMap(byte[] mapPtr)
  {
    Map map = Map(mapPtr, 0);
    MID mid = insertMap(mapPtr);
    java.lang.String row_label = map.getRowLabel();
    java.lang.String column_label = map.getColumnLabel();
    int timestamp = map.getTimeStamp();
    int row_hash = row_label.hashCode();
    int column_hash = column_label.hashCode();

    m_row_set.add(row_hash);
    m_col_set.add(column_hash);

    // Index the Map
    indexMap(map, mid);

    // Check whether to drop the map
    if (checkDropMap(row_hash, column_hash, timestamp))
    {
      MID oldest = getOldest(row_hash, column_hash);
      m_hfile.deleteRecord(oldest);
<<<<<<< HEAD
=======
      // TODO delete oldest index
>>>>>>> 0b83845 (Update bigt.java)
    }

    return mid;
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
  public Stream openStream(int orderType, 
  			java.lang.String rowFilter, 
  			java.lang.String columnFilter, 
  			java.lang.String valueFilter)
  {
    return new Stream(this, orderType, rowFilter, columnFilter, valueFilter);
  }

  public int getStrategy()
  {
    return m_strategy;
  }

  public int getIndexFile()
  {
    return m_indexfile;
  }
} // end of bigt





