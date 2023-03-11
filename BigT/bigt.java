package BigT;

import java.io.*;
import java.lang.*;
import global.*;
import BigT.*;


public class bigt {
  /* 
    Initializes the big table
  	name - name of the table
  	type - the clustering/index strategy to use
  */
  public bigt(java.lang.String name, int type)
  { 
  }
  
  /*
    Deletes the big table from the database
  */
  public void deleteBigt()
  {
  }

  /*
    Returns the number of maps in the big table
  */
  public int getMapCnt()
  {
    return 0;
  }

  /*
    Returns the number of distinct row labels in the big table
  */ 
  public int getRowCnt()
  {
    return 0;
  }

  /*
    Returns the number of distinct column labels in the big table
  */ 
  public int getColumnCnt()
  {
    return 0;
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
    return new MID();
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
} // end of bigt





