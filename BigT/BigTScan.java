package BigT;
   

import BigT.*;
import heap.*;
import global.*;
import bufmgr.*;
import iterator.*;


import java.lang.*;
import java.io.*;

/**
 *open a heapfile and according to the condition expression to get
 *output file, call get_next to get all tuples
 */
public class BigTScan
{
  private BigT bigtable = null;
  private int currentHeapIndex = 0;
  private Scan currentScan = null;

  /**
   *constructor
   *@param bigtable the BigT instance to scan
   */
  public  BigTScan (BigT bigtable)
    throws IOException,
	   FileScanException,
	   TupleUtilsException, 
	   InvalidRelation,
	   InvalidMapSizeException
    {
      this.bigtable = bigtable;
      this.currentHeapIndex = 0;
      this.currentScan = this.bigtable.m_heap_files.get(this.currentHeapIndex).openScan();
    }
  
  /**
   *@return the result tuple
   *@exception JoinsException some join exception
   *@exception IOException I/O errors
   *@exception InvalidMapSizeException invalid tuple size
   *@exception InvalidTypeException tuple type not valid
   *@exception PageNotReadException exception from lower layer
   *@exception PredEvalException exception from PredEval class
   *@exception UnknowAttrType attribute type unknown
   *@exception FieldNumberOutOfBoundException array out of bounds
   *@exception WrongPermat exception for wrong FldSpec argument
   */
  public Map get_next()
    throws JoinsException,
	   IOException,
           InvalidMapSizeException,
	   InvalidTypeException,
	   PageNotReadException, 
	   PredEvalException,
	   UnknowAttrType,
	   FieldNumberOutOfBoundException,
	   WrongPermat
    { 
      RID mid = new RID();    
      Map next_map = this.currentScan.getNext(mid);
      
      if (next_map == null)
      {
        this.currentHeapIndex += 1;
        this.currentScan.closescan();
        this.currentScan = this.bigtable.m_heap_files.get(this.currentHeapIndex).openScan();
        next_map = this.currentScan.getNext(mid);
      }
      
      return next_map;
    }

  /**
   *implement the abstract method close() from super class Iterator
   *to finish cleaning up
   */
  public void close() 
    {
      if (currentScan != null) {
	currentScan.closescan();
      } 
    }
}


