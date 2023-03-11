package BigT;

import java.io.*;
import java.lang.*;
import global.*;
import BigT.*;


public class Stream {
  /*
    Initialize a stream of maps on bigtable.
  */
  public Stream(bigt bigtable, int orderType, 
  		java.lang.String rowFilter, 
  		java.lang.String columnFilter, 
  		java.lang.String valueFilter)
  {
  }
  
  /*
    Closes the stream object.
  */
  public void closestream()
  {
  }
  
  /*
    Retrieve the next map in the stream.
  */
  public Map getNext(MID mid)
  {
    return new Map();
  }
} // end of Stream
