package BigT;

import java.io.*;
import java.lang.*;
import global.*;
import BigT.*;


public class Map {
  /*
    Class constructor create a new map with the appropriate size.
  */
  public Map()
  {
  }
  
  /*
    Construct a map from a byte array.
  */
  public Map(byte[] amap, int offset)
  {
  }
  
  /*
    Construct a map from another map through copy.
  */
  public Map(Map fromMap)
  {
  }
  
  /*
    Returns the row label.
  */
  public java.lang.String getRowLabel()
  {
    return "";
  }
  
  /*
    Returns the column label.
  */
  public java.lang.String getColumnLabel()
  {
    return "";
  }
  
  /*
    Returns the timestamp.
  */
  public int getTimeStamp()
  {
    return 0;
  }
  
  /*
    Returns the value.
  */
  public java.lang.String getValue()
  {
    return "";
  }
  
  /*
    Set the row label.
  */
  public Map setRowLabel(java.lang.String val)
  {
    return new Map();
  }
  
  /*
    Set the column label.
  */
  public Map setColumnLabel(java.lang.String val)
  {
    return new Map();
  }
  
  /*
    Set the timestamp.
  */
  public Map setTimeStamp(int val)
  {
    return new Map();
  }
  
  /*
    Set the value.
  */
  public Map setValue(java.lang.String val)
  {
    return new Map();
  }
  
  /*
    Copy the map to byte array out.
  */
  public byte[] getMapByteArray()
  {
    return new byte[0];
  }
  
  /*
    Print out the map.
  */
  public void print()
  {
  }
  
  /*
    Get the length of the tuple
  */
  public int size()
  {
    return 0;
  }
  
  /*
    Copy the given map
  */
  public Map mapCopy(Map fromMap)
  {
    return new Map();
  }
  
  /*
    This is used when you don’t want to use the constructor.
  */
  public Map mapInit(byte[] amap, int offset)
  {
    return new Map();
  }
  
  /*
    Set a map with the given byte array and offset. 
    After creating this map construct, remove the heap.
    Tuple completely from the minibase and modify all 
    related code to use BigT.Map instead; that is, your 
    version of minibase will not store tuples anymore.
  */
  public Map mapSet(byte[] frommap, int offset)
  {
    return new Map();
  }
} // end of Map
