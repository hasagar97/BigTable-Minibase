package BigT;

import java.io.*;
import java.lang.*;
import global.*;
import BigT.*;

package BigT;

import java.io.*;
import java.lang.*;
import global.*;
import BigT.*;

public class Map {
    private byte[] map;
    private int offset;
    private int size;

  /*
    Class constructor create a new map with the appropriate size.
  */
  public Map()
  {
    this.map = new byte[1024];
    this.offset = 0;
    this.size = this.map.length;
  }
  
  /*
    Construct a map from a byte array.
  */
  public Map(byte[] amap, int offset)
  {
    this.map = amap;
    this.offset = offset;
    this.size = this.map.length;    
  }
  
  /*
    Construct a map from another map through copy.
  */
  public Map(Map fromMap)
  {
    this.map = new byte[fromMap.size];
    System.arraycopy(fromMap.map, fromMap.offset, this.map, 0, fromMap.size);
    this.offset = 0;
    this.size = this.map.length;
  }
  
  /*
    Returns the row label.
  */
  public java.lang.String getRowLabel()
  {
    return new String(map, offset, MAXROWLABELSIZE).trim();
  }
  
  /*
    Returns the column label.
  */
  public java.lang.String getColumnLabel()
  {
    return new String(map, offset + MAXROWLABELSIZE, MAXCOLUMNLABELSIZE).trim();
  }
  
  /*
    Returns the timestamp.
  */
  public int getTimeStamp()
  {
    return Convert.getIntValue(offset + MAXROWLABELSIZE + MAXCOLUMNLABELSIZE, map);
  }
  
  /*
    Returns the value.
  */
  public java.lang.String getValue()
  {
    return new String(map, offset + MAXROWLABELSIZE + MAXCOLUMNLABELSIZE + 4, MAXVALUESIZE).trim();
  }
  
  /*
    Set the row label.
  */
  public Map setRowLabel(java.lang.String val)
  {
    Convert.setStrValue(val, offset, map);
    return this;
  }
  
  /*
    Set the column label.
  */
  public Map setColumnLabel(java.lang.String val)
  {
    Convert.setStrValue(val, offset + MAXROWLABELSIZE, map);
    return this;
  }
  
  /*
    Set the timestamp.
  */
  public Map setTimeStamp(int val)
  {
    Convert.setIntValue(val, offset + MAXROWLABELSIZE + MAXCOLUMNLABELSIZE, map);
    return this;
  }
  
  /*
    Set the value.
  */
  public Map setValue(java.lang.String val)
  {
    Convert.setStrValue(val, offset + MAXROWLABELSIZE + MAXCOLUMNLABELSIZE + 4, map);
    return this;
  }
  
  /*
    Copy the map to byte array out.
  */
  public byte[] getMapByteArray()
  {
    byte[] out = new byte[size];
    System.arraycopy(map, offset, out, 0, size);
    return out;
  }
  
  /*
    Print out the map.
  */
  public void print()
  {
     System.out.println(getRowLabel() + " " + getColumnLabel() + " " + getTimeStamp() + " " + getValue());

  }
  
  /*
    Get the length of the tuple
  */
  public int size()
  {
    return size;
  }
  
  /*
    Copy the given map
  */
  public Map mapCopy(Map fromMap)
  {
    System.arraycopy(fromMap.map, fromMap.offset, this.map, this.offset, fromMap.size);
    this.size = fromMap.size;
  }
  
  /*
    This is used when you don’t want to use the constructor.
  */
  public Map mapInit(byte[] amap, int offset)
  {
     this.map = amap;
    this.offset = offset;
    this.size = this.map.length;
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
    System.arraycopy(frommap, offset, this.map, this.offset, this.size);
  }
} // end of Map
