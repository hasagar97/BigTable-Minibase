package BigT;

import java.io.*;
import java.lang.*;
import global.*;
import BigT.*;
import iterator.CorruptedFieldNo;


public class Map implements  GlobalConst{




    /**
     * a byte array to hold data, along with it's own offset and size
     */
    private byte [] map;

    /**
     * start position of this map in data[]
     */
    private int map_offset = 0;

    /**
     * length of this tuple
     */
    private int map_length;

    /**
     * private field
     * Number of fields in this tuple
     */

    private short ROW_LABEL_SIZE = 20;
    private short COLUMN_LABEL_SIZE = 20;
    private short TIMESTAMP_LABEL_SIZE = 4;
    private short VALUE_LABEL_SIZE = 20;


    /**
     * Maximum size of any tuple
     */
    public final int max_size = ROW_LABEL_SIZE + COLUMN_LABEL_SIZE
            +TIMESTAMP_LABEL_SIZE + VALUE_LABEL_SIZE;


    /*
      Class constructor create a new map with the appropriate size.
    */
    public Map()
    {
        this.map = new byte[max_size];
//        this.size = this.map.length;
    }

    /*
      Construct a map from a byte array.
    */
    public Map(byte[] amap, int offset)
    {
        this.map = amap;
        this.map_offset = offset;
        this.map_length = this.map.length;
    }

    /*
      Construct a map from another map through copy.
    */
    public Map(Map fromMap)
    {
        this.map = new byte[fromMap.map_length];
        System.arraycopy(fromMap.map, fromMap.map_offset, this.map, 0, fromMap.map_length);
        this.map_offset = 0;
//        this.map_length = this.map.length;
    }

    /*
      Returns the row label.
    */
    public java.lang.String getRowLabel()
    {
        return new String(map, map_offset,ROW_LABEL_SIZE);
//        return "";
    }

    /*
      Returns the column label.
    */
    public java.lang.String getColumnLabel()
    {
        return new String(map, map_offset+ROW_LABEL_SIZE,COLUMN_LABEL_SIZE);
//        return "";
    }

    /*
      Returns the timestamp.
    */
    public int getTimeStamp() throws IOException {
        return Convert.getIntValue(map_offset+ROW_LABEL_SIZE+COLUMN_LABEL_SIZE,map);
    }

    /*
      Returns the value.
    */
    public java.lang.String getValue()
    {
        return new String(map, map_offset+ROW_LABEL_SIZE+COLUMN_LABEL_SIZE+TIMESTAMP_LABEL_SIZE,VALUE_LABEL_SIZE);
    }

    /*
      Set the row label.
    */
    public Map setRowLabel(java.lang.String val) throws IOException {
//        if(val.length() > ROW_LABEL_SIZE) {
//            throw CorruptedFieldNo();
//        }
        Convert.setStrValue(val, map_offset, map);
        return this;
    }

    /*
      Set the column label.
    */
    public Map setColumnLabel(java.lang.String val) throws IOException {
        Convert.setStrValue(val, map_offset+ROW_LABEL_SIZE, map);
        return this;
//        return new Map();
    }

    /*
      Set the timestamp.
    */
    public Map setTimeStamp(int val) throws IOException {
        Convert.setIntValue(val, map_offset+ROW_LABEL_SIZE+COLUMN_LABEL_SIZE, map);
        return this;
//        return new Map();
    }

    /*
      Set the value.
    */
    public Map setValue(java.lang.String val) throws IOException {
        Convert.setStrValue(val, map_offset+ROW_LABEL_SIZE+COLUMN_LABEL_SIZE+TIMESTAMP_LABEL_SIZE, map);
        return this;
    }

    /*
      Copy the map to byte array out.
    */
    public byte[] getMapByteArray()
    {
        byte [] mapcopy = new byte [max_size];
        System.arraycopy(this.map, this.map_offset, mapcopy, 0, this.map_length);
        return mapcopy;
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