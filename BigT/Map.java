/* File Tuple.java */

package BigT;

import java.io.*;
import java.lang.*;
import global.*;


public class Map implements GlobalConst{
    /**
     * Maximum size of any tuple
     */
    public static final int max_size = MINIBASE_PAGESIZE;

    /**
     * a byte array to hold data, along with it's own offset and size
     */
    private byte [] map;

    /**
     * start position of this map in data[]
     */
    private int map_offset;

    /**
     * length of this tuple
     */
    private int map_length;

    /**
     * private field
     * Number of fields in this tuple
     */
    private short fldCnt;

    /**
     * private field
     * Array of offsets of the fields
     */

    private short [] fldOffset;


    /**
        Variables for map data structure
     */

    public static final int MAX_FIELD_COUNT = 4;
    public static final int MAX_SIZE = MINIBASE_PAGESIZE;
    public static final int MAXROWLABELSIZE = 16;
    public static final int MAXCOLUMNLABELSIZE = 16;
    public static final int MAXVALUESIZE = 16;




    /*
      Class constructor create a new map with the appropriate size.
    */
    public Map()
    {
        this.map = new byte[MAX_SIZE];
        this.map_offset = 0;
        this.map_length = this.map.length;
    }

    /*
      Construct a map from a byte array.
    */
    public Map(byte[] amap, int offset)
    {
        this.map = amap;
        this.map_offset = offset;
        // [TODO] should the field offset be initialised with data?
        // [todo] should the offset count be set here?
        this.map_length = this.map.length;
    }

    /*
      Construct a map from another map through copy.
    */
    public Map(Map fromMap)
    {
        this.map = fromMap.getMapByteArray();
        // todo should the map offset be copied?
        this.map_offset = 0;
        this.map_length = fromMap.getLength();
        this.fldCnt = fromMap.noOfFlds();
        this.fldOffset = fromMap.copyFldOffset();

//        removing older implementation
//        this.map = new byte[fromMap.map_length];
//        System.arraycopy(fromMap.map, fromMap.map_offset, this.map, 0, fromMap.map_length);
//        this.map_offset = 0;
//        this.map_length = this.map.length;
    }

    /*
      Construct a map from another map through copy with given offset and len.
    */
    public Map(byte [] fromMap,int offset,int size)
    {
        this.map = fromMap;
//        System.arraycopy(fromMap.map, fromMap.offset, this.map, 0, fromMap.size);
        this.map_offset = offset;
        this.map_length = size;
        // [TODO] should the field offset be initialised with data?
        // [todo] should the offset count be set here?
    }

    /** Copy the tuple byte array out
     *  @return  byte[], a byte array contains the tuple
     *		the length of byte[] = length of the tuple
     */

    public byte [] getTupleByteArray(){
        byte [] tuplecopy = new byte [this.map_length];
        System.arraycopy(this.map, this.map_offset, tuplecopy, 0, this.map_length);
        return tuplecopy;
    }

    /*
      Copy the map to byte array out.
    */
    public byte[] getMapByteArray()
    {
        byte[] out = new byte[map_length];
        System.arraycopy(map, map_offset, out, 0, map_length);
        return out;
    }

    /** return the data byte array
     *  @return  data byte array
     */

    public byte [] returnTupleByteArray()
    {
        return this.map;
    }


    /** get the offset of a tuple
     *  @return offset of the tuple in byte array
     */
    public int getOffset()
    {
        return this.map_offset;
    }

    /***
      Returns the row label.
    */
    public java.lang.String getRowLabel()
    {
        return new String(map, map_offset, MAXROWLABELSIZE).trim();
    }

    /**
      Returns the column label.
    */
    public java.lang.String getColumnLabel()
    {
        return new String(map, map_offset + MAXROWLABELSIZE, MAXCOLUMNLABELSIZE).trim();
    }
//
    /**
      Returns the timestamp.
    */
    public int getTimeStamp() throws IOException {
        return Convert.getIntValue(map_offset + MAXROWLABELSIZE + MAXCOLUMNLABELSIZE, map);
    }

    /**
      Returns the Length.
    */
    public int getLength()
    {
        return this.map_length;
    }

    /**
     * Returns number of fields in this tuple
     *
     * @return the number of fields in this tuple
     *
     */

    public short noOfFlds()
    {
        return fldCnt;
    }

    /**
     * Makes a copy of the fldOffset array
     *
     * @return a copy of the fldOffset arrray
     *
     */

    public short[] copyFldOffset()
    {
        short[] newFldOffset = new short[fldCnt + 1];
        for (int i=0; i<=fldCnt; i++) {
            newFldOffset[i] = fldOffset[i];
        }

        return newFldOffset;
    }

    /*
      Returns the value.
    */
    public java.lang.String getValue()
    {
        return new String(map, map_offset + MAXROWLABELSIZE + MAXCOLUMNLABELSIZE + 4, MAXVALUESIZE).trim();
    }

    /*
      Set the row label.
    */
    public Map setRowLabel(java.lang.String val) throws IOException {
        Convert.setStrValue(val, map_offset, map);
        return this;
    }

    /*
      Set the column label.
    */
    public Map setColumnLabel(java.lang.String val) throws IOException {
        Convert.setStrValue(val, map_offset + MAXROWLABELSIZE, map);
        return this;
    }


    /*
      Set the timestamp.
    */
    public Map setTimeStamp(int val) throws IOException {
        Convert.setIntValue(val, map_offset + MAXROWLABELSIZE + MAXCOLUMNLABELSIZE, map);
        return this;
    }

    /*
      Set the value.
    */
    public Map setValue(java.lang.String val) throws IOException {
        Convert.setStrValue(val, map_offset + MAXROWLABELSIZE + MAXCOLUMNLABELSIZE + 4, map);
        return this;
    }



    /*
      Print out the map.
    */
    public void print() throws IOException {
        System.out.println(getRowLabel() + " " + getColumnLabel() + " " + getTimeStamp() + " " + getValue());

    }

    public void tupleCopy(Map fromTuple)
    {
        byte [] temparray = fromTuple.getTupleByteArray();
        System.arraycopy(temparray, 0, this.map, this.map_offset, this.map_length);
//       fldCnt = fromTuple.noOfFlds();
//       fldOffset = fromTuple.copyFldOffset();
    }


    /*
      Get the length of the tuple
    */
    public int size()
    {
        return map_length;
    }

    /*
      Copy the given map
    */
    public Map mapCopy(Map fromMap)
    {
        System.arraycopy(fromMap.map, fromMap.map_offset, this.map, this.map_offset, fromMap.map_length);
        this.map_length = fromMap.map_length;
        return this;
    }

    /*
      This is used when you don’t want to use the constructor.
    */
    public Map mapInit(byte[] amap, int offset)
    {
        this.map = amap;
        this.map_offset = offset;
        this.map_length = this.map.length;
        return this;
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
        System.arraycopy(frommap, offset, this.map, this.map_offset, this.map_length);
        return this;
    }
} // end of Map