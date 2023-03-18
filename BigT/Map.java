/* File Tuple.java */

package BigT;

import java.io.*;
import java.lang.*;
import global.*;
import iterator.*;
import heap.*;

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

    public static final short MAX_FIELD_COUNT = 4;
    public static final int MAX_SIZE = MINIBASE_PAGESIZE;
    public static final short ROW_INDEX = 1;
    public static final short COLUMN_INDEX = 2;
    public static final short TIMESTAMP_INDEX = 3;
    public static final short VALUE_INDEX = 3;




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
    public Map(byte[] amap, int offset) throws IOException {
        System.out.println("amap offset");
        this.map = amap;
        this.map_offset = offset;
        // [TODO] should the field offset be initialised with data?
        // [todo] should the offset count be set here?
        this.map_length = this.map.length;
        initFieldOffset();
        this.fldCnt = Convert.getShortValue(offset,this.map);
    }



    /*
      Construct a map from another map through copy with given offset and len.
    */
    public Map(byte [] fromMap,int offset,int size) throws IOException {
        System.out.println("from byte array, offset,size");
        this.map = fromMap;
//        System.arraycopy(fromMap.map, fromMap.offset, this.map, 0, fromMap.size);
        this.map_offset = offset;
        this.map_length = size;
        initFieldOffset();
        this.fldCnt = Convert.getShortValue(offset,this.map);


        System.out.println("Map created with offset: "+offset+" \n");
        // [TODO] should the field offset be initialised with data?
        // [todo] should the offset count be set here?
    }

    /*
      Construct a map from another map through copy.
    */
    public Map(Map fromMap)
    {
        System.out.println("from map");
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
      Class constructor create a new map with the appropriate size.
    */
    public Map(int size)
    {
        System.out.println("Map(size)");
        this.map = new byte[size];
        this.map_offset = 0;
        this.map_length = this.map.length;
    }


    /** Copy the tuple byte array out
     *  @return  byte[], a byte array contains the tuple
     *		the length of byte[] = length of the tuple
     */

    public byte [] getMapByteArray(){
        byte [] tuplecopy = new byte [this.map_length];
        System.arraycopy(this.map, this.map_offset, tuplecopy, 0, this.map_length);
        return tuplecopy;
    }

//    /*
//      Copy the map to byte array out.
//    */
//    public byte[] getMapByteArray()
//    {
//        byte[] out = new byte[map_length];
//        System.arraycopy(this.map, map_offset, out, 0, map_length);
//        return out;
//    }

    /** return the data byte array
     *  @return  data byte array
     */

    public byte [] returnMapByteArray()
    {
        byte [] tuplecopy = new byte [this.map_length];
        System.arraycopy(this.map, this.map_offset, tuplecopy, 0, this.map_length);
        return tuplecopy;
    }



    /** set the byte array
     */

    public void setByteData(byte[] data) throws IOException {
        this.map = data;
        initFieldOffset();
        fldCnt = Convert.getShortValue(0, data);
    }

    /** get the offset of a tuple
     *  @return offset of the tuple in byte array
     */
    public int getOffset()
    {
        return this.map_offset;
    }

    /** set the offset of a tuple
     */
    public void setOffset(int offset)
    {
        this.map_offset = offset;
    }

    /**
     Returns the Length.
     */
    public int getLength()
    {
        return this.map_length;
    }

    /**
     sets the Length.
     */
    public void setLength(int length)
    {
        this.map_length = length;
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


    public void setFldCnt(short count){
        this.fldCnt = count;
    }

    public short[] getFldOffset(){
        return fldOffset;
    }

    public void setFldOffset(short[] fldOffset) {
        this.fldOffset = fldOffset;
    }


    public String getStringField(short fieldNumber) throws IOException, CorruptedFieldNo {
        if (fieldNumber == 3) {
            throw new CorruptedFieldNo(null, "MAP: Invalid field number");
        } else {
            return Convert.getStrValue(this.fldOffset[fieldNumber - 1], this.map, this.fldOffset[fieldNumber] - this.fldOffset[fieldNumber - 1]);
        }
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
      Copy the given map
    */
    public Map copyMap(Map fromMap)
    {
        byte[] tmp = fromMap.getMapByteArray();
        // TODO should I copy the length fromMap
        System.arraycopy(fromMap.map, fromMap.map_offset, this.map, this.map_offset, fromMap.map_length);
        this.map_length = fromMap.map_length;
        return this;
    }

    
    /***
      Returns the row label.
    */
    public java.lang.String getRowLabel() throws IOException {
        return Convert.getStrValue(this.fldOffset[ROW_INDEX - 1], this.map,
                this.fldOffset[ROW_INDEX] - this.fldOffset[ROW_INDEX - 1]);
//        return new String(map, map_offset, MAXROWLABELSIZE).trim();
    }

    /*
      Set the row label.
    */
    public Map setRowLabel(java.lang.String val) throws IOException {
        Convert.setStrValue(val, this.fldOffset[ROW_INDEX - 1], this.map);
        return this;
    }

    /**
      Returns the column label.
    */
    public java.lang.String getColumnLabel() throws IOException {
        return Convert.getStrValue(this.fldOffset[COLUMN_INDEX - 1], this.map,
                this.fldOffset[COLUMN_INDEX] - this.fldOffset[COLUMN_INDEX - 1]);
//        return new String(map, map_offset + MAXROWLABELSIZE, MAXCOLUMNLABELSIZE).trim();
    }
//

    /*
  Set the column label.
*/
    public Map setColumnLabel(java.lang.String val) throws IOException {
        Convert.setStrValue(val, this.fldOffset[COLUMN_INDEX - 1], this.map);
        return this;
    }



    /**
      Returns the timestamp.
    */
    public int getTimeStamp() throws IOException {
        System.out.println("attempting to getTimeStamp()\n");
        return Convert.getIntValue(this.fldOffset[TIMESTAMP_INDEX - 1], this.map);
//        return Convert.getIntValue(map_offset + MAXROWLABELSIZE + MAXCOLUMNLABELSIZE, map);
    }

    /*
      Set the timestamp.
    */
    public Map setTimeStamp(int val) throws IOException {
        Convert.setIntValue(val, this.fldOffset[TIMESTAMP_INDEX-1], map);
        return this;
    }






    /*
      Returns the value.
    */
    public java.lang.String getValue()
    {
        return new String(map, map_offset + ROW_INDEX + COLUMN_INDEX + 4, VALUE_INDEX).trim();
    }



    /*
      Set the value.
    */
    public Map setValue(java.lang.String val) throws IOException {
        Convert.setStrValue(val, this.fldOffset[VALUE_INDEX-1], map);
        return this;
    }





    /*
      Print out the map.
    */
    public void print() throws IOException {
        System.out.println("{row_lable:"+getRowLabel() + " ,column_label" + getColumnLabel() + " ,timestamp"
                + getTimeStamp() + " ,value:" + getValue()+"}");

    }


    /*
  Get the length of the tuple
*/
    public int size()
    {
        return (short) (this.fldOffset[fldCnt] - this.map_offset);
//        return map_length;
    }
    

    public void tupleCopy(Map fromTuple)
    {
        byte [] temparray = fromTuple.getMapByteArray();
        System.arraycopy(temparray, 0, this.map, this.map_offset, this.map_length);
//       fldCnt = fromTuple.noOfFlds();
//       fldOffset = fromTuple.copyFldOffset();
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
        System.arraycopy(frommap, offset, this.map, 0, this.map_length);
        // reseting map_offset to 0
        this.map_offset = 0;
        return this;
    }


    /*
      This is used when you don’t want to use the constructor.
    */
    public Map mapInit(byte[] amap, int offset) throws  IOException
    {
        this.map = amap;
        this.map_offset = offset;
        // TODO do we need to copy the map length
//        this.map_length = this.map.length;
//        initFieldOffset();
//        this.fldCnt = Convert.getShortValue(offset,this.map);
        return  this;
    }


    private void initFieldOffset() throws IOException {
        int ptr = this.map_offset +2;
        this.fldOffset = new short[MAX_FIELD_COUNT +1];

        for(int i=0;i<= MAX_FIELD_COUNT; i++){
            this.fldOffset[i] = Convert.getShortValue(ptr,this.map);
            ptr +=2;//short has size 2
        }
    }




    @Override
    public String toString() {
        String s="";
        try {
            s= "{row_lable:"+getRowLabel() + " ,column_label" + getColumnLabel() + " ,timestamp"
                    + getTimeStamp() + " ,value:" + getValue()+"}";
        } catch (IOException e) {
            e.printStackTrace();
        }
        return s;
    }

    public void setHdr (short numFlds,  AttrType types[], short strSizes[])
            throws IOException, InvalidTypeException, InvalidMapSizeException
    {
        if((numFlds +2)*2 > max_size)
            throw new InvalidMapSizeException(null, "TUPLE: TUPLE_TOOBIG_ERROR");

        fldCnt = numFlds;
        Convert.setShortValue(numFlds, map_offset, map);
        fldOffset = new short[numFlds+1];
        int pos = map_offset+2;  // start position for fldOffset[]

        //sizeof short =2  +2: array siaze = numFlds +1 (0 - numFilds) and
        //another 1 for fldCnt
        fldOffset[0] = (short) ((numFlds +2) * 2 + map_offset);

        Convert.setShortValue(fldOffset[0], pos, map);
        pos +=2;
        short strCount =0;
        short incr;
        int i;

        for (i=1; i<numFlds; i++)
        {
            switch(types[i-1].attrType) {

                case AttrType.attrInteger:
                    incr = 4;
                    break;

                case AttrType.attrReal:
                    incr =4;
                    break;

                case AttrType.attrString:
                    incr = (short) (strSizes[strCount] +2);  //strlen in bytes = strlen +2
                    strCount++;
                    break;

                default:
                    throw new InvalidTypeException (null, "TUPLE: TUPLE_TYPE_ERROR");
            }
            fldOffset[i]  = (short) (fldOffset[i-1] + incr);
            Convert.setShortValue(fldOffset[i], pos, map);
            pos +=2;

        }
        switch(types[numFlds -1].attrType) {

            case AttrType.attrInteger:
                incr = 4;
                break;

            case AttrType.attrReal:
                incr =4;
                break;

            case AttrType.attrString:
                incr =(short) ( strSizes[strCount] +2);  //strlen in bytes = strlen +2
                break;

            default:
                throw new InvalidTypeException (null, "TUPLE: TUPLE_TYPE_ERROR");
        }

        fldOffset[numFlds] = (short) (fldOffset[i-1] + incr);
        Convert.setShortValue(fldOffset[numFlds], pos, map);

        map_length = fldOffset[numFlds] - map_offset;

        if(map_length > max_size)
            throw new InvalidMapSizeException(null, "TUPLE: TUPLE_TOOBIG_ERROR");
    }

    /**
     * Convert this field into integer
     *
     * @param	fldNo	the field number
     * @return		the converted integer if success
     *
     * @exception   IOException I/O errors
     * @exception FieldNumberOutOfBoundException Tuple field number out of bound
     */

    public int getIntFld(int fldNo)
            throws IOException, FieldNumberOutOfBoundException
    {
        int val;
        if ( (fldNo > 0) && (fldNo <= fldCnt))
        {
            val = Convert.getIntValue(fldOffset[fldNo -1], map);
            return val;
        }
        else
            throw new FieldNumberOutOfBoundException (null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");
    }

    /**
     * Convert this field in to float
     *
     * @param    fldNo   the field number
     * @return           the converted float number  if success
     *
     * @exception   IOException I/O errors
     * @exception   FieldNumberOutOfBoundException Tuple field number out of bound
     */

    public float getFloFld(int fldNo)
            throws IOException, FieldNumberOutOfBoundException
    {
        float val;
        if ( (fldNo > 0) && (fldNo <= fldCnt))
        {
            val = Convert.getFloValue(fldOffset[fldNo -1], map);
            return val;
        }
        else
            throw new FieldNumberOutOfBoundException (null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");
    }


    /**
     * Convert this field into String
     *
     * @param    fldNo   the field number
     * @return           the converted string if success
     *
     * @exception   IOException I/O errors
     * @exception   FieldNumberOutOfBoundException Tuple field number out of bound
     */

    public String getStrFld(int fldNo)
            throws IOException, FieldNumberOutOfBoundException
    {
        String val;
        if ( (fldNo > 0) && (fldNo <= fldCnt))
        {
            val = Convert.getStrValue(fldOffset[fldNo -1], map,
                    fldOffset[fldNo] - fldOffset[fldNo -1]); //strlen+2
            return val;
        }
        else
            throw new FieldNumberOutOfBoundException (null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");
    }

    /**
     * Convert this field into a character
     *
     * @param    fldNo   the field number
     * @return           the character if success
     *
     * @exception   IOException I/O errors
     * @exception   FieldNumberOutOfBoundException Tuple field number out of bound
     */

    public char getCharFld(int fldNo)
            throws IOException, FieldNumberOutOfBoundException
    {
        char val;
        if ( (fldNo > 0) && (fldNo <= fldCnt))
        {
            val = Convert.getCharValue(fldOffset[fldNo -1], map);
            return val;
        }
        else
            throw new FieldNumberOutOfBoundException (null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");

    }

    /**
     * Set this field to integer value
     *
     * @param	fldNo	the field number
     * @param	val	the integer value
     * @exception   IOException I/O errors
     * @exception   FieldNumberOutOfBoundException Tuple field number out of bound
     */

    public Map setIntFld(int fldNo, int val)
            throws IOException, FieldNumberOutOfBoundException
    {
        if ( (fldNo > 0) && (fldNo <= fldCnt))
        {
            Convert.setIntValue (val, fldOffset[fldNo -1], map);
            return this;
        }
        else
            throw new FieldNumberOutOfBoundException (null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");
    }

    /**
     * Set this field to float value
     *
     * @param     fldNo   the field number
     * @param     val     the float value
     * @exception   IOException I/O errors
     * @exception   FieldNumberOutOfBoundException Tuple field number out of bound
     */

    public Map setFloFld(int fldNo, float val)
            throws IOException, FieldNumberOutOfBoundException
    {
        if ( (fldNo > 0) && (fldNo <= fldCnt))
        {
            Convert.setFloValue (val, fldOffset[fldNo -1], map);
            return this;
        }
        else
            throw new FieldNumberOutOfBoundException (null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");

    }

    /**
     * Set this field to String value
     *
     * @param     fldNo   the field number
     * @param     val     the string value
     * @exception   IOException I/O errors
     * @exception   FieldNumberOutOfBoundException Tuple field number out of bound
     */

    public Map setStrFld(int fldNo, String val)
            throws IOException, FieldNumberOutOfBoundException
    {
        if ( (fldNo > 0) && (fldNo <= fldCnt))
        {
            Convert.setStrValue (val, fldOffset[fldNo -1], map);
            return this;
        }
        else
            throw new FieldNumberOutOfBoundException (null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");
    }






} // end of Map