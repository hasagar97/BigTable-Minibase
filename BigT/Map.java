package BigT;

import java.io.*;
import java.lang.*;
import java.nio.charset.StandardCharsets;

import global.*;
import iterator.*;
import heap.*;

public class Map implements GlobalConst{
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
//    private int map_length;

    /**
     * private field
     * Number of fields in this tuple
     */

    public static short ROW_LABEL_SIZE = 20;
    public static short COLUMN_LABEL_SIZE = 20;
    public static short TIMESTAMP_LABEL_SIZE = 4;
    public static short VALUE_LABEL_SIZE = 20;


    public static int MAX_STR_SIZE = 20;


    public int[] fldOffset = {map_offset,map_offset+ROW_LABEL_SIZE,ROW_LABEL_SIZE+COLUMN_LABEL_SIZE, map_offset+ROW_LABEL_SIZE+COLUMN_LABEL_SIZE+TIMESTAMP_LABEL_SIZE};
    public int fldCnt = 4;

    /**
     * Maximum size of any tuple
     */
    public static final int max_size = ROW_LABEL_SIZE + COLUMN_LABEL_SIZE
            +TIMESTAMP_LABEL_SIZE + VALUE_LABEL_SIZE;


    /*
      Class constructor create a new map with the appropriate size.
    */
    public Map()
    {
        this.map = new byte[max_size];
    }

    /*
      Construct a map from a byte array.
    */
    public Map(byte[] amap, int offset)
    {
        this.map = new byte[max_size];
        System.arraycopy(amap, offset, this.map, 0, amap.length - offset);
        this.map_offset = 0;
        // this.map = amap;
        // this.map_offset = offset;
    }

    /*
      Construct a map from another map through copy.
    */
    public Map(Map fromMap)
    {
        this.map = new byte[max_size];
        System.arraycopy(fromMap.map, fromMap.map_offset, this.map, 0, fromMap.max_size);
        this.map_offset = 0;
    }

    /*
      Construct a map from another map through copy with given offset and len.
    */
    public Map(byte [] fromMap,int offset,int size) {
        this.map = fromMap;
        this.map_offset = offset;
    }



    /*
      Returns the row label.
    */
    public java.lang.String getRowLabel() throws IOException {
        return Convert.getMapStrValue(map_offset,map,ROW_LABEL_SIZE);
//        return "";
    }

    /*
      Returns the column label.
    */
    public java.lang.String getColumnLabel() throws IOException {
        return Convert.getMapStrValue(map_offset+ROW_LABEL_SIZE,map,COLUMN_LABEL_SIZE);
//        return "";
    }

    /*
      Returns the timestamp.
    */
    public int getTimeStamp() throws IOException {
        return Convert.getIntValue(map_offset+ROW_LABEL_SIZE+COLUMN_LABEL_SIZE,map);
    }


    /*
      Class constructor create a new map with the appropriate size.
    */
    public Map(int size)
    {
        // ignoring the size provided and creating a map with fixed size
        this.map = new byte[max_size];
//        //System.out.println("Map(size)");
//        this.map = new byte[size];
//        this.map_offset = 0;
//        this.map_length = this.map.length;
//        this.fldCnt=4;
    }


    public int getLength(){
        return max_size;
    }

    /*
      Returns the value.
    */
    public java.lang.String getValue() throws IOException {
        return Convert.getMapStrValue( map_offset+ROW_LABEL_SIZE+COLUMN_LABEL_SIZE+TIMESTAMP_LABEL_SIZE,map,VALUE_LABEL_SIZE);
    }

    /*
      Set the row label.
    */
    public Map setRowLabel(java.lang.String val) throws IOException, InvalidFieldSize {
        if(val.length() > ROW_LABEL_SIZE)
            throw new InvalidFieldSize(null,"MAP: INVALID FIELD SIZE, expected maximum of "
                    +ROW_LABEL_SIZE+" , recieved"+val.length());
        Convert.setStrValue(val, map_offset, map);
        return this;
    }

    /*
      Set the column label.
    */
    public Map setColumnLabel(java.lang.String val) throws IOException, InvalidFieldSize {
        if(val.length() > COLUMN_LABEL_SIZE)
            throw new InvalidFieldSize(null,"MAP: INVALID FIELD SIZE, expected maximum of "
                    +COLUMN_LABEL_SIZE+" , recieved"+val.length());
        Convert.setStrValue(val, map_offset+ROW_LABEL_SIZE, map);
        return this;
    }

    /*
      Set the timestamp.
    */
    public Map setTimeStamp(int val) throws IOException {
        Convert.setIntValue(val, map_offset+ROW_LABEL_SIZE+COLUMN_LABEL_SIZE, map);
        return this;
    }

    /*
      Set the value.
    */
    public Map setValue(java.lang.String val) throws IOException, InvalidFieldSize {
        if(val.length() > VALUE_LABEL_SIZE)
            throw new InvalidFieldSize(null,"MAP: INVALID FIELD SIZE, expected maximum of "
                    +VALUE_LABEL_SIZE+" , recieved"+val.length());
        Convert.setStrValue(val, map_offset+ROW_LABEL_SIZE+COLUMN_LABEL_SIZE+TIMESTAMP_LABEL_SIZE, map);
        return this;
    }

    /*
      Copy the map to byte array out.
    */
    public byte[] getMapByteArray()
    {
        byte [] mapcopy = new byte [max_size];
        System.arraycopy(this.map, this.map_offset, mapcopy, 0, this.max_size);
        return mapcopy;
    }


    public byte [] returnMapByteArray()
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

    /*
      Print out the map.
    */
    public void print() throws IOException {
        System.out.println("{ row_label: "+getRowLabel() + ", column_label: " + getColumnLabel() + ", timestamp: "
                + getTimeStamp() + ", value: " + getValue()+" }");
    }

    @Override
    public String toString() {
      try {
        return "{ row_label: "+getRowLabel() + ", column_label: " + getColumnLabel() + ", timestamp: "
        + getTimeStamp() + ", value: " + getValue()+" }";
      }
      catch(Exception E){
        System.out.println("Exception in Map.toString");
        E.printStackTrace();
      }
      return "";
    }

    /*
      Get the length of the tuple
    */
    public int size()
    {
        return max_size;
    }

    /*
      Copy the given map
    */
    public Map mapCopy(Map fromMap)
    {
        byte [] temparray = fromMap.getMapByteArray();
        System.arraycopy(temparray, 0, this.map, this.map_offset, max_size);
        return this;
    }

    /*
      This is used when you don’t want to use the constructor.
    */
    public Map mapInit(byte[] amap, int offset)
    {
        this.map = amap;
        this.map_offset = offset;
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
        System.arraycopy(frommap, offset, this.map, 0, frommap.length - offset);
        // reseting map_offset to 0
        this.map_offset = 0;
        return this;
    }


    /*
      Set a map with the given byte array and offset and length.
      After creating this map construct, remove the heap.
      Tuple completely from the minibase and modify all
      related code to use BigT.Map instead; that is, your
      version of minibase will not store tuples anymore.
    */
    public Map mapSet(byte[] frommap, int offset,int size)
    {
        System.arraycopy(frommap, offset, this.map, 0, Math.min(max_size,frommap.length-offset));
        // reseting map_offset to 0
        this.map_offset = 0;
        return this;
    }



    /**
     * Convert this field into integer
     *
     * @param fldNo the field number
     * @return    the converted integer if success
     *
     * @exception   IOException I/O errors
     * @exception FieldNumberOutOfBoundException Tuple field number out of bound
     */

    public int getIntFld(int fldNo)
            throws IOException, FieldNumberOutOfBoundException
    {
//        //System.out.println("getIntFld(int fldNo)");
//        int val;
//        if ( (fldNo > 0) && (fldNo <= fldCnt))
//        {
//            val = Convert.getIntValue(fldOffset[fldNo -1], map);
//            return val;
//        }
//        else
//            throw new FieldNumberOutOfBoundException (null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");
        return 0;
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
       //System.out.println("getFloFld(int fldNo)");
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
//       System.out.println("getStrFld(int fldNo) "+fldNo);
//       System.out.println("first field offset "+fldOffset[fldNo -1]+" map"+ map +" size  "+ (fldOffset[fldNo] - fldOffset[fldNo -1]) + " map length " +map.length);
       String val;
       if ( (fldNo > 0) && (fldNo <= fldCnt))
       {
           int length = VALUE_LABEL_SIZE;
           if(fldNo !=fldCnt) length = fldOffset[fldNo] - fldOffset[fldNo -1];
           val = Convert.getMapStrValue(fldOffset[fldNo -1], map,
                   length); //strlen+2
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
       //System.out.println("getCharFld(int fldNo)");
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
     * @param fldNo the field number
     * @param val the integer value
     * @exception   IOException I/O errors
     * @exception   FieldNumberOutOfBoundException Tuple field number out of bound
     */

    public Map setIntFld(int fldNo, int val)
            throws IOException, FieldNumberOutOfBoundException
    {
       //System.out.println("setIntFld(int fldNo, int val)");
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
       //System.out.println("setFloFld(int fldNo, float val)");
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
//        System.out.println("\t setStrFld(int fldNo, String val) fldno :"+fldNo+ " val:" + val);
//       System.out.println("\t Map size:"+map.length+" offset : "+map_offset+ " field offset"+ fldOffset[fldNo -1]);
       if ( (fldNo > 0) && (fldNo <= fldCnt))
       {
           Convert.setStrValue (val, fldOffset[fldNo -1], map);
//           System.out.print("Map after setting map value:");
          //  this.print();
           return this;
       }
       else
           throw new FieldNumberOutOfBoundException (null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");
    }


    public void setHdr (short numFlds,  AttrType types[], short strSizes[])
            throws IOException, InvalidTypeException, InvalidMapSizeException
    {
//        //System.out.println("setHdr (short numFlds,  AttrType types[], short strSizes[])");
//        if((numFlds +2)*2 > max_size)
//            throw new InvalidMapSizeException(null, "TUPLE: TUPLE_TOOBIG_ERROR");
//
//        fldCnt = numFlds;
//        Convert.setShortValue(numFlds, map_offset, map);
//        fldOffset = new short[numFlds+1];
//        int pos = map_offset+2;  // start position for fldOffset[]
//
//        //sizeof short =2  +2: array siaze = numFlds +1 (0 - numFilds) and
//        //another 1 for fldCnt
//        fldOffset[0] = (short) ((numFlds +2) * 2 + map_offset);
//
//        Convert.setShortValue(fldOffset[0], pos, map);
//        pos +=2;
//        short strCount =0;
//        short incr;
//        int i;
//
//        for (i=1; i<numFlds; i++)
//        {
//            switch(types[i-1].attrType) {
//
//                case AttrType.attrInteger:
//                    incr = 4;
//                    break;
//
//                case AttrType.attrReal:
//                    incr =4;
//                    break;
//
//                case AttrType.attrString:
//                    incr = (short) (strSizes[strCount] +2);  //strlen in bytes = strlen +2
//                    strCount++;
//                    break;
//
//                default:
//                    throw new InvalidTypeException (null, "TUPLE: TUPLE_TYPE_ERROR");
//            }
//            fldOffset[i]  = (short) (fldOffset[i-1] + incr);
//            Convert.setShortValue(fldOffset[i], pos, map);
//            pos +=2;
//
//        }
//        switch(types[numFlds -1].attrType) {
//
//            case AttrType.attrInteger:
//                incr = 4;
//                break;
//
//            case AttrType.attrReal:
//                incr =4;
//                break;
//
//            case AttrType.attrString:
//                incr =(short) ( strSizes[strCount] +2);  //strlen in bytes = strlen +2
//                break;
//
//            default:
//                throw new InvalidTypeException (null, "TUPLE: TUPLE_TYPE_ERROR");
//        }
//
//        fldOffset[numFlds] = (short) (fldOffset[i-1] + incr);
//        Convert.setShortValue(fldOffset[numFlds], pos, map);
//
//        map_length = fldOffset[numFlds] - map_offset;
//
//        if(map_length > max_size)
//            throw new InvalidMapSizeException(null, "TUPLE: TUPLE_TOOBIG_ERROR");
        return;
    }




} // end of Map
