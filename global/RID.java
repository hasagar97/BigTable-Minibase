/*  File RID.java   */

package global;

import java.io.*;

/** class RID
 */

public class RID{
  
  /** public int slotNo
   */
  public int slotNo;
  
  /** public PageId pageNo
   */
  public PageId pageNo = new PageId();
  
  /** public int heapIndex
   */
  public int heapIndex;
  
  /**
   * default constructor of class
   */
  public RID () { }
  
  /**
   *  constructor of class
   */
  public RID (PageId pageno, int slotno, int heapindex)
    {
      pageNo = pageno;
      slotNo = slotno;
      heapIndex = heapindex;
    }
  
  /**
   * make a copy of the given rid
   */
  public void copyRid (RID rid)
    {
      pageNo = rid.pageNo;
      slotNo = rid.slotNo;
      heapIndex = rid.heapIndex;
    }
  
  /** Write the rid into a byte array at offset
   * @param ary the specified byte array
   * @param offset the offset of byte array to write 
   * @exception java.io.IOException I/O errors
   */ 
  public void writeToByteArray(byte [] ary, int offset)
    throws java.io.IOException
    {
      Convert.setIntValue ( slotNo, offset, ary);
      Convert.setIntValue ( pageNo.pid, offset+4, ary);
      Convert.setIntValue ( heapIndex, offset+8, ary);
    }
  
  
  /** Compares two RID object, i.e, this to the rid
   * @param rid RID object to be compared to
   * @return true is they are equal
   *         false if not.
   */
  public boolean equals(RID rid) {
    
    if ((this.pageNo.pid==rid.pageNo.pid)
	&&(this.slotNo==rid.slotNo)
	&&(this.heapIndex==rid.heapIndex))
      return true;
    else
      return false;
  }
  
}
