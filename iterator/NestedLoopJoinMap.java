package iterator;

import BigT.*;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.PinPageException;
import heap.*;
import global.*;
import bufmgr.*;
import index.*;
import java.lang.*;
import java.io.*;
/**
 *
 *  This file contains an implementation of the nested loops join
 *  algorithm as described in the Shapiro paper.
 *  The algorithm is extremely simple:
 *
 *      foreach tuple r in R do
 *          foreach tuple s in S do
 *              if (ri == sj) then add (r, s) to the result.
 */

public class NestedLoopJoinMap extends Iterator
{
    private AttrType      _in1[],  _in2[];
    private   int        in1_len, in2_len;
    private Stream outer;
    private   short t2_str_sizescopy[];
    private   CondExpr OutputFilter[];
    private   CondExpr RightFilter[];
    private   int        n_buf_pgs;        // # of buffer pages available.
    private   boolean        done,         // Is the join complete
            get_from_outer;                 // if TRUE, a tuple is got from outer
    private Map outer_tuple, inner_tuple;
    private Map Jtuple;           // Joined tuple
    private   FldSpec   perm_mat[];
    private   int        nOutFlds;
    private   Heapfile  hf;
    private BigTScan inner;
    private BigT rightTable;


    /**constructor
     *Initialize the two relations which are joined, including relation type,
     *@exception IOException some I/O fault
     *@exception NestedLoopException exception from this class
     * @param in1  Array containing field types of R.
     * @param len_in1  # of columns in R.
     * @param t1_str_sizes shows the length of the string fields.
     * @param in2  Array containing field types of S
     * @param len_in2  # of columns in S
     * @param  t2_str_sizes shows the length of the string fields.
     * @param amt_of_mem  IN PAGES
     * @param am1  access method for left i/p to join
     * @param relationName  access hfapfile for right i/p to join
     * @param outFilter   select expressions
     * @param rightFilter reference to filter applied on right i/p
     * @param proj_list shows what input fields go where in the output tuple
     * @param n_out_flds number of outer relation fileds
     */
    public NestedLoopJoinMap(AttrType[] in1,
                             int     len_in1,
                             short[] t1_str_sizes,
                             AttrType[] in2,
                             int     len_in2,
                             short[] t2_str_sizes,
                             int     amt_of_mem,
                             Stream am1,
                             String relationName,
                             CondExpr[] outFilter,
                             CondExpr[] rightFilter,
                             FldSpec[] proj_list,
                             int        n_out_flds
    ) throws IOException, NestedLoopException, InvalidMapSizeException {

        _in1 = new AttrType[in1.length];
        _in2 = new AttrType[in2.length];
        System.arraycopy(in1,0,_in1,0,in1.length);
        System.arraycopy(in2,0,_in2,0,in2.length);
        in1_len = len_in1;
        in2_len = len_in2;


        outer = am1;
        boolean print_inputStream_in_nestedjoin=false;
                if(print_inputStream_in_nestedjoin){
                Stream xx = outer;
                Map t = null;
                int printcount=0;
                try {
                    while ((t = xx.getNext(new RID())) != null) {
                //    t.print(JJtype);
                t.print();
                printcount++;
                //    qcheck2.Check(t);
                    }
                }
                catch (Exception e) {
                System.err.println ("*** Error printing the first stream");
                System.err.println (""+e);
                Runtime.getRuntime().exit(1);
            }

            System.out.println(":Printed values from input stream:"+printcount);
            }
        t2_str_sizescopy =  t2_str_sizes;
        inner_tuple = new Map();
        Jtuple = new Map();
        OutputFilter = outFilter;
        RightFilter  = rightFilter;

        n_buf_pgs    = amt_of_mem;
        inner = null;
        done  = false;
        get_from_outer = true;

        AttrType[] Jtypes = new AttrType[n_out_flds];
        short[]    t_size;

        perm_mat = proj_list;
        nOutFlds = n_out_flds;
        try {
            t_size = MapUtils.setup_op_tuple(Jtuple, Jtypes,
                    in1, len_in1, in2, len_in2,
                    t1_str_sizes, t2_str_sizes,
                    proj_list, nOutFlds);
        }catch (TupleUtilsException e){
            throw new NestedLoopException(e,"TupleUtilsException is caught by NestedLoopsJoins.java");
        }



        try {
            hf = new Heapfile(relationName);
            rightTable = new BigT(relationName);

        }
        catch(Exception e) {
            throw new NestedLoopException(e, "Create new heapfile failed.");
        }
    }

    /**
     *@return The joined tuple is returned
     *@exception IOException I/O errors
     *@exception JoinsException some join exception
     *@exception IndexException exception from super class
     *@exception InvalidMapSizeException invalid tuple size
     *@exception InvalidTypeException tuple type not valid
     *@exception PageNotReadException exception from lower layer
     *@exception TupleUtilsException exception from using tuple utilities
     *@exception PredEvalException exception from PredEval class
     *@exception SortException sort exception
     *@exception LowMemException memory error
     *@exception UnknowAttrType attribute type unknown
     *@exception UnknownKeyTypeException key type unknown
     *@exception Exception other exceptions

     */
    public Map get_next()
            throws IOException,
            JoinsException ,
            IndexException,
            InvalidMapSizeException,
            InvalidTypeException,
            PageNotReadException,
            TupleUtilsException,
            PredEvalException,
            SortException,
            LowMemException,
            UnknowAttrType,
            UnknownKeyTypeException,
            Exception
    {
        // This is a DUMBEST form of a join, not making use of any key information...


        if (done)
            return null;

        do
        {
            // If get_from_outer is true, Get a tuple from the outer, delete
            // an existing scan on the file, and reopen a new scan on the file.
            // If a get_next on the outer returns DONE?, then the nested loops
            //join is done too.

            if (get_from_outer == true)
            {
                get_from_outer = false;
                if (inner != null)     // If this not the first time,
                {
                    // close scan
                    inner = null;
                }

                try {
//                    inner = hf.openScan();
                    inner = rightTable.openScan();
                }
                catch(Exception e){
                    throw new NestedLoopException(e, "openScan failed");
                }

                if ((outer_tuple=outer.getNext(new RID())) == null)
                {
                    done = true;
                    if (inner != null)
                    {

                        inner = null;
                    }

                    return null;
                }
            }  // ENDS: if (get_from_outer == TRUE)


            // The next step is to get a tuple from the inner,
            // while the inner is not completely scanned && there
            // is no match (with pred),get a tuple from the inner.


            RID rid = new RID();
            while ((inner_tuple = inner.getNext(rid)) != null)
            {
                inner_tuple.setHdr((short)in2_len, _in2,t2_str_sizescopy);
                // System.out.println("NestedLoopjoinMap: inside join loop");
                if (PredEval.Eval(RightFilter, inner_tuple, null, _in2, null) == true)
                {
                    if (PredEval.Eval(OutputFilter, outer_tuple, inner_tuple, _in1, _in2) == true)
                    {
                        System.out.println("match found for: "+inner_tuple.toString()+"  AND " + outer_tuple.toString());
                        // Apply a projection on the outer and inner tuples.
                        Projection.Join(outer_tuple, _in1,
                                inner_tuple, _in2,
                                Jtuple, perm_mat, nOutFlds);
                        return Jtuple;
                    }
                }
            }

            // There has been no match. (otherwise, we would have
            //returned from t//he while loop. Hence, inner is
            //exhausted, => set get_from_outer = TRUE, go to top of loop

            get_from_outer = true; // Loop back to top and get next outer tuple.
        } while (true);
    }

    public Stream nestedRowJoin(Stream left, Stream right) throws InvalidMapSizeException, IOException, ConstructPageException, HFDiskMgrException, HFException, GetFileEntryException, HFBufMgrException, PinPageException, InvalidFieldSize, SpaceNotAvailableException, InvalidSlotNumberException {
        BigT recentValueTable = new BigT("nestedJoinTable1.in");
        Map outputMap = new Map();
        Map leftMap = null;
        Map rightMap = null;
        while((leftMap = left.getNext(new RID()))!=null){
            while((rightMap = right.getNext(new RID()))!=null){
                if (leftMap.getValue() == rightMap.getValue() && leftMap.getColumnLabel().equalsIgnoreCase(rightMap.getColumnLabel())){
                        outputMap.setRowLabel(leftMap.getRowLabel()+":"+rightMap.getRowLabel());
                        outputMap.setColumnLabel(leftMap.getColumnLabel());
                        outputMap.setTimeStamp(0);
                        outputMap.setValue(leftMap.getValue());
                        recentValueTable.insertMap(outputMap.returnMapByteArray(), 1);
                        break;
                }else if(leftMap.getValue() == rightMap.getValue() && !leftMap.getColumnLabel().equalsIgnoreCase(rightMap.getColumnLabel())){
                    outputMap.setRowLabel(leftMap.getRowLabel()+":"+rightMap.getRowLabel());
                    outputMap.setColumnLabel(leftMap.getColumnLabel());
                    outputMap.setTimeStamp(leftMap.getTimeStamp());
                    outputMap.setValue(leftMap.getValue());
                    recentValueTable.insertMap(outputMap.returnMapByteArray(), 1);
                    outputMap.setColumnLabel(rightMap.getColumnLabel());
                    recentValueTable.insertMap(outputMap.returnMapByteArray(), 1);
                    break;
                }
            }
        }
        return new Stream(recentValueTable, 6, "*", "*", "*");
    }

    /**
     * implement the abstract method close() from super class Iterator
     *to finish cleaning up
     *@exception IOException I/O error from lower layers
     *@exception JoinsException join error from lower layers
     *@exception IndexException index access error
     */
    public void close() throws JoinsException, IOException,IndexException
    {
        if (!closeFlag) {

            try {
                System.out.println(outer);
                outer.close();
            }catch (Exception e) {
                throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
            }
            closeFlag = true;
        }
    }
}






