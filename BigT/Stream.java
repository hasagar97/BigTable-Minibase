package BigT;

import java.io.*;
import java.util.List;
import java.util.ArrayList;
import global.*;
import BigT.*;
import java.util.Map;

import iterator.FileScan;
import iterator.CondExpr;
import iterator.Sort;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.Iterator;

public class Stream {
  /*
    Initialize a stream of maps on bigtable.
  */
  private String bigTable;
  private int orderType;
  private CondExpr[] filters = new CondExpr[100];
  private boolean isOpen;
  private Sort sortedStream;

  public Stream(String bigTable, int orderType, 
  		java.lang.String rowFilter, 
  		java.lang.String columnFilter, 
  		java.lang.String valueFilter)
  {

    this.bigTable = bigTable;
    this.orderType = orderType;

    List<CondExpr> filters = new ArrayList<CondExpr>();
    List<CondExpr> row = this.processFilter(rowFilter, 1);
    List<CondExpr> column = this.processFilter(columnFilter, 2);
    List<CondExpr> value = this.processFilter(valueFilter, 4);

    filters.addAll(row);
    filters.addAll(column);
    filters.addAll(value);

    FldSpec[] projection = new FldSpec[4];
    RelSpec rel = new RelSpec(RelSpec.outer);
    projection[0] = new FldSpec(rel, 1);
    projection[1] = new FldSpec(rel, 2);
    projection[2] = new FldSpec(rel, 3);
    projection[3] = new FldSpec(rel, 4);

    this.isOpen = true;
    try {
      AttrType[] attrTypes = new AttrType[]{new AttrType(0), new AttrType(0), new AttrType(1), new AttrType(0)};
      short len_in = 4;      
      short [] str_sizes = {(short)25, (short)25, (short)25};
      int n_out_flds = 4;
      FldSpec[] proj_list = null;
      this.filters = filters.toArray(new CondExpr[100]);
      
      FileScan iterator = new FileScan(this.bigTable, attrTypes, str_sizes, len_in, n_out_flds, projection, this.filters);

      this.sortedStream = new Sort(attrTypes, len_in, str_sizes, iterator, this.orderType, new TupleOrder(TupleOrder.Ascending), len_in, 10);
    } catch (Exception e) {
        System.err.println("*** Error opening scan ***");
        e.printStackTrace();
    }
  }
  
  /*
    Closes the stream object.
  */
  private List<CondExpr> processFilter(String filter, int fldNum) {
    String str = filter.replaceAll("\\[|\\]", ""); // remove square brackets
    String[] parts = str.split(","); // split into two parts
    List<CondExpr> result = new ArrayList<CondExpr>();

    if (parts.length == 2) {
      String num1 = parts[0].trim();
      String num2 = parts[1].trim();
      CondExpr expr1 = new CondExpr();
      expr1.op = new AttrOperator(AttrOperator.aopGE);
      expr1.type1 = new AttrType(AttrType.attrSymbol);
      expr1.type2 = new AttrType(AttrType.attrString);
      expr1.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fldNum);
      expr1.operand2.string = num1;
      expr1.next = null;
      CondExpr expr2 = new CondExpr();
      expr2.op = new AttrOperator(AttrOperator.aopLE);
      expr2.type1 = new AttrType(AttrType.attrSymbol);
      expr2.type2 = new AttrType(AttrType.attrString);
      expr2.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fldNum);
      expr2.operand2.string = num2;
      expr2.next = null;
      result.add(expr1);
      result.add(expr2);
    }
    else if (parts.length == 1) {
      String pattern = parts[0].trim();
      if (pattern == "*") {
        // return [];
      } else {
        System.err.println("*** Error in one of the filters, no filtering applied***");
      }
    }
    else {
      System.err.println("*** Error in one of the filters, no filtering applied***");
    }
    return result;
  }

  public void closestream()
  {
    if (this.isOpen) {
        try {
            this.sortedStream.close();
            this.isOpen = false;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
  }
  
  /*
    Retrieve the next map in the stream.
  */
  public BigT.Map getNext()
  {
    if (!this.isOpen || this.sortedStream == null) {
      System.err.println("*** Error: Stream is closed ***");
      return null;
    }
    try {
      BigT.Map nextMapItem = this.sortedStream.get_next();
      if (nextMapItem == null) {
        System.out.println("*** All items from DB fetched ***");
      }
      return nextMapItem;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }
} // end of Stream
