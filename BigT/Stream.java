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
  private BigTable bigTable;
  private int orderType;
  private List<CondExpr> filters;
  private boolean isOpen;
  private Sort sortedStream;

  public Stream(BigTable bigTable, int orderType, 
  		java.lang.String rowFilter, 
  		java.lang.String columnFilter, 
  		java.lang.String valueFilter)
  {

    this.bigTable = bigTable;
    this.orderType = orderType;

    List<CondExpr> row = this.processFilter(rowFilter, 1);
    List<CondExpr> column = this.processFilter(columnFilter, 2);
    List<CondExpr> value = this.processFilter(valueFilter, 4);

    this.filters.addAll(row);
    this.filters.addAll(column);
    this.filters.addAll(value);

    this.isOpen = true;
    try {
      AttrType attrtype = new AttrType(0);
      AttrType[] attrtypearr = {attrtype};
      short s1_sizes = 2048; 
      short len_in = 2048;              
      int n_out_flds = 1;
      FldSpec[] proj_list = null;
      FileScan iterator = new FileScan(bigTable.getName(), attrtypearr, s1_sizes, len_in, n_out_flds, null, filters);

      short [] str_sizes = {2048};
      this.sortedStream = new Sort(attrtypearr,len_in,str_sizes,iterator, this.orderType, new TupleOrder(0), len_in, 10);
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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
  }
  
  /*
    Retrieve the next map in the stream.
  */
  public BigT.Map getNext(MID mid)
  {
    if (!this.isOpen) {
      System.err.println("*** Error: Stream is closed ***");
      return null;
    }
    BigT.Map nextMapItem = this.sortedStream.get_next();
    if (nextMapItem == null) {
      System.out.println("*** All items from DB fetched ***");
    }
    return nextMapItem;
  }
} // end of Stream
