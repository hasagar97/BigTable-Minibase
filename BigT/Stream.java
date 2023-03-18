package BigT;

import java.io.*;
import java.lang.*;
import global.*;
import BigT.*;
import java.util.Map;
import global.AttrOperator;

public class Stream {
  /*
    Initialize a stream of maps on bigtable.
  */
  private BigTable bigTable;
  private int orderType;
  private List<CondExpr> filters;
  private boolean isOpen;
  private Sort sortedStream;
  private Scan scan;
  CondExpr[] condExprs;

  public Stream(bigt bigTable, int orderType, 
  		java.lang.String rowFilter, 
  		java.lang.String columnFilter, 
  		java.lang.String valueFilter)
  {

    this.bigTable = bigTable;
    this.orderType = orderType;

    CondExpr rowFilter = processFilter(rowFilter, 1);
    CondExpr columnFilter = processFilter(columnFilter, 2);
    CondExpr valueFilter = processFilter(valueFilter, 4);

    this.filters.add(rowFilter);
    this.filters.add(columnFilter);
    this.filters.add(valueFilter);

    this.isOpen = true;
    try {
      FileScanMap iterator = new FileScan(bigTable.getName(), null, filters);
      this.sortedStream = new Sort(null, null, null, iterator, this.orderType, new TupleOrder(TupleOrder.Ascending), null);
    } catch (Exception e) {
        System.err.println("*** Error opening scan ***");
        e.printStackTrace();
    }
  }
  
  /*
    Closes the stream object.
  */
  private List<CondExpr> processFilter(filter, fldNum) {
    str = str.replaceAll("\\[|\\]", ""); // remove square brackets
    String[] parts = str.split(","); // split into two parts
    List<CondExpr> result = new ArrayList<CondExpr>();

    if (parts.length() == 2) {
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
    else if (parts.length() == 1) {
      String pattern = Integer.parseInt(parts[0].trim());
      if (pattern == "*") {
        // return [];
      } else {
        System.err.println("*** Error in one of the filters, no filtering applied***");
      }
    }
    else {
      System.err.println("*** Error in one of the filters, no filtering applied***");
    }
    return result
  }

  public void closestream()
  {
    if (this.isOpen) {
        try {
            this.scan.closescan();
            this.isOpen = false;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
  }
  
  /*
    Retrieve the next map in the stream.
  */
  public Map getNext(MID mid)
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
