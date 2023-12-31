//package tests;
////originally from : joins.C
//
//import BigT.Map;
//import iterator.*;
//import heap.*;
//import global.*;
//
//import java.io.*;
//import java.util.*;
//import java.lang.*;
//
///**
//   Here is the implementation for the tests. There are N tests performed.
//   We start off by showing that each operator works on its own.
//   Then more complicated trees are constructed.
//   As a nice feature, we allow the user to specify a selection condition.
//   We also allow the user to hardwire trees together.
//*/
//
//////Define the SM_Sailor schema
//class SM_Sailor {
//  public int    sid;
//  public String sname;
//  public int    rating;
//  public double age;
//
//  public SM_Sailor (int _sid, String _sname, int _rating,double _age) {
//    sid    = _sid;
//    sname  = _sname;
//    rating = _rating;
//    age    = _age;
//  }
//}
//
////Define the Boat schema
//class SM_Boats {
//  public int    bid;
//  public String bname;
//  public String color;
//
//
//  public SM_Boats (int _bid, String _bname, String _color) {
//    bid   = _bid;
//    bname = _bname;
//    color = _color;
//  }
//}
//
////Define the SM_Reserves schema
//class SM_Reserves {
//  public int    sid;
//  public int    bid;
//  public String date;
//
//  public SM_Reserves (int _sid, int _bid, String _date) {
//    sid  = _sid;
//    bid  = _bid;
//    date = _date;
//  }
//}
//
//class SM_JoinsDriver implements GlobalConst {
//
//  private boolean OK = true;
//  private boolean FAIL = false;
//  private Vector SM_Sailors;
//  private Vector SM_Boats;
//  private Vector SM_Reserves;
//  /** Constructor
//   */
//  public SM_JoinsDriver() {
//
//    //build SM_Sailor, SM_Boats, SM_Reserves table
//    SM_Sailors  = new Vector();
//    SM_Boats    = new Vector();
//    SM_Reserves = new Vector();
//
//    SM_Sailors.addElement(new SM_Sailor(53, "Bob Holloway",       9, 53.6));
//    SM_Sailors.addElement(new SM_Sailor(54, "Susan Horowitz",     1, 34.2));
//    SM_Sailors.addElement(new SM_Sailor(57, "Yannis Ioannidis",   8, 40.2));
//    SM_Sailors.addElement(new SM_Sailor(59, "Deborah Joseph",    10, 39.8));
//    SM_Sailors.addElement(new SM_Sailor(61, "Landwebber",         8, 56.7));
//    SM_Sailors.addElement(new SM_Sailor(63, "James Larus",        9, 30.3));
//    SM_Sailors.addElement(new SM_Sailor(64, "Barton Miller",      5, 43.7));
//    SM_Sailors.addElement(new SM_Sailor(67, "David Parter",       1, 99.9));
//    SM_Sailors.addElement(new SM_Sailor(69, "Raghu Ramakrishnan", 9, 37.1));
//    SM_Sailors.addElement(new SM_Sailor(71, "Guri Sohi",         10, 42.1));
//    SM_Sailors.addElement(new SM_Sailor(73, "Prasoon Tiwari",     8, 39.2));
//    SM_Sailors.addElement(new SM_Sailor(39, "Anne Condon",        3, 30.3));
//    SM_Sailors.addElement(new SM_Sailor(47, "Charles Fischer",    6, 46.3));
//    SM_Sailors.addElement(new SM_Sailor(49, "James Goodman",      4, 50.3));
//    SM_Sailors.addElement(new SM_Sailor(50, "Mark Hill",          5, 35.2));
//    SM_Sailors.addElement(new SM_Sailor(75, "Mary Vernon",        7, 43.1));
//    SM_Sailors.addElement(new SM_Sailor(79, "David Wood",         3, 39.2));
//    SM_Sailors.addElement(new SM_Sailor(84, "Mark Smucker",       9, 25.3));
//    SM_Sailors.addElement(new SM_Sailor(87, "Martin Reames",     10, 24.1));
//    SM_Sailors.addElement(new SM_Sailor(10, "Mike Carey",         9, 40.3));
//    SM_Sailors.addElement(new SM_Sailor(21, "David Dewitt",      10, 47.2));
//    SM_Sailors.addElement(new SM_Sailor(29, "Tom Reps",           7, 39.1));
//    SM_Sailors.addElement(new SM_Sailor(31, "Jeff Naughton",      5, 35.0));
//    SM_Sailors.addElement(new SM_Sailor(35, "Miron Livny",        7, 37.6));
//    SM_Sailors.addElement(new SM_Sailor(37, "Marv Solomon",      10, 48.9));
//
//    SM_Boats.addElement(new SM_Boats(1, "Onion",      "white"));
//    SM_Boats.addElement(new SM_Boats(2, "Buckey",     "red"  ));
//    SM_Boats.addElement(new SM_Boats(3, "Enterprise", "blue" ));
//    SM_Boats.addElement(new SM_Boats(4, "Voyager",    "green"));
//    SM_Boats.addElement(new SM_Boats(5, "Wisconsin",  "red"  ));
//
//    SM_Reserves.addElement(new SM_Reserves(10, 1, "05/10/95"));
//    SM_Reserves.addElement(new SM_Reserves(21, 1, "05/11/95"));
//    SM_Reserves.addElement(new SM_Reserves(10, 2, "05/11/95"));
//    SM_Reserves.addElement(new SM_Reserves(31, 1, "05/12/95"));
//    SM_Reserves.addElement(new SM_Reserves(10, 3, "05/13/95"));
//    SM_Reserves.addElement(new SM_Reserves(69, 4, "05/12/95"));
//    SM_Reserves.addElement(new SM_Reserves(69, 5, "05/14/95"));
//    SM_Reserves.addElement(new SM_Reserves(21, 5, "05/16/95"));
//    SM_Reserves.addElement(new SM_Reserves(57, 2, "05/10/95"));
//    SM_Reserves.addElement(new SM_Reserves(35, 3, "05/15/95"));
//
//    boolean status = OK;
//    int numSM_Sailors = 25;
//    int numSM_Sailors_attrs = 4;
//    int numSM_Reserves = 10;
//    int numSM_Reserves_attrs = 3;
//    int numSM_Boats = 5;
//    int numSM_Boats_attrs = 3;
//
//    String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb";
//    String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";
//
//    String remove_cmd = "/bin/rm -rf ";
//    String remove_logcmd = remove_cmd + logpath;
//    String remove_dbcmd = remove_cmd + dbpath;
//    String remove_joincmd = remove_cmd + dbpath;
//
//    try {
//      Runtime.getRuntime().exec(remove_logcmd);
//      Runtime.getRuntime().exec(remove_dbcmd);
//      Runtime.getRuntime().exec(remove_joincmd);
//    }
//    catch (IOException e) {
//      System.err.println (""+e);
//    }
//
//
//    /*
//    ExtendedSystemDefs extSysDef =
//      new ExtendedSystemDefs( "/tmp/minibase.jointestdb", "/tmp/joinlog",
//			      1000,500,200,"Clock");
//    */
//
//    SystemDefs sysdef = new SystemDefs( dbpath, 1000, NUMBUF, "Clock" );
//
//    // creating the SM_Sailors relation
//    AttrType [] Stypes = new AttrType[4];
//    Stypes[0] = new AttrType (AttrType.attrInteger);
//    Stypes[1] = new AttrType (AttrType.attrString);
//    Stypes[2] = new AttrType (AttrType.attrInteger);
//    Stypes[3] = new AttrType (AttrType.attrReal);
//
//    //SOS
//    short [] Ssizes = new short [1];
//    Ssizes[0] = 30; //first elt. is 30
//
//    BigT.Map t = new BigT.Map();
//    try {
//      t.setHdr((short) 4,Stypes, Ssizes);
//    }
//    catch (Exception e) {
//      System.err.println("*** error in Tuple.setHdr() ***");
//      status = FAIL;
//      e.printStackTrace();
//    }
//
//    int size = t.size();
//
//    // inserting the tuple into file "SM_Sailors"
//    RID             rid;
//    Heapfile        f = null;
//    try {
//      f = new Heapfile("SM_Sailors.in");
//    }
//    catch (Exception e) {
//      System.err.println("*** error in Heapfile constructor ***");
//      status = FAIL;
//      e.printStackTrace();
//    }
//
//    t = new BigT.Map(size);
//    try {
//      t.setHdr((short) 4, Stypes, Ssizes);
//    }
//    catch (Exception e) {
//      System.err.println("*** error in Tuple.setHdr() ***");
//      status = FAIL;
//      e.printStackTrace();
//    }
//
//    for (int i=0; i<numSM_Sailors; i++) {
//      try {
//	t.setIntFld(1, ((SM_Sailor)SM_Sailors.elementAt(i)).sid);
//	t.setStrFld(2, ((SM_Sailor)SM_Sailors.elementAt(i)).sname);
//	t.setIntFld(3, ((SM_Sailor)SM_Sailors.elementAt(i)).rating);
//	t.setFloFld(4, (float)((SM_Sailor)SM_Sailors.elementAt(i)).age);
//      }
//      catch (Exception e) {
//	System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
//	status = FAIL;
//	e.printStackTrace();
//      }
//
//      try {
//	rid = f.insertMap(t.returnMapByteArray());
//      }
//      catch (Exception e) {
//	System.err.println("*** error in Heapfile.insertRecord() ***");
//	status = FAIL;
//	e.printStackTrace();
//      }
//    }
//    if (status != OK) {
//      //bail out
//      System.err.println ("*** Error creating relation for SM_Sailors");
//      Runtime.getRuntime().exit(1);
//    }
//
//    //creating the SM_Boats relation
//    AttrType [] Btypes = {
//      new AttrType(AttrType.attrInteger),
//      new AttrType(AttrType.attrString),
//      new AttrType(AttrType.attrString),
//    };
//
//    short  []  Bsizes = new short[2];
//    Bsizes[0] = 30;
//    Bsizes[1] = 20;
//    t = new BigT.Map();
//    try {
//      t.setHdr((short) 3,Btypes, Bsizes);
//    }
//    catch (Exception e) {
//      System.err.println("*** error in Tuple.setHdr() ***");
//      status = FAIL;
//      e.printStackTrace();
//    }
//
//    size = t.size();
//
//    // inserting the tuple into file "SM_Boats"
//    //RID             rid;
//    f = null;
//    try {
//      f = new Heapfile("SM_Boats.in");
//    }
//    catch (Exception e) {
//      System.err.println("*** error in Heapfile constructor ***");
//      status = FAIL;
//      e.printStackTrace();
//    }
//
//    t = new BigT.Map(size);
//    try {
//      t.setHdr((short) 3, Btypes, Bsizes);
//    }
//    catch (Exception e) {
//      System.err.println("*** error in Tuple.setHdr() ***");
//      status = FAIL;
//      e.printStackTrace();
//    }
//
//    for (int i=0; i<numSM_Boats; i++) {
//      try {
//	t.setIntFld(1, ((SM_Boats)SM_Boats.elementAt(i)).bid);
//	t.setStrFld(2, ((SM_Boats)SM_Boats.elementAt(i)).bname);
//	t.setStrFld(3, ((SM_Boats)SM_Boats.elementAt(i)).color);
//      }
//      catch (Exception e) {
//	System.err.println("*** error in Tuple.setStrFld() ***");
//	status = FAIL;
//	e.printStackTrace();
//      }
//
//      try {
//	rid = f.insertMap(t.returnMapByteArray());
//      }
//      catch (Exception e) {
//	System.err.println("*** error in Heapfile.insertRecord() ***");
//	status = FAIL;
//	e.printStackTrace();
//      }
//    }
//    if (status != OK) {
//      //bail out
//      System.err.println ("*** Error creating relation for SM_Boats");
//      Runtime.getRuntime().exit(1);
//    }
//
//    //creating the SM_Boats relation
//    AttrType [] Rtypes = new AttrType[3];
//    Rtypes[0] = new AttrType (AttrType.attrInteger);
//    Rtypes[1] = new AttrType (AttrType.attrInteger);
//    Rtypes[2] = new AttrType (AttrType.attrString);
//
//    short [] Rsizes = new short [1];
//    Rsizes[0] = 15;
//    t = new BigT.Map();
//    try {
//      t.setHdr((short) 3,Rtypes, Rsizes);
//    }
//    catch (Exception e) {
//      System.err.println("*** error in Tuple.setHdr() ***");
//      status = FAIL;
//      e.printStackTrace();
//    }
//
//    size = t.size();
//
//    // inserting the tuple into file "SM_Boats"
//    //RID             rid;
//    f = null;
//    try {
//      f = new Heapfile("SM_Reserves.in");
//    }
//    catch (Exception e) {
//      System.err.println("*** error in Heapfile constructor ***");
//      status = FAIL;
//      e.printStackTrace();
//    }
//
//    t = new BigT.Map(size);
//    try {
//      t.setHdr((short) 3, Rtypes, Rsizes);
//    }
//    catch (Exception e) {
//      System.err.println("*** error in Tuple.setHdr() ***");
//      status = FAIL;
//      e.printStackTrace();
//    }
//
//    for (int i=0; i<numSM_Reserves; i++) {
//      try {
//	t.setIntFld(1, ((SM_Reserves)SM_Reserves.elementAt(i)).sid);
//	t.setIntFld(2, ((SM_Reserves)SM_Reserves.elementAt(i)).bid);
//	t.setStrFld(3, ((SM_Reserves)SM_Reserves.elementAt(i)).date);
//
//      }
//      catch (Exception e) {
//	System.err.println("*** error in Tuple.setStrFld() ***");
//	status = FAIL;
//	e.printStackTrace();
//      }
//
//      try {
//	rid = f.insertMap(t.returnMapByteArray());
//      }
//      catch (Exception e) {
//	System.err.println("*** error in Heapfile.insertRecord() ***");
//	status = FAIL;
//	e.printStackTrace();
//      }
//    }
//    if (status != OK) {
//      //bail out
//      System.err.println ("*** Error creating relation for SM_Reserves");
//      Runtime.getRuntime().exit(1);
//    }
//
//  }
//
//  public boolean runTests() {
//
//    Disclaimer();
//    Query1();
//
//    Query2();
//    Query3();
//
//
//    Query4();
//    Query5();
//    Query6();
//
//
//    System.out.print ("Finished joins testing"+"\n");
//
//
//    return true;
//  }
//
//  private void Query1_CondExpr(CondExpr[] expr) {
//
//    expr[0].next  = null;
//    expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
//    expr[0].type1 = new AttrType(AttrType.attrSymbol);
//    expr[0].type2 = new AttrType(AttrType.attrSymbol);
//    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
//    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
//
//    expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
//    expr[1].next  = null;
//    expr[1].type1 = new AttrType(AttrType.attrSymbol);
//    expr[1].type2 = new AttrType(AttrType.attrInteger);
//    expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),2);
//    expr[1].operand2.integer = 1;
//
//    expr[2] = null;
//  }
//
//  private void Query2_CondExpr(CondExpr[] expr, CondExpr[] expr2) {
//
//    expr[0].next  = null;
//    expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
//    expr[0].type1 = new AttrType(AttrType.attrSymbol);
//    expr[0].type2 = new AttrType(AttrType.attrSymbol);
//    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
//    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
//
//    expr[1] = null;
//
//    expr2[0].next  = null;
//    expr2[0].op    = new AttrOperator(AttrOperator.aopEQ);
//    expr2[0].type1 = new AttrType(AttrType.attrSymbol);
//    expr2[0].type2 = new AttrType(AttrType.attrSymbol);
//    expr2[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
//    expr2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
//
//    expr2[1].op   = new AttrOperator(AttrOperator.aopEQ);
//    expr2[1].next = null;
//    expr2[1].type1 = new AttrType(AttrType.attrSymbol);
//    expr2[1].type2 = new AttrType(AttrType.attrString);
//    expr2[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);
//    expr2[1].operand2.string = "red";
//
//    expr2[2] = null;
//  }
//
//  private void Query3_CondExpr(CondExpr[] expr) {
//
//    expr[0].next  = null;
//    expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
//    expr[0].type1 = new AttrType(AttrType.attrSymbol);
//    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
//    expr[0].type2 = new AttrType(AttrType.attrSymbol);
//    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
//    expr[1] = null;
//  }
//
//  private CondExpr[] Query5_CondExpr() {
//    CondExpr [] expr2 = new CondExpr[3];
//    expr2[0] = new CondExpr();
//
//
//    expr2[0].next  = null;
//    expr2[0].op    = new AttrOperator(AttrOperator.aopEQ);
//    expr2[0].type1 = new AttrType(AttrType.attrSymbol);
//
//    expr2[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
//    expr2[0].type2 = new AttrType(AttrType.attrSymbol);
//
//    expr2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
//
//    expr2[1] = new CondExpr();
//    expr2[1].op   = new AttrOperator(AttrOperator.aopGT);
//    expr2[1].next = null;
//    expr2[1].type1 = new AttrType(AttrType.attrSymbol);
//
//    expr2[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),4);
//    expr2[1].type2 = new AttrType(AttrType.attrReal);
//    expr2[1].operand2.real = (float)40.0;
//
//
//    expr2[1].next = new CondExpr();
//    expr2[1].next.op   = new AttrOperator(AttrOperator.aopLT);
//    expr2[1].next.next = null;
//    expr2[1].next.type1 = new AttrType(AttrType.attrSymbol); // rating
//    expr2[1].next.operand1.symbol = new FldSpec ( new RelSpec(RelSpec.outer),3);
//    expr2[1].next.type2 = new AttrType(AttrType.attrInteger);
//    expr2[1].next.operand2.integer = 7;
//
//    expr2[2] = null;
//    return expr2;
//  }
//
//  private void Query6_CondExpr(CondExpr[] expr, CondExpr[] expr2) {
//
//    expr[0].next  = null;
//    expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
//    expr[0].type1 = new AttrType(AttrType.attrSymbol);
//
//    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
//    expr[0].type2 = new AttrType(AttrType.attrSymbol);
//
//    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
//
//    expr[1].next  = null;
//    expr[1].op    = new AttrOperator(AttrOperator.aopGT);
//    expr[1].type1 = new AttrType(AttrType.attrSymbol);
//
//    expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
//    expr[1].type2 = new AttrType(AttrType.attrInteger);
//    expr[1].operand2.integer = 7;
//
//    expr[2] = null;
//
//    expr2[0].next  = null;
//    expr2[0].op    = new AttrOperator(AttrOperator.aopEQ);
//    expr2[0].type1 = new AttrType(AttrType.attrSymbol);
//
//    expr2[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
//    expr2[0].type2 = new AttrType(AttrType.attrSymbol);
//
//    expr2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
//
//    expr2[1].next = null;
//    expr2[1].op   = new AttrOperator(AttrOperator.aopEQ);
//    expr2[1].type1 = new AttrType(AttrType.attrSymbol);
//
//    expr2[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);
//    expr2[1].type2 = new AttrType(AttrType.attrString);
//    expr2[1].operand2.string = "red";
//
//    expr2[2] = null;
//  }
//
//  public void Query1() {
//
//    System.out.print("**********************Query1 strating *********************\n");
//    boolean status = OK;
//
//    // SM_Sailors, SM_Boats, SM_Reserves Queries.
//    System.out.print ("Query: Find the names of SM_Sailors who have reserved "
//		      + "boat number 1.\n"
//		      + "       and print out the date of reservation.\n\n"
//		      + "  SELECT S.sname, R.date\n"
//		      + "  FROM   SM_Sailors S, SM_Reserves R\n"
//		      + "  WHERE  S.sid = R.sid AND R.bid = 1\n\n");
//
//    System.out.print ("\n(Tests FileScan, Projection, and Sort-Merge Join)\n");
//
//    CondExpr[] outFilter = new CondExpr[3];
//    outFilter[0] = new CondExpr();
//    outFilter[1] = new CondExpr();
//    outFilter[2] = new CondExpr();
//
//    Query1_CondExpr(outFilter);
//
//    BigT.Map t = new BigT.Map();
//
//    AttrType [] Stypes = new AttrType[4];
//    Stypes[0] = new AttrType (AttrType.attrInteger);
//    Stypes[1] = new AttrType (AttrType.attrString);
//    Stypes[2] = new AttrType (AttrType.attrInteger);
//    Stypes[3] = new AttrType (AttrType.attrReal);
//
//    //SOS
//    short [] Ssizes = new short[1];
//    Ssizes[0] = 30; //first elt. is 30
//
//    FldSpec [] Sprojection = new FldSpec[4];
//    Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
//    Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
//    Sprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
//    Sprojection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
//
//    CondExpr [] selects = new CondExpr [1];
//    selects = null;
//
//
//    FileScan am = null;
//    try {
//      am  = new FileScan("SM_Sailors.in", Stypes, Ssizes,
//				  (short)4, (short)4,
//				  Sprojection, null);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      System.err.println (""+e);
//    }
//
//    if (status != OK) {
//      //bail out
//      System.err.println ("*** Error setting up scan for SM_Sailors");
//      Runtime.getRuntime().exit(1);
//    }
//
//    AttrType [] Rtypes = new AttrType[3];
//    Rtypes[0] = new AttrType (AttrType.attrInteger);
//    Rtypes[1] = new AttrType (AttrType.attrInteger);
//    Rtypes[2] = new AttrType (AttrType.attrString);
//
//    short [] Rsizes = new short[1];
//    Rsizes[0] = 15;
//    FldSpec [] Rprojection = new FldSpec[3];
//    Rprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
//    Rprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
//    Rprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
//
//    FileScan am2 = null;
//    try {
//      am2 = new FileScan("SM_Reserves.in", Rtypes, Rsizes,
//				  (short)3, (short) 3,
//				  Rprojection, null);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      System.err.println (""+e);
//    }
//
//    if (status != OK) {
//      //bail out
//      System.err.println ("*** Error setting up scan for SM_Reserves");
//      Runtime.getRuntime().exit(1);
//    }
//
//
//    FldSpec [] proj_list = new FldSpec[2];
//    proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 2);
//    proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
//
//    AttrType [] jtype = new AttrType[2];
//    jtype[0] = new AttrType (AttrType.attrString);
//    jtype[1] = new AttrType (AttrType.attrString);
//
//    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
//    SortMerge sm = null;
//    try {
//      sm = new SortMerge(Stypes, 4, Ssizes,
//			 Rtypes, 3, Rsizes,
//			 1, 4,
//			 1, 4,
//			 10,
//			 am, am2,
//			 false, false, ascending,
//			 outFilter, proj_list, 2);
//    }
//    catch (Exception e) {
//      System.err.println("*** join error in SortMerge constructor ***");
//      status = FAIL;
//      System.err.println (""+e);
//      e.printStackTrace();
//    }
//
//    if (status != OK) {
//      //bail out
//      System.err.println ("*** Error constructing SortMerge");
//      Runtime.getRuntime().exit(1);
//    }
//
//
//
//    QueryCheck qcheck1 = new QueryCheck(1);
//
//
//    t = null;
//
//    try {
//      while ((t = sm.get_next()) != null) {
//        t.print(jtype);
//
//        qcheck1.Check(t);
//      }
//    }
//    catch (Exception e) {
//      System.err.println (""+e);
//       e.printStackTrace();
//       status = FAIL;
//    }
//    if (status != OK) {
//      //bail out
//      System.err.println ("*** Error in get next tuple ");
//      Runtime.getRuntime().exit(1);
//    }
//
//    qcheck1.report(1);
//    try {
//      sm.close();
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//    System.out.println ("\n");
//    if (status != OK) {
//      //bail out
//      System.err.println ("*** Error in closing ");
//      Runtime.getRuntime().exit(1);
//    }
//  }
//
//  public void Query2() {}
//
//
//   public void Query3() {
//    System.out.print("**********************Query3 strating *********************\n");
//    boolean status = OK;
//
//        // SM_Sailors, SM_Boats, SM_Reserves Queries.
//
//    System.out.print
//      ( "Query: Find the names of SM_Sailors who have reserved a boat.\n\n"
//	+ "  SELECT S.sname\n"
//	+ "  FROM   SM_Sailors S, SM_Reserves R\n"
//	+ "  WHERE  S.sid = R.sid\n\n"
//	+ "(Tests FileScan, Projection, and SortMerge Join.)\n\n");
//
//    CondExpr [] outFilter = new CondExpr[2];
//    outFilter[0] = new CondExpr();
//    outFilter[1] = new CondExpr();
//
//    Query3_CondExpr(outFilter);
//
//    BigT.Map t = new BigT.Map();
//    t = null;
//
//    AttrType Stypes[] = {
//      new AttrType(AttrType.attrInteger),
//      new AttrType(AttrType.attrString),
//      new AttrType(AttrType.attrInteger),
//      new AttrType(AttrType.attrReal)
//    };
//    short []   Ssizes = new short[1];
//    Ssizes[0] = 30;
//
//    AttrType [] Rtypes = {
//      new AttrType(AttrType.attrInteger),
//      new AttrType(AttrType.attrInteger),
//      new AttrType(AttrType.attrString),
//    };
//    short  []  Rsizes = new short[1];
//    Rsizes[0] =15;
//
//    FldSpec [] Sprojection = {
//       new FldSpec(new RelSpec(RelSpec.outer), 1),
//       new FldSpec(new RelSpec(RelSpec.outer), 2),
//       new FldSpec(new RelSpec(RelSpec.outer), 3),
//       new FldSpec(new RelSpec(RelSpec.outer), 4)
//    };
//
//    CondExpr[] selects = new CondExpr [1];
//    selects = null;
//
//    iterator.Iterator am = null;
//    try {
//      am  = new FileScan("SM_Sailors.in", Stypes, Ssizes,
//				  (short)4, (short) 4,
//				  Sprojection, null);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      System.err.println (""+e);
//    }
//
//    if (status != OK) {
//      //bail out
//      System.err.println ("*** Error setting up scan for SM_Sailors");
//      Runtime.getRuntime().exit(1);
//    }
//
//    FldSpec [] Rprojection = {
//       new FldSpec(new RelSpec(RelSpec.outer), 1),
//       new FldSpec(new RelSpec(RelSpec.outer), 2),
//       new FldSpec(new RelSpec(RelSpec.outer), 3)
//    };
//
//    iterator.Iterator am2 = null;
//    try {
//      am2 = new FileScan("SM_Reserves.in", Rtypes, Rsizes,
//				  (short)3, (short)3,
//				  Rprojection, null);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      System.err.println (""+e);
//    }
//
//    if (status != OK) {
//      //bail out
//      System.err.println ("*** Error setting up scan for SM_Reserves");
//      Runtime.getRuntime().exit(1);
//    }
//
//    FldSpec [] proj_list = {
//      new FldSpec(new RelSpec(RelSpec.outer), 2)
//    };
//
//    AttrType [] jtype     = { new AttrType(AttrType.attrString) };
//
//    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
//    SortMerge sm = null;
//    try {
//      sm = new SortMerge(Stypes, 4, Ssizes,
//			 Rtypes, 3, Rsizes,
//			 1, 4,
//			 1, 4,
//			 10,
//			 am, am2,
//			 false, false, ascending,
//			 outFilter, proj_list, 1);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      System.err.println (""+e);
//    }
//
//    if (status != OK) {
//      //bail out
//      System.err.println ("*** Error constructing SortMerge");
//      Runtime.getRuntime().exit(1);
//    }
//
//    QueryCheck qcheck3 = new QueryCheck(3);
//
//
//    t = null;
//
//    try {
//      while ((t = sm.get_next()) != null) {
//        t.print(jtype);
//        qcheck3.Check(t);
//      }
//    }
//    catch (Exception e) {
//      System.err.println (""+e);
//      e.printStackTrace();
//       Runtime.getRuntime().exit(1);
//    }
//
//
//    qcheck3.report(3);
//
//    System.out.println ("\n");
//    try {
//      sm.close();
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//
//    if (status != OK) {
//      //bail out
//      System.err.println ("*** Error setting up scan for SM_Sailors");
//      Runtime.getRuntime().exit(1);
//    }
//  }
//
//   public void Query4() {
//     System.out.print("**********************Query4 strating *********************\n");
//    boolean status = OK;
//
//    // SM_Sailors, SM_Boats, SM_Reserves Queries.
//
//    System.out.print
//      ("Query: Find the names of SM_Sailors who have reserved a boat\n"
//       + "       and print each name once.\n\n"
//       + "  SELECT DISTINCT S.sname\n"
//       + "  FROM   SM_Sailors S, SM_Reserves R\n"
//       + "  WHERE  S.sid = R.sid\n\n"
//       + "(Tests FileScan, Projection, Sort-Merge Join and "
//       + "Duplication elimination.)\n\n");
//
//    CondExpr [] outFilter = new CondExpr[2];
//    outFilter[0] = new CondExpr();
//    outFilter[1] = new CondExpr();
//
//    Query3_CondExpr(outFilter);
//
//    BigT.Map t = new BigT.Map();
//    t = null;
//
//    AttrType Stypes[] = {
//      new AttrType(AttrType.attrInteger),
//      new AttrType(AttrType.attrString),
//      new AttrType(AttrType.attrInteger),
//      new AttrType(AttrType.attrReal)
//    };
//    short []   Ssizes = new short[1];
//    Ssizes[0] = 30;
//
//    AttrType [] Rtypes = {
//      new AttrType(AttrType.attrInteger),
//      new AttrType(AttrType.attrInteger),
//      new AttrType(AttrType.attrString),
//    };
//    short  []  Rsizes = new short[1];
//    Rsizes[0] =15;
//
//    FldSpec [] Sprojection = {
//       new FldSpec(new RelSpec(RelSpec.outer), 1),
//       new FldSpec(new RelSpec(RelSpec.outer), 2),
//       new FldSpec(new RelSpec(RelSpec.outer), 3),
//       new FldSpec(new RelSpec(RelSpec.outer), 4)
//    };
//
//    CondExpr[] selects = new CondExpr [1];
//    selects = null;
//
//    iterator.Iterator am = null;
//    try {
//      am  = new FileScan("SM_Sailors.in", Stypes, Ssizes,
//				  (short)4, (short) 4,
//				  Sprojection, null);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      System.err.println (""+e);
//    }
//
//    if (status != OK) {
//      //bail out
//      System.err.println ("*** Error setting up scan for SM_Sailors");
//      Runtime.getRuntime().exit(1);
//    }
//
//    FldSpec [] Rprojection = {
//       new FldSpec(new RelSpec(RelSpec.outer), 1),
//       new FldSpec(new RelSpec(RelSpec.outer), 2),
//       new FldSpec(new RelSpec(RelSpec.outer), 3)
//    };
//
//    iterator.Iterator am2 = null;
//    try {
//      am2 = new FileScan("SM_Reserves.in", Rtypes, Rsizes,
//				  (short)3, (short)3,
//				  Rprojection, null);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      System.err.println (""+e);
//    }
//
//    if (status != OK) {
//      //bail out
//      System.err.println ("*** Error setting up scan for SM_Reserves");
//      Runtime.getRuntime().exit(1);
//    }
//
//    FldSpec [] proj_list = {
//      new FldSpec(new RelSpec(RelSpec.outer), 2)
//    };
//
//    AttrType [] jtype     = { new AttrType(AttrType.attrString) };
//
//    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
//    SortMerge sm = null;
//    short  []  jsizes    = new short[1];
//    jsizes[0] = 30;
//    try {
//      sm = new SortMerge(Stypes, 4, Ssizes,
//			 Rtypes, 3, Rsizes,
//			 1, 4,
//			 1, 4,
//			 10,
//			 am, am2,
//			 false, false, ascending,
//			 outFilter, proj_list, 1);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      System.err.println (""+e);
//    }
//
//    if (status != OK) {
//      //bail out
//      System.err.println ("*** Error constructing SortMerge");
//      Runtime.getRuntime().exit(1);
//    }
//
//
//    DuplElim ed = null;
//    try {
//      ed = new DuplElim(jtype, (short)1, jsizes, sm, 10, false);
//    }
//    catch (Exception e) {
//      System.err.println (""+e);
//      Runtime.getRuntime().exit(1);
//    }
//
//    QueryCheck qcheck4 = new QueryCheck(4);
//
//
//    t = null;
//
//    try {
//      while ((t = ed.get_next()) != null) {
//        t.print(jtype);
//        qcheck4.Check(t);
//      }
//    }
//    catch (Exception e) {
//      System.err.println (""+e);
//      e.printStackTrace();
//      Runtime.getRuntime().exit(1);
//      }
//
//    qcheck4.report(4);
//    try {
//      ed.close();
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//   System.out.println ("\n");
//    if (status != OK) {
//      //bail out
//      System.err.println ("*** Error setting up scan for SM_Sailors");
//      Runtime.getRuntime().exit(1);
//    }
// }
//
//   public void Query5() {
//   System.out.print("**********************Query5 strating *********************\n");
//    boolean status = OK;
//        // SM_Sailors, SM_Boats, SM_Reserves Queries.
//
//    System.out.print
//      ("Query: Find the names of old SM_Sailors or SM_Sailors with "
//       + "a rating less\n       than 7, who have reserved a boat, "
//       + "(perhaps to increase the\n       amount they have to "
//       + "pay to make a reservation).\n\n"
//       + "  SELECT S.sname, S.rating, S.age\n"
//       + "  FROM   SM_Sailors S, SM_Reserves R\n"
//       + "  WHERE  S.sid = R.sid and (S.age > 40 || S.rating < 7)\n\n"
//       + "(Tests FileScan, Multiple Selection, Projection, "
//       + "and Sort-Merge Join.)\n\n");
//
//
//    CondExpr [] outFilter;
//    outFilter = Query5_CondExpr();
//
//    BigT.Map t = new BigT.Map();
//    t = null;
//
//    AttrType Stypes[] = {
//      new AttrType(AttrType.attrInteger),
//      new AttrType(AttrType.attrString),
//      new AttrType(AttrType.attrInteger),
//      new AttrType(AttrType.attrReal)
//    };
//    short []   Ssizes = new short[1];
//    Ssizes[0] = 30;
//
//    AttrType [] Rtypes = {
//      new AttrType(AttrType.attrInteger),
//      new AttrType(AttrType.attrInteger),
//      new AttrType(AttrType.attrString),
//    };
//    short  []  Rsizes = new short[1];
//    Rsizes[0] = 15;
//
//    FldSpec [] Sprojection = {
//      new FldSpec(new RelSpec(RelSpec.outer), 1),
//      new FldSpec(new RelSpec(RelSpec.outer), 2),
//      new FldSpec(new RelSpec(RelSpec.outer), 3),
//      new FldSpec(new RelSpec(RelSpec.outer), 4)
//    };
//
//    CondExpr[] selects = new CondExpr [1];
//    selects[0] = null;
//
//    FldSpec [] proj_list = {
//      new FldSpec(new RelSpec(RelSpec.outer), 2),
//      new FldSpec(new RelSpec(RelSpec.outer), 3),
//      new FldSpec(new RelSpec(RelSpec.outer), 4)
//    };
//
//    FldSpec [] Rprojection = {
//      new FldSpec(new RelSpec(RelSpec.outer), 1),
//      new FldSpec(new RelSpec(RelSpec.outer), 2),
//      new FldSpec(new RelSpec(RelSpec.outer), 3)
//    };
//
//    AttrType [] jtype     = {
//      new AttrType(AttrType.attrString),
//      new AttrType(AttrType.attrInteger),
//      new AttrType(AttrType.attrReal)
//    };
//
//
//    iterator.Iterator am = null;
//    try {
//      am  = new FileScan("SM_Sailors.in", Stypes, Ssizes,
//				  (short)4, (short)4,
//				  Sprojection, null);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      System.err.println (""+e);
//    }
//
//    if (status != OK) {
//      //bail out
//      System.err.println ("*** Error setting up scan for SM_Sailors");
//      Runtime.getRuntime().exit(1);
//    }
//
//    iterator.Iterator am2 = null;
//    try {
//      am2 = new FileScan("SM_Reserves.in", Rtypes, Rsizes,
//			 (short)3, (short)3,
//			 Rprojection, null);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      System.err.println (""+e);
//    }
//
//    if (status != OK) {
//      //bail out
//      System.err.println ("*** Error setting up scan for SM_Reserves");
//      Runtime.getRuntime().exit(1);
//    }
//
//    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
//    SortMerge sm = null;
//    try {
//      sm = new SortMerge(Stypes, 4, Ssizes,
//			 Rtypes, 3, Rsizes,
//			 1, 4,
//			 1, 4,
//			 10,
//			 am, am2,
//			 false, false, ascending,
//			 outFilter, proj_list, 3);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      System.err.println (""+e);
//    }
//
//    if (status != OK) {
//      //bail out
//      System.err.println ("*** Error constructing SortMerge");
//      Runtime.getRuntime().exit(1);
//    }
//
//    QueryCheck qcheck5 = new QueryCheck(5);
//    //Tuple t = new Tuple();
//    t = null;
//
//    try {
//      while ((t = sm.get_next()) != null) {
//        t.print(jtype);
//        qcheck5.Check(t);
//      }
//    }
//    catch (Exception e) {
//      System.err.println (""+e);
//      Runtime.getRuntime().exit(1);
//    }
//
//    qcheck5.report(5);
//    try {
//      sm.close();
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//    System.out.println ("\n");
//    if (status != OK) {
//      //bail out
//      System.err.println ("*** Error close for sortmerge");
//      Runtime.getRuntime().exit(1);
//    }
// }
//
//  public void Query6(){}
//
//
//  private void Disclaimer() {
//    System.out.print ("\n\nAny resemblance of persons in this database to"
//         + " people living or dead\nis purely coincidental. The contents of "
//         + "this database do not reflect\nthe views of the University,"
//         + " the Computer  Sciences Department or the\n"
//         + "developers...\n\n");
//  }
//}
//
//public class SM_JoinTest
//{
//  public static void main(String argv[])
//  {
//    boolean sortstatus;
//    //SystemDefs global = new SystemDefs("bingjiedb", 100, 70, null);
//    //JavabaseDB.openDB("/tmp/nwangdb", 5000);
//
//    SM_JoinsDriver jjoin = new SM_JoinsDriver();
//
//    sortstatus = jjoin.runTests();
//    if (sortstatus != true) {
//      System.out.println("Error ocurred during join tests");
//    }
//    else {
//      System.out.println("join tests completed successfully");
//    }
//  }
//}
//
