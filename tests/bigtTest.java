package tests;

import java.io.*;
import BigT.*;
import global.*;


class bigtDriver extends TestDriver 
  implements GlobalConst {

  public bigtDriver() {
    super("bigttest");
  }

  public boolean runTests ()  {
    
    System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");
    
    SystemDefs sysdef = new SystemDefs( dbpath, 300, NUMBUF, "Clock" );

    // Kill anything that might be hanging around
    String newdbpath;
    String newlogpath;
    String remove_logcmd;
    String remove_dbcmd;
    String remove_cmd = "/bin/rm -rf ";

    newdbpath = dbpath;
    newlogpath = logpath;

    remove_logcmd = remove_cmd + logpath;
    remove_dbcmd = remove_cmd + dbpath;

    // Commands here is very machine dependent.  We assume
    // user are on UNIX system here
    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
    } 
    catch (IOException e) {
      System.err.println (""+e);
    }
    
    remove_logcmd = remove_cmd + newlogpath;
    remove_dbcmd = remove_cmd + newdbpath;

    //This step seems redundant for me.  But it's in the original
    //C++ code.  So I am keeping it as of now, just in case I
    //I missed something
    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
    } 
    catch (IOException e) {
      System.err.println (""+e);
    }

    //Run the tests. Return type different from C++
    boolean _pass = runAllTests();

    //Clean up again
    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
    } 
    catch (IOException e) {
      System.err.println (""+e);
    }
    
    System.out.println ("\n" + "..." + testName() + " tests ");
    System.out.println (_pass==OK ? "completely successfully" : "failed");
    System.out.println (".\n\n");
    
    return _pass;
  }

  protected boolean test1()
  {
    System.out.println("------------------------ TEST 1 --------------------------");
    
    boolean status = OK;
    Map test_map;
    
    // Create the bigT class
    bigt testclass = new bigt("test", 1);

    // Insert some test maps
    for (int i = 0; i < 10; i++)
    {
      test_map = Map();
      test_map.setRowLabel("test"+i);
      test_map.setColumnLabel("test"+(i+1));
      test_map.setTimeStamp(i);
      testclass.insertMap(test_map.getMapByteArray());
    }

    // Check row/col counts are correct
    int row_count = testclass.getRowCnt();
    int col_count = testclass.getColumnCnt();

    if (row_count != 10)
    {
      System.err.println("FAIL - Row count 1 did not match entries\n");
      status = FAIL;
    }
    if (col_count != 10)
    {
      System.err.println("FAIL - Column count 1 did not match entries\n");
      status = FAIL;
    }

    // Insert some duplicate maps
    for (int i = 0; i < 3; i++)
    {
      test_map = Map();
      test_map.setRowLabel("test"+i);
      test_map.setColumnLabel("test"+(i+1));
      test_map.setTimeStamp(i+5);
      testclass.insertMap(test_map.getMapByteArray());
      test_map.setTimeStamp(i+6);
      testclass.insertMap(test_map.getMapByteArray());
    }

    // Insert one new map with duplicate label but new column
    test_map = Map();
    test_map.setRowLabel("test"+0);
    test_map.setColumnLabel("test"+11);
    test_map.setTimeStamp(15);
    testclass.insertMap(test_map.getMapByteArray());

    // Check row/col counts are still correct
    int row_count = testclass.getRowCnt();
    int col_count = testclass.getColumnCnt();

    if (row_count != 10)
    {
      System.err.println("FAIL - Row count 2 did not match entries\n");
      status = FAIL;
    }
    if (col_count != 11)
    {
      System.err.println("FAIL - Column count 2 did not match entries\n");
      status = FAIL;
    }

    // Insert some duplicate maps that should force older maps to drop
    for (int i = 0; i < 3; i++)
    {
      test_map = Map();
      test_map.setRowLabel("test"+i);
      test_map.setColumnLabel("test"+(i+1));
      test_map.setTimeStamp(i+8);
      testclass.insertMap(test_map.getMapByteArray());
    }

    // Check row/col counts are still correct
    int row_count = testclass.getRowCnt();
    int col_count = testclass.getColumnCnt();

    if (row_count != 10)
    {
      System.err.println("FAIL - Row count 3 did not match entries\n");
      status = FAIL;
    }
    if (col_count != 10)
    {
      System.err.println("FAIL - Column count 3 did not match entries\n");
      status = FAIL;
    }
    
    System.err.println("------------------- TEST 1 completed ---------------------\n");
    
    return status;
  }
}

public class bigtTest
{
  public static void main(String argv[])
  {
    boolean bigtstatus;

    bigtDriver bigtt = new bigtDriver();

    bigtstatus = bigtt.runTests();
    if (bigtstatus != true) {
      System.out.println("Error ocurred during bigt tests");
    }
    else {
      System.out.println("bigt tests completed successfully");
    }
  }
}