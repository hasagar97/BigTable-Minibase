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
    Map test_map = null;
    BigT testclass = null;
    int row_count = 0;
    int col_count = 0;
    
    try
    {
      // Create the bigT class
      testclass = new BigT("test", 5);
    }
    catch (Exception e)
    {
      System.err.println("FAIL - Failed to create BigT instance\n");
      status = FAIL;
    }

    try
    {
      // Insert some test maps
      for (int i = 0; i < 10; i++)
      {
        test_map = new Map();
        test_map.setRowLabel("test"+i);
        test_map.setColumnLabel("test"+(i+1));
        test_map.setTimeStamp(i);
        testclass.insertMap(test_map.getMapByteArray());
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
      System.err.println("FAIL - Failed to insert maps\n");
      status = FAIL;
    }

    try
    {
      // Check row/col counts are correct
      row_count = testclass.getRowCnt();
      col_count = testclass.getColumnCnt();
    }
    catch (Exception e)
    {
      System.err.println("FAIL - Failed to get row/col counts\n");
      status = FAIL;
    }

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

    try
    {
      // Insert some duplicate maps
      for (int i = 0; i < 3; i++)
      {
        test_map = new Map();
        test_map.setRowLabel("test"+i);
        test_map.setColumnLabel("test"+(i+1));
        test_map.setTimeStamp(i+5);
        testclass.insertMap(test_map.getMapByteArray());
        test_map.setTimeStamp(i+6);
        testclass.insertMap(test_map.getMapByteArray());
      }

      // Insert one new map with duplicate label but new column
      test_map = new Map();
      test_map.setRowLabel("test"+0);
      test_map.setColumnLabel("test"+11);
      test_map.setTimeStamp(15);
      testclass.insertMap(test_map.getMapByteArray());
    }
    catch (Exception e)
    {
      System.err.println("FAIL - Failed to insert maps\n");
      status = FAIL;
    }

    
    try
    {
      // Check row/col counts are correct
      row_count = testclass.getRowCnt();
      col_count = testclass.getColumnCnt();
    }
    catch (Exception e)
    {
      System.err.println("FAIL - Failed to get row/col counts\n");
      status = FAIL;
    }


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

    try
    {
      // Insert some duplicate maps that should force older maps to drop
      for (int i = 0; i < 3; i++)
      {
        test_map = new Map();
        test_map.setRowLabel("test"+i);
        test_map.setColumnLabel("test"+(i+1));
        test_map.setTimeStamp(i+8);
        testclass.insertMap(test_map.getMapByteArray());
      }
      }
    catch (Exception e)
    {
      System.err.println("FAIL - Failed to insert maps\n");
      status = FAIL;
    }

    try
    {
      // Check row/col counts are correct
      row_count = testclass.getRowCnt();
      col_count = testclass.getColumnCnt();
    }
    catch (Exception e)
    {
      System.err.println("FAIL - Failed to get row/col counts\n");
      status = FAIL;
    }

    if (row_count != 10)
    {
      System.err.println("FAIL - Row count 3 did not match entries\n");
      status = FAIL;
    }
    if (col_count != 11)
    {
      System.err.println("FAIL - Column count 3 did not match entries\n");
      status = FAIL;
    }

    // Use a stream to make sure there are no more than 3 instances of a row/col combination 
    // and that the instance with the oldest timestamp was dropped
    int count = 0;
    
    try
    {
      Stream stream = testclass.openStream(6, "test0", "test1", null);
      RID mid = null;

      while (test_map != null)
      {
        test_map = stream.getNext(mid);

        if (test_map != null)
        {
          count += 1;

          if (test_map.getTimeStamp() == 0)
          {
            System.err.println("FAIL - Map with oldest timestamp was not dropped\n");
            status = FAIL;
          }
        }
      }
    }
    catch (Exception e)
    {
      System.err.println("FAIL - Failed to get run stream\n");
      status = FAIL;
    }

    if (count > 3)
    {
      System.err.println("FAIL - Count shows Map with oldest timestamp was not dropped\n");
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
