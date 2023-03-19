package dboperations;

import diskmgr.PCounter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Batchinsert {
    private String data_file_path;
    private BigT bigtable;
    PCounter pCounter = new PCounter();

    public Batchinsert(String DATAFILENAME, int TYPE, String BIGTABLENAME) {
        this.data_file_path = DATAFILENAME;
        bigtable = new BigT(BIGTABLENAME, TYPE);

        pCounter.initialize();
    }

    public void run() throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(data_file_path));
        String line;
        while((line = reader.readLine()) != null) {
            String[] mapAttributes = line.trim().split(",");

            // Create Map
            byte[] mapPtr = new byte[MAP_SIZE];
            Map map = new Map(mapPtr, 0);
            map.setRowLabel(mapAttributes[0]);
            map.setColumnLabel(mapAttributes[1]);
            map.setTimeStamp(Integer.valueOf(mapAttributes[2]));
            map.setValue(mapAttributes[3]);

            // Insert Map into BigTable
            bigtable.insertMap(map.getMapByteArray());
        }
        reader.close();

        // print num of db pages read/written
        pCounter.print();
    }
}