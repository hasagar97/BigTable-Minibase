package diskmgr;

public class PCounter {
    private static int rcounter;
    private static int wcounter;
    public void initialize() {
        rcounter = 0;
        wcounter = 0;
    }

    public void readIncrement() {
        rcounter++;
    }

    public void writeIncrement() {
        wcounter++;
    }

    public int getReadCounter() {
        return rcounter;
    }

    public int getWriteCounter() {
        return wcounter;
    }

    public void print() {
        System.out.println("Page Read Count = " + rcounter + " Page Write Count = " + wcounter);
    }
}
