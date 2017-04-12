
import java.util.concurrent.*;

class DataManager extends DBKernel implements Runnable {

    LinkedBlockingQueue<dbOp> tmsc;
    ConcurrentSkipListSet<Integer> blSet;
    private String filesDir;
    private int bSize;

    DataManager(String name, LinkedBlockingQueue<dbOp> opEntry, LinkedBlockingQueue<dbOp> tmsc, ConcurrentSkipListSet<Integer> blSetIn, String dir, int size) {
        threadName = name;
        operationsEntryQueue = opEntry;
        this.tmsc = tmsc;
        blSet = blSetIn;
        filesDir = dir;
        bSize = size;
    }

    @Override
    public void run() {
        //code for DM goes here.
    }

    public void start() {
        //standard start function
        if (t == null) {
            t = new Thread(this, threadName);
            t.start();
        }
    }
}
