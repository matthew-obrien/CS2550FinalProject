
import java.util.concurrent.*;

class DataManager extends DBKernel implements Runnable {

    LinkedBlockingQueue<dbOp> scdm;
    LinkedBlockingQueue<dbOp> tmsc;
    ConcurrentSkipListSet<Integer> blSet;
    private String filesDir;
    private int bSize;

    DataManager(String name, LinkedBlockingQueue<dbOp> q1, LinkedBlockingQueue<dbOp> q2, ConcurrentSkipListSet<Integer> blSetIn, String dir, int size) {
        threadName = name;
        tmsc = q1;
        scdm = q2;
        blSet = blSetIn;
        filesDir = dir;
        bSize = size;
    }

    @Override
    public void run() {
        //code for DM goes here.
        try {
            dbOp oper = scdm.take();
            System.out.println("\nDM has received the following operation:");
            System.out.println(oper);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void start() {
        //standard start function
        if (t == null) {
            t = new Thread(this, threadName);
            t.start();
        }
    }
}
