
import java.util.concurrent.*;

class Scheduler extends DBKernel implements Runnable {

    LinkedBlockingQueue<dbOp> scdm;

    Scheduler(String name, LinkedBlockingQueue<dbOp> q1, LinkedBlockingQueue<dbOp> q2) {
        threadName = name;
        operationsEntryQueue = q1;
        scdm = q2;
    }

    @Override
    public void run() {
        //code for SC goes here.
    }

    public void start() {
        //standard start function
        if (t == null) {
            t = new Thread(this, threadName);
            t.start();
        }
    }
}
