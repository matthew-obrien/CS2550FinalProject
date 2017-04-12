
import java.util.concurrent.*;

class Scheduler extends DBKernel implements Runnable {

    final private LinkedBlockingQueue<dbOp> tmsc;
    final private LinkedBlockingQueue<dbOp> scdm;

    Scheduler(String name, LinkedBlockingQueue<dbOp> q1, LinkedBlockingQueue<dbOp> q2) {
        threadName = name;
        tmsc = q1;
        scdm = q2;
    }

    @Override
    public void run() {
        try {
            dbOp oper = tmsc.take();
            System.out.println("\nSC has received the following operation:");
            System.out.println(oper);
            scdm.add(oper);
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
