import java.util.*;
import java.lang.Thread;
import java.util.concurrent.*;

public class myPTA
{

    
    public static void main (String[] args)
    {
        //Here, initialize all shared data structures
        LinkedBlockingQueue<dbOp> tmsc = new LinkedBlockingQueue<dbOp>(); //queue containing operations being passed from the tm to the sc. Sometimes used by DM (for spontaneous aborts).
        LinkedBlockingQueue<dbOp> scdm = new LinkedBlockingQueue<dbOp>(); //queue containing operations being passed from sc to dm.
        
        //now intialize and start the threads
        TransactionManager tm = new TransactionManager("TM", tmsc);
        Scheduler sc = new Scheduler("SC", tmsc, scdm);
        DataManager dm = new DataManager("DM", tmsc, scdm);
        tm.start();
        sc.start();
        dm.start();
    }
    
}
