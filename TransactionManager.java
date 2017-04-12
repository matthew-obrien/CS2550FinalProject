import java.lang.Thread.*;
import java.util.*;
import java.util.concurrent.*;

class TransactionManager implements Runnable 
{
    private Thread t;
    private String threadName;
    LinkedBlockingQueue<dbOp> tmsc;
    ConcurrentSkipListSet<Integer> blSet;
    String scriptsDir;

	TransactionManager(String name, LinkedBlockingQueue<dbOp> q1, ConcurrentSkipListSet<Integer> blSetIn, String dir) 
    {
		threadName = name;
        tmsc = q1;
        blSet = blSetIn;
        scriptsDir = dir;
	}
	
    public void run() 
    {
		//code for TM goes here.
	}
    
    public void start () 
    {
      //standard start function
      if (t == null) {
         t = new Thread (this, threadName);
         t.start ();
      }
   }
}