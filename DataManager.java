import java.lang.Thread.*;
import java.util.*;
import java.util.concurrent.*;

class DataManager implements Runnable 
{
    private Thread t;
    private String threadName;
    LinkedBlockingQueue<dbOp> tmsc;
    LinkedBlockingQueue<dbOp> scdm;
    ConcurrentSkipListSet<Integer> blSet;
    String filesDir;
    int bSize;
	
	DataManager(String name, LinkedBlockingQueue<dbOp> q1, LinkedBlockingQueue<dbOp> q2, ConcurrentSkipListSet<Integer> blSetIn, String dir, int size) 
    {
		threadName = name;
        tmsc = q1;
        tmsc = q2;
        blSet = blSetIn;
        filesDir = dir;
        bSize = size;
	}
    
    public void run() 
    {
		//code for DM goes here.
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