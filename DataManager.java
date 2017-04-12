import java.lang.Thread.*;
import java.util.*;
import java.util.concurrent.*;

class DataManager implements Runnable 
{
    private Thread t;
    private String threadName;
    LinkedBlockingQueue<dbOp> tmsc;
    LinkedBlockingQueue<dbOp> scdm;
	
	DataManager(String name, LinkedBlockingQueue<dbOp> q1, LinkedBlockingQueue<dbOp> q2) 
    {
		threadName = name;
        tmsc = q1;
        tmsc = q2;
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