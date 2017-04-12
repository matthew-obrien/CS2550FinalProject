import java.lang.Thread.*;

class TransactionManager implements Runnable 
{
    private Thread t;
    private String threadName;

	TransactionManager(String name) 
    {
		threadName = name;
	}
	
    public void run() 
    {
		System.out.println("Running " +  threadName );
        try 
        {
            for(int i = 4; i > 0; i--) 
            {
                System.out.println("Thread: " + threadName + ", " + i);
                // Let the thread sleep for a while.
                Thread.sleep(50);
            }
        }
        catch (InterruptedException e) 
        {
            System.out.println("Thread " +  threadName + " interrupted.");
        }
        System.out.println("Thread " +  threadName + " exiting.");
	}
    
    public void start () 
    {
      System.out.println("Starting " +  threadName );
      if (t == null) {
         t = new Thread (this, threadName);
         t.start ();
      }
   }
}