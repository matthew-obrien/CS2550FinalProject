import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.*;

class Scheduler extends DBKernel implements Runnable {

    final private LinkedBlockingQueue<dbOp> tmsc;
    final private LinkedBlockingQueue<dbOp> scdm;
    final private AtomicBoolean twopl;
    final private HashMap<String, Lock> itemLocks = new HashMap<String, Lock>(); //given a primary key, any locks held on it
    final private HashMap<String, Lock> tableLocks = new HashMap<String, Lock>(); //given a table, any lock held on it
    final private HashMap<Integer, LinkedList<Lock>> transLocks = new HashMap<Integer, LinkedList<Lock>>(); //given a transaction ID, all locks that ID holds.
    
    

    Scheduler(String name, LinkedBlockingQueue<dbOp> q1, LinkedBlockingQueue<dbOp> q2, AtomicBoolean twoplin) {
        threadName = name;
        tmsc = q1;
        scdm = q2;
        twopl = twoplin;
    }

    @Override
    public void run() {
        try {
            //Carefull with the race condition here. If th sc get something and nothing new is added
            //it will break out of the loop. Maybe a external control thread initiated in main?
            while(true)
            {
                dbOp oper = tmsc.take();
                if(oper.op == null) //check for final message
                {
                    System.out.println("Final operation scheduled. SC exiting.");
                    scdm.add(oper);
                    return;
                }
                //System.out.println("\nSC has received the following operation:\n"+oper);
                if(twopl.get())
                {
                    //handle as twopl
                    scdm.add(oper);
                }
                else
                {
                    //handle as optimistic
                    scdm.add(oper);
                }
                
                
                
            }
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

class Lock {
    
    LinkedList<Integer> tIDs; //the tIDs which hold the lock. For write locks, it will only ever be one, but for reads it can be multiple.
    LinkedList<dbOp> waits; //the dbOps waiting on the lock
    
    Lock(int tID)
    {
        tIDs = new LinkedList<Integer>();
        tIDs.add(tID);
    }
    
    public boolean removeLock(int tID)
    {
        //given a tID, remove the lock it has on this. If that was the last lock, return true
        //if tID didn't hold the last lock on this item, return false.
        int index  = tIDs.indexOf(tID);
        if (index != -1)
        {
            tIDs.remove(index);
        }
        if(tIDs.size() == 0)
        {
            return true;
        }
        return false;
    }
    
}
