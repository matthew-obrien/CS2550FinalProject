
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.*;

//<Matthew O'Brien>
class Scheduler extends DBKernel implements Runnable {

    final private TwoPhaseLock twoPLSC;
    final private LinkedBlockingQueue<dbOp> tmsc;
    final private LinkedBlockingQueue<dbOp> scdm;
    final private AtomicBoolean twopl;
    final private HashMap<String, Lock> itemLocks = new HashMap<String, Lock>(); //given a primary key, any locks held on it
    final private HashMap<String, Lock> tableLocks = new HashMap<String, Lock>(); //given a table, any lock held on it
    final private HashMap<Integer, LinkedList<Lock>> transLocks = new HashMap<Integer, LinkedList<Lock>>(); //given a transaction ID, all locks that ID holds.
    final private HashMap<Integer, HashSet<String>> readSch = new HashMap<Integer, HashSet<String>>();
    final private HashMap<Integer, HashSet<String>> writeSch = new HashMap<Integer, HashSet<String>>();
    final private HashMap<Integer, HashSet<Integer>> readFrom = new HashMap<Integer, HashSet<Integer>>();
    final private HashSet<Integer> aborted = new HashSet<Integer>(); //keeps track of transactions that aborted. If some Tj is both in some Ti's readFrom set and Tj aborted, Ti aborts.
    final private ConcurrentSkipListSet<Integer> blockingSet;
    final private ConcurrentSkipListSet<Integer> abortingSet;

    Scheduler(String name, LinkedBlockingQueue<dbOp> q1, LinkedBlockingQueue<dbOp> q2, AtomicBoolean twoplin, ConcurrentSkipListSet<Integer> blSet, ConcurrentSkipListSet<Integer> aSet) {
        threadName = name;
        tmsc = q1;
        scdm = q2;
        twopl = twoplin;
        twoPLSC = new TwoPhaseLock(scdm,blSet,aSet);
        blockingSet = blSet;
        abortingSet = aSet;
    }

    @Override
    public void run() {
        try {

            while (true) {
                dbOp oper = tmsc.take();
                if (oper.tID == -2) //check for change message
                {
                    scdm.add(oper);
                    boolean wait = twopl.get();
                    while (wait == twopl.get());
                    continue;
                }
                if (oper.op == null) //check for final message
                {
                    System.out.println("Final operation scheduled. SC exiting.");
                    scdm.add(oper);
                    return;
                }

                if (twopl.get()) {
                    twoPLSC.processOperation(oper);
                } else {
                    //</Matthew O'Brien>>
                    //System.out.println("\nSC has received the following operation:\n"+oper);
                    processOperationInOCC(oper);
                    //</Matthew O'Brien>
                }

            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void processOperationInOCC(dbOp oper) {
        //<Matthew O'Brien>
        //handle as optimistic, using 2PL certifier
        OperationType opType = oper.op;
        HashSet<String> readSet;
        HashSet<String> writeSet;
        HashSet<Integer> fromSet;
        switch (opType) {
            case Begin:
                readSet = new HashSet<String>();
                writeSet = new HashSet<String>();
                fromSet = new HashSet<Integer>();
                readSch.put(oper.tID, readSet);
                writeSch.put(oper.tID, writeSet);
                readFrom.put(oper.tID, fromSet);
                //if it's a process, add it to aborted immediately
                if (oper.type == 0) {
                    aborted.add(oper.tID);
                }
                //that's it
                scdm.add(oper);
                break;
            case Read:
                //treat processes differently
                if (oper.type == 0) {
                    //don't track process reads, since they don't count as conflicting and never abort
                    scdm.add(oper);
                    break;
                }
                //otherwise add the item to the readset
                readSet = readSch.get(oper.tID);
                readSet.add(oper.table + "," + oper.value);
                //now we need to update the readFrom set if necessary
                fromSet = readFrom.get(oper.tID);
                for (int key : writeSch.keySet()) {
                    writeSet = writeSch.get(key);
                    if (writeSet.contains(oper.table + "," + oper.value) || writeSet.contains(oper.table)) {
                        fromSet.add(key); //basically, if it's reading something an uncommitted T has written to, it may need to cascade aborts. Hence the set.
                    }
                }
                scdm.add(oper);
                break;
            case Write:
                //just add the item and move on
                writeSet = writeSch.get(oper.tID);
                String pk = getValueFromWrite(oper);
                writeSet.add(oper.table + "," + pk);
                writeSch.put(oper.tID, writeSet);
                //that's it
                scdm.add(oper);
                break;
            case MRead:
                //add the table to the readset and move on
                if (oper.type == 0) {
                    //don't track process reads, since they don't count as conflicting and never abort
                    scdm.add(oper);
                    break;
                }
                readSet = readSch.get(oper.tID);
                readSet.add(oper.table);
                readSch.put(oper.tID, readSet);
                scdm.add(oper);
                break;
            case Delete:
                //add table to writeset and move on
                writeSet = writeSch.get(oper.tID);
                writeSet.add(oper.table);
                writeSch.put(oper.tID, writeSet);
                scdm.add(oper);
                break;
            case Commit:
                //first skip everything if we're a process
                if (oper.type == 0) {
                    scdm.add(oper);
                    break;
                }
                //then check if we read from any aboreted transactions
                fromSet = readFrom.get(oper.tID);
                for (Integer readID : fromSet) {
                    //check each thing we read from to see if it aborted
                    if (aborted.contains(readID)) {
                        //then we read from an aborted tranasaction
                        dbOp newoper = new dbOp(oper.tID, oper.type, OperationType.Abort, null, null);
                        scdm.add(newoper);
                        readSch.remove(oper.tID);
                        writeSch.remove(oper.tID);
                        aborted.add(oper.tID);
                        System.out.println("Transaction " + oper.tID + " aborting. :(");
                        break;
                    }
                }
                if(aborted.contains(oper.tID))
                {
                    break;
                }
                //then the standard checks
                readSet = readSch.get(oper.tID);
                writeSet = writeSch.get(oper.tID);
                for (Integer key : readSch.keySet()) //for each other transaction's read lsit
                {
                    if (key == oper.tID) {
                        continue; //if it's our list, ignore it.
                    }
                    HashSet<String> theirSet = readSch.get(key);
                    for (String theirItem : theirSet) {
                        String[] bits = theirItem.split(",");
                        String table = bits[0];
                        if(readSet == null)
                        {
                            System.out.println("readset");
                            System.exit(0);
                        }
                        if(writeSet == null)
                        {
                            System.out.println("writeset");
                            System.exit(0);
                        }
                        if (readSet.contains(table) || writeSet.contains(table) || readSet.contains(theirItem) || writeSet.contains(theirItem))//if it's in our list, or the table is in our list, bad things
                        {
                            dbOp newoper = new dbOp(oper.tID, oper.type, OperationType.Abort, null, null);
                            scdm.add(newoper);
                            readSch.remove(oper.tID);
                            writeSch.remove(oper.tID);
                            aborted.add(oper.tID);
                            System.out.println("Transaction " + oper.tID + " aborting. :(");
                            break;
                        }
                        if(aborted.contains(oper.tID))
                        {
                            break;
                        }
                    }
                }
                if(aborted.contains(oper.tID))
                {
                    break;
                }
                for (Integer key : writeSch.keySet())//for each other transaction's write lsit
                {
                    if (key == oper.tID) {
                        continue; //if it's our list, ignore it.
                    }
                    HashSet<String> theirSet = readSch.get(key);
                    for (String theirItem : theirSet)//for each item in their list
                    {
                        String[] bits = theirItem.split(",");
                        String table = bits[0];
                        if (readSet.contains(table) || writeSet.contains(table) || readSet.contains(theirItem) || writeSet.contains(theirItem))//if it's in our list, or the table is in our list, bad things
                        {
                            dbOp newoper = new dbOp(oper.tID, oper.type, OperationType.Abort, null, null);
                            scdm.add(newoper);
                            readSch.remove(oper.tID);
                            writeSch.remove(oper.tID);
                            aborted.add(oper.tID);
                            System.out.println("Transaction " + oper.tID + " aborting. :(");
                            break;
                        }
                    }
                    if(aborted.contains(oper.tID))
                    {
                        break;
                    }
                }
                if(aborted.contains(oper.tID))
                {
                    break;
                }
                //if we're still alive here, commit
                readSch.remove(oper.tID);
                writeSch.remove(oper.tID);
                scdm.add(oper);
                break;
            case Abort:
                //we actually don't do anything other tahn remove all the schedules
                readSch.remove(oper.tID);
                writeSch.remove(oper.tID);
                aborted.add(oper.tID);
                //and that's it, send it along
                scdm.add(oper);
                break;
        }
    }

    public void start() {
        //standard start function
        if (t == null) {
            t = new Thread(this, threadName);
            t.start();
        }
    }

    private String getValueFromWrite(dbOp oper) //Matthew O'Brien
    {
        if (oper.op != OperationType.Write) {
            return null;
        }
        String tuple = oper.value;
        tuple = tuple.replace("(", "");
        tuple = tuple.replace(")", "");
        String[] values = tuple.split(",");
        return values[0];
    }
}

class Lock {

    LinkedList<Integer> tIDs; //the tIDs which hold the lock. For write locks, it will only ever be one, but for reads it can be multiple.
    LinkedList<dbOp> waits; //the dbOps waiting on the lock

    Lock(int tID) {
        tIDs = new LinkedList<Integer>();
        tIDs.add(tID);
    }

    public boolean removeLock(int tID) {
        //given a tID, remove the lock it has on this. If that was the last lock, return true
        //if tID didn't hold the last lock on this item, return false.
        int index = tIDs.indexOf(tID);
        if (index != -1) {
            tIDs.remove(index);
        }
        if (tIDs.size() == 0) {
            return true;
        }
        return false;
    }

}
