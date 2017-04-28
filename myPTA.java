import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

//<Matthew O'Brien>
public class myPTA
{
    public static void main (String[] args)
    {
        //First, verify correct number of arguments
        if(args.length != 3 && args.length != 4)
        {
            System.out.println("Incorrect number of args.");
            System.out.println("Use one of: \n\tjava myPTA scriptDirectory tablesDirectory bufferSize\n\tjava myPTA scriptDirectory tablesDirectory bufferSize seed");
            return;
        }
        //We can just grab the directory paths.
        String scriptsDir = args[0];
        String filesDir = args[1];
        //But we need to verify that the provided bufferSize is actually an integer. See Appendix (2).
        int bufferSize;
        try
        {
            bufferSize = Integer.parseInt(args[2]);
        } 
        catch (NumberFormatException e)
        {
            System.out.println("Given bufferSize is NaN.");
            System.out.println("Use one of: \n\tjava myPTA scriptDirectory tablesDirectory bufferSize\n\tjava myPTA scriptDirectory tablesDirectory bufferSize seed");
            return;
        }
        Long seed = null;
        if(args.length == 4)
        {
            try
            {
                seed = Long.parseLong(args[3]);
            } 
            catch (NumberFormatException e)
            {
                System.out.println("Given seed is NaN.");
                System.out.println("Use one of: \n\tjava myPTA scriptDirectory tablesDirectory bufferSize\n\tjava myPTA scriptDirectory tablesDirectory bufferSize seed");
                return;
            }
        }
        
        //Here, initialize all shared data structures
        LinkedBlockingQueue<dbOp> tmsc = new LinkedBlockingQueue<>(); //queue for operations being passed from the tm to the sc. Sometimes used by DM (for spontaneous aborts).
        LinkedBlockingQueue<dbOp> scdm = new LinkedBlockingQueue<>(); //queue for operations being passed from sc to dm.
        ConcurrentSkipListSet<Integer> blockingSet = new ConcurrentSkipListSet<>(); //description in Appendix (1)
        ConcurrentSkipListSet<Integer> abortingSet = new ConcurrentSkipListSet<>(); //transaction set to prematurely abort have their IDs put here
        AtomicBoolean twopl = new AtomicBoolean(true); //we start in 2pl
        
        //now intialize and start the threads
        TransactionManager tm;
        if(seed == null)
        {
            tm = new TransactionManager("TM", tmsc, blockingSet, scriptsDir, abortingSet, twopl);            
        }
        else
        {
            tm = new TransactionManager("TM", tmsc, blockingSet, scriptsDir, seed, abortingSet, twopl);            
        }
        Scheduler sc = new Scheduler("SC", tmsc, scdm, twopl,abortingSet,blockingSet);
        DataManager dm = new DataManager("DM", tmsc, scdm, blockingSet,filesDir,bufferSize, abortingSet, twopl);
        
        tm.start();
        sc.start();
        dm.start();
    }
}
//</Matthew O'Brien>

/*
||APPENDIX||~Matthew O'Brien

|NOTES|
1 - The blockingSet is the data structure used for operation series synchronization. Essentially, we don't want the TM to send multiple operations from the same
transaction/process out into the other threads because then if the transaction/process blocks or aborts the other threads need to handle the fact that there might be other
operations out there from that transaction/process that it now either needs to throw out or block behind the blocking operation. Instead, whenever the TM maintains a
mapping from transaction ID to operation series ID, and whenever it reads from a particular oepration series it then puts the transaction ID currently associated with
that series into the set. Then, if it would go to read from that series again, but the number is still in the set, it doesn't do it. When an operation finishes being dealt
with in the DM, then the DM accesses the set and removes the transaction ID of that operation from the set, which allows the TM to resume reading from the series. In other
threads, blockingSet is referred to as blSet. (mro25)

2 - We could probably check the paths, but I'm not sure that's worth it. It would be difficult to check, and we might as well just work around it.
For instance, we could have the DM create the tables folder if it doesn't exist, and if there's a problem with the path we give TM, it will
just crash, which we can catch, print an error message, then System.exit(0); (mro25)

*/