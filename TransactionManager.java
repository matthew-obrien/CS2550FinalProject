import java.util.concurrent.atomic.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Random;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Arrays;

class TransactionManager extends DBKernel implements Runnable {

    final private LinkedBlockingQueue<dbOp> tmsc;
    final private ConcurrentSkipListSet<Integer> blSet;
    final private ConcurrentSkipListSet<Integer> abSet; //if a transaction is added to this, abort it prematurely.
    final private String scriptsDir;
    final private ArrayList<LinkedList<dbOp>> loadedScripts = new ArrayList<>();
    final private boolean rand;
    final private Random random;
    private Integer currTID = 0;
    private Short currRequestType = 0;
    private int[] tIDMappings;
    final private AtomicBoolean twopl;
    final private boolean changing = false; //is set to true when moving from OCC to 2pl, and determines whether a new transaction will be allowed to start.
    
    TransactionManager(String name, LinkedBlockingQueue<dbOp> q1, ConcurrentSkipListSet<Integer> blSetIn, String dir, ConcurrentSkipListSet<Integer> abSetIn, AtomicBoolean twoplin) {
        threadName = name;
        tmsc = q1;
        blSet = blSetIn;
        abSet = abSetIn;
        scriptsDir = dir;
        random = new Random();
        rand = false;
        twopl = twoplin;
    }
    
    TransactionManager(String name, LinkedBlockingQueue<dbOp> q1, ConcurrentSkipListSet<Integer> blSetIn, String dir, long seed, ConcurrentSkipListSet<Integer> abSetIn, AtomicBoolean twoplin) {
        threadName = name;
        tmsc = q1;
        blSet = blSetIn;
        abSet = abSetIn;
        scriptsDir = dir;
        random = new Random(seed);
        rand = true;
        twopl = twoplin;
    }

    @Override
    public void run() {
        try {
            loadScripts();
            int i = 0; //initialize it now. Doesn't matter for random, but round robin needs to persist.
            while(!loadedScripts.isEmpty())
            { 
                if(rand)
                {
                    //random implementation
                    i = random.nextInt(loadedScripts.size());
                    while(blSet.contains(tIDMappings[i]))
                    {
                        i = (i+1)%loadedScripts.size();
                    }
                    LinkedList<dbOp> opers = loadedScripts.get(i);
                    dbOp oper = opers.poll();
                    blSet.add(oper.tID);
                    tIDMappings[i] = oper.tID;
                    //System.out.println("\nTM has sent the following operation:\n"+oper);
                    tmsc.add(oper);
                    if(opers.isEmpty())
                    {
                        loadedScripts.remove(i); //remove the list if it is now empty so it isn't chosen again.
                    }
                }
                else
                {
                    //round robin
                    while(blSet.contains(tIDMappings[i]))
                    {
                        i = (i+1)%loadedScripts.size();
                    }
                    LinkedList<dbOp> opers = loadedScripts.get(i);
                    dbOp oper = opers.poll();
                    blSet.add(oper.tID);
                    tIDMappings[i] = oper.tID;
                    //System.out.println("\nTM has sent the following operation:\n"+oper);
                    tmsc.add(oper);
                    if(opers.isEmpty())
                    {
                        loadedScripts.remove(i);
                        if(i == loadedScripts.size()) i = 0; //if we just removed the last list in the list, set i to 0
                    }
                    else
                    {
                        i = (i+1)%loadedScripts.size();
                    }
                    
                }
            }
            dbOp end = new dbOp(-1, (short)-1, null, null, null);
            tmsc.add(end);
            System.out.println("Final operation read and sent. TM exiting.");
            return;            
        } catch (Exception ex) {
            Logger.getLogger(TransactionManager.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void start() {
        //standard start function
        if (t == null) {
            t = new Thread(this, threadName);
            t.start();
        }
    }

    private void loadScripts() throws Exception {
        System.out.println("Starting script files load procedure");

        ArrayList<File> listOfFiles = findOnlyFiles(scriptsDir);
        tIDMappings = new int[listOfFiles.size()]; //intialize the mapping array
        Arrays.fill(tIDMappings, -1);               //and fill it with -1s to start
        for (File file : listOfFiles) {
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                String line = br.readLine();
                LinkedList<dbOp> fileOperations = new LinkedList<>();

                while (line != null) {
                    dbOp operation = operationParser(line);
                    fileOperations.add(operation);
                    line = br.readLine();
                }
                loadedScripts.add(fileOperations);
            }
        }
        System.out.println("All scripts within " + scriptsDir + " were loaded!");
    }

    private dbOp operationParser(String line) throws Exception {
        String[] operationSymbols = line.split(" ");

        OperationType opType = OperationType.decodeOperation(line);

        dbOp op = new dbOp();
        op.op = opType;

        // Transaction ID and type are globals to all operations within a Begin-Commit/Abort so I just passed them as refs
        // and update when we have a new Begin, which means a new transacation with possibly a different type.
        // Maybe a unecessary level of granularity. But lets keep it here. We may need to add some other control flags 
        switch (opType) {
            case Begin:
                currRequestType = (short) (line.charAt(2) == '0' ? 0 : 1);
                currTID++;
                break;
            case Write:
                op.table = operationSymbols[1];
                op.value = operationSymbols[2];
                break;
            case Read:
                op.table = operationSymbols[1];
                op.value = operationSymbols[2];
                break;
            case MRead:
                op.table = operationSymbols[1];
                op.value = operationSymbols[2];
                break;
            case Commit:
                break;
            case Abort:
                break;
            case Delete:
                op.table = operationSymbols[1];
                break;
        }
        
        op.tID = currTID;
        op.type = currRequestType;
        return op;
    }
}
