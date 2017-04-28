
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
    private AtomicBoolean twopl;
    private boolean changing = false; //is set to true when moving from OCC to 2pl, and determines whether a new transaction will be allowed to start.
    private boolean[] reads;
    private int readsInd = 0;

    //<Matthew O'Brien>
    TransactionManager(String name, LinkedBlockingQueue<dbOp> q1, ConcurrentSkipListSet<Integer> blSetIn, String dir, ConcurrentSkipListSet<Integer> abSetIn, AtomicBoolean twoplin) {
        threadName = name;
        tmsc = q1;
        blSet = blSetIn;
        abSet = abSetIn;
        scriptsDir = dir;
        random = new Random();
        rand = false;
        twopl = twoplin;
        reads = new boolean[15];
        for (int i = 0; i < 15; i++) {
            reads[i] = false;
        }
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
        reads = new boolean[15];
        for (int i = 0; i < 15; i++) {
            reads[i] = true;
        }
    }
    //</Matthew O'Brien>

    @Override
    public void run() {
        try {
            loadScripts();
            //<Matthew O'Brien>
            int i = 0; //initialize it now. Doesn't matter for random, but round robin needs to persist.
            while (!loadedScripts.isEmpty()) {
                OperationType operation;
                if (rand) {
                    //random implementation
                    i = random.nextInt(loadedScripts.size());
                    int loop = i;
                    boolean changed = false;
                    while (blSet.contains(tIDMappings[i]) || (changing && loadedScripts.get(i).peek().op == OperationType.Begin)) {
                        i = (i + 1) % loadedScripts.size();
                        if (i == loop && changing) //than we may be ready to switch modes
                        {
                            boolean change = true;
                            for (int j = 0; j < loadedScripts.size(); j++) {
                                //check to see if any of the remaining dudes are on anything other than a begin as their next operation.
                                dbOp oper1 = loadedScripts.get(j).peek();
                                //System.out.println(oper1.op == OperationType.Begin);
                                if (!(oper1.op == OperationType.Begin)) {
                                    change = false; //if any transaction is still going, don't change yet
                                }
                            }
                            //if we get here, then we have no active transactions. Send the message to change.
                            if (change) {
                                //System.out.println("Changing modes.");
                                dbOp changeMsg = new dbOp(-2, (short) -1, null, null, null);
                                changed = true;
                                tmsc.add(changeMsg);
                                boolean match = twopl.get();
                                int x = 0;
                                while (match == twopl.get()) {
                                    x++; //wait until the scheduler changes the modes
                                }                                //System.out.println(x);
                                break;
                            }
                        }
                    }
                    if (changed) {
                        changing = false;
                        continue; //and finally, if we changed modes, continue to the next loop
                    }

                    //now get the operation and act
                    LinkedList<dbOp> opers = loadedScripts.get(i);
                    dbOp oper = opers.poll();
                    if (abSet.contains(oper.tID))//then the transaction has been scheduled for early abort.
                    {
                        while (oper.op != OperationType.Commit && oper.op != OperationType.Abort) {
                            oper = opers.poll(); //go until the commit or abort
                        }
                        oper.op = OperationType.Abort;
                    }
                    operation = oper.op;
                    blSet.add(oper.tID);
                    tIDMappings[i] = oper.tID;
                    //System.out.println("\nTM has sent the following operation:\n"+oper);
                    tmsc.add(oper);
                    if (opers.isEmpty()) {
                        loadedScripts.remove(i); //remove the list if it is now empty so it isn't chosen again.
                        for(int j = i; j < tIDMappings.length-1; j++)
                        {
                            tIDMappings[j] = tIDMappings[j+1]; //if a list is removed, make sure the mappings are still right.
                        }
                    }
                } else {
                    //round robin
                    boolean changed = false;
                    int loop = i;
                    while (blSet.contains(tIDMappings[i]) || (changing && loadedScripts.get(i).peek().op == OperationType.Begin)) {
                        i = (i + 1) % loadedScripts.size();
                        if (i == loop && changing) //than we may be ready to switch modes
                        {
                            boolean change = true;
                            for (int j = 0; j < loadedScripts.size(); j++) {
                                //check to see if any of the remaining dudes are on anything other than a begin as their next operation.
                                dbOp oper1 = loadedScripts.get(j).peek();
                                //System.out.println(oper1.op == OperationType.Begin);
                                if (!(oper1.op == OperationType.Begin)) {
                                    change = false; //if any transaction is still going, don't change yet
                                }
                            }
                            //if we get here, then we have no active transactions. Send the message to change.
                            if (change) {
                                //System.out.println("Changing modes.");
                                dbOp changeMsg = new dbOp(-2, (short) -1, null, null, null);
                                changed = true;
                                tmsc.add(changeMsg);
                                boolean match = twopl.get();
                                int x = 0;
                                while (match == twopl.get()) {
                                    x++; //wait until the scheduler changes the modes
                                }                                //System.out.println(x);
                                break;
                            }
                        }
                    }

                    if (changed) {
                        changing = false;
                        continue; //and finally, if we changed modes, continue to the next loop
                    }
                    //now get the operation and stuff
                    LinkedList<dbOp> opers = loadedScripts.get(i);
                    dbOp oper = opers.poll();
                    if (abSet.contains(oper.tID))//then the transaction has been scheduled for early abort.
                    {
                        while (oper.op != OperationType.Commit && oper.op != OperationType.Abort) {
                            oper = opers.poll(); //go until the commit or abort
                        }
                        oper.op = OperationType.Abort;
                    }
                    operation = oper.op;
                    blSet.add(oper.tID);
                    tIDMappings[i] = oper.tID;
                    //System.out.println("\nTM has sent the following operation:\n"+oper);
                    tmsc.add(oper);
                    if (opers.isEmpty()) {
                        loadedScripts.remove(i);
                        for(int j = i; j < tIDMappings.length-1; j++)
                        {
                            tIDMappings[j] = tIDMappings[j+1]; //if a list is removed, make sure the mappings are still right.
                        }
                        if (i == loadedScripts.size()) {
                            i = 0; //if we just removed the last list in the list, set i to 0
                        }
                    } else {
                        i = (i + 1) % loadedScripts.size();
                    }

                }
                upDateReadTable(operation);
                checkIfModeShouldChange();
            }
            dbOp end = new dbOp(-1, (short) -1, null, null, null);
            tmsc.add(end);
            System.out.println("Final operation read and sent. TM exiting.");
            return;
        } catch (Exception ex) {
            Logger.getLogger(TransactionManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        //</Matthew O'Brien>
    }

    private void upDateReadTable(OperationType operation) {
        //update the reads table
        if (operation == OperationType.Read || operation == OperationType.MRead) {
            reads[readsInd] = true;
            readsInd = (readsInd + 1) % reads.length;
        } else if (operation == OperationType.Write) {
            reads[readsInd] = false;
            readsInd = (readsInd + 1) % reads.length;
        }
    }

    private void checkIfModeShouldChange() {
        //see if we should be switching modes or not
        if (twopl.get()) {
            //if we're in twopl, we check if we shoudl switch to OCC or, if we are switching, if we should stop switching.
            if (mostlyReads()) {
                changing = true;
            }
            if (enoughWrites())//both condition can't be true, but there are situations where neither is, so no else statment
            {
                changing = false;
            }
        } else {
            //if we're in OCC, check if we should be switching, or if we are switching if we should stop.
            if (mostlyReads()) {
                changing = false;
            }
            if (enoughWrites())//both condition can't be true, but there are situations where neither is, so no else statment
            {
                changing = true;
            }
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

    //<Matthew O'Brien>
    private boolean mostlyReads() //if there are enough reads in the buffer to switch modes.
    {
        int count = 0;
        for (int i = 0; i < reads.length; i++) {
            if (reads[i]) {
                count++;
            }
        }
        if (count > 12) {
            return true;
        }
        return false;
    }

    private boolean enoughWrites() //if there are enough writes in the buffer to switch modes.
    {
        int count = 0;
        for (int i = 0; i < reads.length; i++) {
            if (reads[i]) {
                count++;
            }
        }
        if (count < 8) {
            return true;
        }
        return false;
    }
    //</Matthew O'Brien>
}
