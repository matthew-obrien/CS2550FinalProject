
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Random;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

class TransactionManager extends DBKernel implements Runnable {

    final private LinkedBlockingQueue<dbOp> tmsc;
    final private ConcurrentSkipListSet<Integer> blSet;
    final private String scriptsDir;
    final private ArrayList<LinkedList<dbOp>> loadedScripts = new ArrayList<LinkedList<dbOp>>();
    final private boolean rand;
    final private Random random;
    private Integer currTID = 0;
    private Short currRequestType = 0;

    TransactionManager(String name, LinkedBlockingQueue<dbOp> q1, ConcurrentSkipListSet<Integer> blSetIn, String dir) {
        threadName = name;
        tmsc = q1;
        blSet = blSetIn;
        scriptsDir = dir;
        random = new Random();
        rand = false;
    }
    
    TransactionManager(String name, LinkedBlockingQueue<dbOp> q1, ConcurrentSkipListSet<Integer> blSetIn, String dir, long seed) {
        threadName = name;
        tmsc = q1;
        blSet = blSetIn;
        scriptsDir = dir;
        random = new Random(seed);
        rand = true;
    }

    @Override
    public void run() {
        try {
            loadScripts();
            int i = 0; //initialize it now. Doesn't matter for random, but round robin needs to persist.
            while(loadedScripts.size() != 0)
            { 
                if(rand)
                {
                    //random implementation
                    i = random.nextInt(loadedScripts.size());
                    LinkedList<dbOp> opers = loadedScripts.get(i);
                    dbOp oper = opers.poll();
                    tmsc.add(oper);
                    if(opers.isEmpty())
                    {
                        loadedScripts.remove(i); //remove the list if it is now empty so it isn't chosen again.
                    }
                }
                else
                {
                    //round robin
                    LinkedList<dbOp> opers = loadedScripts.get(i);
                    dbOp oper = opers.poll();
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
        //Integer currentTransactionID = 0;
        for (File file : listOfFiles) {
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {

                String line = br.readLine();

                
                //Short transcationType = 0;

                LinkedList<dbOp> fileOperations = new LinkedList<dbOp>();

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
