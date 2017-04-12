
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.concurrent.*;

class TransactionManager extends DBKernel implements Runnable {

    ConcurrentSkipListSet<Integer> blSet;
    private String scriptsDir;

    TransactionManager(String name, LinkedBlockingQueue<dbOp> q1, ConcurrentSkipListSet<Integer> blSetIn, String dir) {
        threadName = name;
        operationsEntryQueue = q1;
        blSet = blSetIn;
        scriptsDir = dir;
    }

    @Override
    public void run() {
        //code for TM goes here.
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

        for (File file : listOfFiles) {
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                StringBuilder sb = new StringBuilder();
                String line = br.readLine();

                while (line != null) {

                    sb.append(line);
                    sb.append(System.lineSeparator());

                    line = br.readLine();
                }
                String everything = sb.toString();

            }
        }

        System.out.println("All scripts within " + scriptsDir + " were loaded!");
    }

    private void operationParser(String line) throws Exception {
        OperationType opType = OperationType.decodeOperation(line);
        
    }

}
