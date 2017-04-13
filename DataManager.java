
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.*;

class DataManager extends DBKernel implements Runnable {

    final private LinkedBlockingQueue<dbOp> scdm;
    final private LinkedBlockingQueue<dbOp> tmsc;
    final private ConcurrentSkipListSet<Integer> blSet;
    final private String filesDir;
    final private int bSize;

    DataManager(String name, LinkedBlockingQueue<dbOp> q1, LinkedBlockingQueue<dbOp> q2, ConcurrentSkipListSet<Integer> blSetIn, String dir, int size) {
        threadName = name;
        tmsc = q1;
        scdm = q2;
        blSet = blSetIn;
        filesDir = dir;
        bSize = size;
    }

    @Override
    public void run() {
        //code for DM goes here.
        try {
            while(true)
            {
                dbOp oper = scdm.take();
                /*if(oper.op == OperationType.Begin)*/ System.out.println("\nDM has received the following operation:\n"+oper);
                blSet.remove(oper.tID);
                if(oper.op == null)
                {
                    break;
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
    /*
     * Load the table script into memory
     */
    void loadTableIntoMemory(String tableFilePath){ 
    	try {
    		//open the table script file
        	FileInputStream fstream = new FileInputStream(tableFilePath);
        	BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

        	String tupeLine;
        	//read tuples one by one
			while ((tableFilePath = br.readLine()) != null)   {
			  System.out.println (tableFilePath);
			}
			//Close the script stream
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
    }
    /*
     * Table schema
     */
    class Client{
    	//ID: 4-byte integer (Primary Key)
    	//ClientName: 16-byte long string
    	//Phone: 12-byte long string
    	int ID;
    	String ClientName;
    	String Phone;
    }

}
