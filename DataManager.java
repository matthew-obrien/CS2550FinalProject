
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.concurrent.*;

class DataManager extends DBKernel implements Runnable {

    final private LinkedBlockingQueue<dbOp> scdm;
    final private LinkedBlockingQueue<dbOp> tmsc;
    final private ConcurrentSkipListSet<Integer> blSet;
    final private String filesDir;
    final private int bSize;
    //data structure that stores testing table content
    private static ConcurrentHashMap<Integer,Client> tableInMemory;
    //data buffer object, a hash map with key being ID and value being tuple
    private static ConcurrentHashMap<Integer,Client> dataBuffer;

    DataManager(String name, LinkedBlockingQueue<dbOp> q1, LinkedBlockingQueue<dbOp> q2, ConcurrentSkipListSet<Integer> blSetIn, String dir, int size) {
        threadName = name;
        tmsc = q1;
        scdm = q2;
        blSet = blSetIn;
        filesDir = dir;
        bSize = size;
        tableInMemory = new ConcurrentHashMap<Integer,Client>();
        dataBuffer = new ConcurrentHashMap<Integer,Client>();
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
    static void loadTableIntoMemory(String tableFilePath){ 
    	try {
    		//open the table script file
        	FileInputStream fstream = new FileInputStream(tableFilePath);
        	BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

        	String tupeLine;
        	//read tuples one by one
			while ((tupeLine = br.readLine()) != null)   {
			  System.out.println (tupeLine);
			  String[] tupeStrs = tupeLine.split(",");
			  Client client = new Client();
			  client.ID = Integer.parseInt(tupeStrs[0]);
			  client.ClientName = tupeStrs[1];
			  client.Phone = tupeStrs[2];
			  tableInMemory.put(client.ID, client);
			  System.out.println (client.ID+"---"+tableInMemory.size());
			}
			//Close the script stream
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
    }
    Client readRecordFromBuffer(int ID){
    	if(dataBuffer.contains(ID)){
    		//TODO
    	}else{
    		//TODO
    	}
    	return null;
    }
    void writeRecordToBuffer(int ID){
    	if(dataBuffer.contains(ID)){
    		//TODO
    	}else{
    		//TODO
    	}
    }
    /*
     * Used for testing functions
     */
//    public static void main(String[] args) {
//    	tableInMemory = new ConcurrentHashMap<Integer,Client>();
//    	loadTableIntoMemory("scripts/Y.txt");
//    }

}
/*
 * Table schema.    
 * This class could be written in a single individual file, at this moment, we define it here temporarily.
 */
class Client{
	//ID: 4-byte integer (Primary Key)
	//ClientName: 16-byte long string
	//Phone: 12-byte long string
	int ID;
	String ClientName;
	String Phone;
	
	//the flag that indicates whether the record has been updated or not.
	boolean isDirty = false;
	//the time stamp that indicates when this record was mostly recently used.
	long leastedUsageTimestamp = 0;
	public Client(){}
}
/*
 * Hashing structure class.    
 * This class could be written in a single individual file, at this moment, we define it here temporarily.
 */
class HashIndex{
	int MAXIMUM_BUCKET_SIZE = 6;
	ConcurrentHashMap<Integer,ConcurrentHashMap<Integer,Integer>> indexContainer;
	//store all the overflowed indices
	ConcurrentHashMap<Integer,Integer> overflowBucket ; 
	public HashIndex(){
		indexContainer = new ConcurrentHashMap<Integer,ConcurrentHashMap<Integer,Integer>>();
		overflowBucket = new ConcurrentHashMap<Integer,Integer>();
	}
	public void insert(int ID, int index){
		//hash function
		int key = ID%16;
		if(indexContainer.contains(key)){
			if(indexContainer.get(key).size()>=MAXIMUM_BUCKET_SIZE){
				//if the bucket's size surpass the bucket limit, then put it in the overflow bucket
				overflowBucket.put(ID, index);
			}else{
				indexContainer.get(key).put(key, index);
			}
		}else{
			ConcurrentHashMap<Integer,Integer> bucket = new ConcurrentHashMap<Integer,Integer>();
			bucket.put(ID, index);
			indexContainer.put(key, bucket);
		}
	}
	public int getIndex(int ID){
		int index = 0;
		return index;
	}
}
