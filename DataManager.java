
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
    private CopyOnWriteArrayList<Client> tableInMemory;
    //data buffer object, a hash map with key being ID and value being tuple
    private ConcurrentHashMap<Integer,Client> dataBuffer;
    private HashIndex hashingObject;

    DataManager(String name, LinkedBlockingQueue<dbOp> q1, LinkedBlockingQueue<dbOp> q2, ConcurrentSkipListSet<Integer> blSetIn, String dir, int size) {
        threadName = name;
        tmsc = q1;
        scdm = q2;
        blSet = blSetIn;
        filesDir = dir;
        bSize = size;
        tableInMemory = new CopyOnWriteArrayList<Client>();
        hashingObject = new HashIndex();
        loadTableIntoMemory("scripts/Y.txt");
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
    void loadTableIntoMemory(String tableFilePath){ 
    	try {
    		ArrayList<Client> temp = new ArrayList<Client>();
    		int idCounter = 0;
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
			  if(idCounter<client.ID){
				  idCounter = client.ID;
			  }
			  temp.add(client);
			  System.out.println (client.ID+"---"+temp.size());
			}
			//Close the script stream
			br.close();
			
			//put all the tuples in, with ClientID as the index of tuple being positioned at
			for(int i =0;i<=idCounter;i++){
				tableInMemory.add(null);
			}
			for(Client client: temp){
				tableInMemory.set(client.ID, client);
				//add it to the hashing object
				hashingObject.insert(client.ID, client.ID);
			}
			//System.out.println ("---"+tableInMemory.size());
		} catch (FileNotFoundException e) {
			System.err.println("Table script does not exist. Please enter a valid script path.");
			e.printStackTrace();
		} catch (NumberFormatException e) {
			System.err.println("Table script content is not consistent with data type requirements. Please use a valid script file.");
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Failed to read the table script.");
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
    public static void main(String[] args) {
    	DataManager manager = new DataManager(null, null, null, null, null, 0);
    }

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
	int HASH_BASE = 16;
	private ConcurrentHashMap<Integer,ConcurrentHashMap<Integer,Integer>> indexContainer;
	//store all the overflowed indices
	private ConcurrentHashMap<Integer,Integer> overflowBucket ; 
	public HashIndex(){
		indexContainer = new ConcurrentHashMap<Integer,ConcurrentHashMap<Integer,Integer>>();
		overflowBucket = new ConcurrentHashMap<Integer,Integer>();
	}
	public void insert(int ID, int index){
		//hash function
		int key = hashFunction(ID);
		if(indexContainer.contains(key)){
			if(indexContainer.get(key).size()>=MAXIMUM_BUCKET_SIZE){
				//if the bucket's size surpass the bucket limit, then put it in the overflow bucket
				overflowBucket.put(ID, index);
			}else{
				indexContainer.get(key).put(ID, index);
			}
		}else{
			ConcurrentHashMap<Integer,Integer> bucket = new ConcurrentHashMap<Integer,Integer>();
			bucket.put(ID, index);
			indexContainer.put(key, bucket);
		}
	}
	/*
	 * search the hashing structure to get the primary key or position of the client record in the database table.
	 */
	public int getIndex(int ID){
		int index = 0;
		int key = hashFunction(ID);
		if(indexContainer.contains(key) && indexContainer.get(key).contains(ID)){
			index = indexContainer.get(key).get(ID);
		}else{
			if(overflowBucket.contains(ID)){
				index = overflowBucket.get(ID);
			}else{
				System.err.println("Hashing structure does not contain the input client ID -> "+ID);
			}
		}
		return index;
	}
	int hashFunction(int ID){
		return ID%HASH_BASE;
	}
}
