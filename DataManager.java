
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
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
    private HashMap<Integer,Client> dataBuffer;
    private HashIndex hashingObject;
    private PrintWriter log1Writer;
    private PrintWriter log2Writer;

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
        dataBuffer = new HashMap<Integer,Client>();
//        try {
//			log1Writer = new PrintWriter("log1.log", "UTF-8");
//			log2Writer = new PrintWriter("log2.log", "UTF-8");
//		} catch (FileNotFoundException | UnsupportedEncodingException e) {
//			System.err.println("Failed to read the table script.");
//			e.printStackTrace();
//		} 
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
        closeLog();
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
			  client.areaCode = Integer.parseInt(client.Phone.split("-")[0]);
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
    /*
     * Read a specific record from buffer. If buffer does not hold this record at the moment, it will fetch this record from database table.
     * If the buffer is full, it will evict the least recently used record.
     */
    Client readRecordFromBuffer(int ID){
    	if(dataBuffer.containsKey(ID)){
    		dataBuffer.get(ID).leastedUsageTimestamp = System.currentTimeMillis();
    		return dataBuffer.get(ID);
    	}else{
    		//fetch this record from database table
    		int index = hashingObject.getIndex(ID);
    		if(index>0){
    			Client client = tableInMemory.get(index);
    			checkBufferStatus();
    			client.leastedUsageTimestamp = System.currentTimeMillis();
    			dataBuffer.put(client.ID, client);
    			return client;
    		}else{
    			//TODO no such record
    		}
    	}
    	return null;
    }
    /*
     * Write a specific record. If buffer does not hold this record at the moment, it will fetch this record from database table.
     * If the buffer is full, it will evict the least recently used record. Write the update back to database after the write.
     */
    boolean writeRecordToBuffer(int ID){
    	if(dataBuffer.containsKey(ID)){
    		dataBuffer.get(ID).leastedUsageTimestamp = System.currentTimeMillis();
    		Client client = dataBuffer.get(ID);//TODO update attributes
    		int index = hashingObject.getIndex(ID);
    		tableInMemory.set(index, client);
			return true;
    	}else{
    		//fetch this record from database table
    		int index = hashingObject.getIndex(ID);
    		if(index>0){
    			Client client =  tableInMemory.get(index);
    			checkBufferStatus();
    			client.leastedUsageTimestamp = System.currentTimeMillis();
    			//TODO update attributes
    			tableInMemory.set(index, client);
    			dataBuffer.put(client.ID, client);
    			return true;
    		}else{
    			//TODO no such record
    		}
    	}
    	return false;
    }
    void deleteAllRecords(){
    	//TODO
    	dataBuffer.clear();
    	tableInMemory.clear();
    	hashingObject.clear();
    }
    void getAllByArea(int areaCode){
    	//TODO
    	for(Client client:tableInMemory){
    		if(client.areaCode==areaCode){
    			System.out.println (areaCode+"-areaCode--"+client.ID);
    		}
    	}
    }
    void checkBufferStatus(){
    	if(dataBuffer.size() >= 10){//TODO bSize
    		long time = 0;
    		int key = 0;
    		for(Entry<Integer, Client> entry: dataBuffer.entrySet()){
    			if(time==0){
    				time = entry.getValue().leastedUsageTimestamp;
    				key = entry.getKey();
    			}else{
    				if(entry.getValue().leastedUsageTimestamp<time){
        				time = entry.getValue().leastedUsageTimestamp;
        				key = entry.getKey();
        			}
    			}
    		}
    		System.out.println ("Evict---"+key);
    		dataBuffer.remove(key);
    	}
    }
    void closeLog(){
    	log1Writer.close();
    	log2Writer.close();
    }
    /*
     * Used for testing functions
     */
    public static void main(String[] args) {
    	DataManager manager = new DataManager(null, null, null, null, null, 16);
//    	for(int i =1;i<20;i++){
//    		manager.writeRecordToBuffer(i);
//    	}
//    	for(Entry<Integer, Client> entry: manager.dataBuffer.entrySet()){
//    		System.out.println (entry.getKey()+"---"+entry.getValue().ID);
//    		System.out.println ("---"+manager.tableInMemory.get(entry.getKey()).leastedUsageTimestamp);
//    	}
    	//manager.getAllByArea(412);
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
	
	int areaCode;
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
	private int MAXIMUM_BUCKET_SIZE = 6;
	private int HASH_BASE = 16;
	private  HashMap<Integer,HashMap<Integer,Integer>> indexContainer;
	//store all the overflowed indices
	private HashMap<Integer,Integer> overflowBucket ; 
	public HashIndex(){
		indexContainer = new HashMap<Integer,HashMap<Integer,Integer>>();
		overflowBucket = new HashMap<Integer,Integer>();
	}
	public void insert(int ID, int index){
		//hash function
		int key = hashFunction(ID);
		if(indexContainer.containsKey(key)){
			if(indexContainer.get(key).size()>MAXIMUM_BUCKET_SIZE){
				//if the bucket's size surpass the bucket limit, then put it in the overflow bucket
				overflowBucket.put(ID, index);
			}else{
				indexContainer.get(key).put(ID, index);
			}
		}else{
			HashMap<Integer,Integer> bucket = new HashMap<Integer,Integer>();
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
		
		if(indexContainer.containsKey(key)){
			if(indexContainer.get(key).containsKey(ID)){
				index = indexContainer.get(key).get(ID);
			}else{
				if(overflowBucket.containsKey(ID)){
					index = overflowBucket.get(ID);
				}else{
					System.err.println("Hashing structure does not contain the input client ID -> "+ID);
				}
			}
		}else{
			System.err.println("Hashing structure does not contain the input client ID -> "+ID);
		}
		return index;
	}
	public void clear(){
		indexContainer.clear();
		overflowBucket.clear();
	}
	private int hashFunction(int ID){
		return ID%HASH_BASE;
	}
}
