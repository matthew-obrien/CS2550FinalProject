
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

class DataManager extends DBKernel implements Runnable {

    final private LinkedBlockingQueue<dbOp> scdm;
    final private LinkedBlockingQueue<dbOp> tmsc;
    final private ConcurrentSkipListSet<Integer> blSet;
    final private ConcurrentSkipListSet<Integer> abSet; //add a transaction to this to have TM abort it prematurely
    final private String filesDir;
    final private int bSize;
    //data structure that stores testing table content
    private CopyOnWriteArrayList<Client> tableInMemory;
    //data buffer object, a hash map with key being ID and value being tuple
    private HashMap<Integer,Client> dataBuffer;
    private HashIndex hashingObject;
    private PrintWriter log1Writer;
    private PrintWriter log2Writer;
    final private AtomicBoolean twopl;

    DataManager(String name, LinkedBlockingQueue<dbOp> q1, LinkedBlockingQueue<dbOp> q2, ConcurrentSkipListSet<Integer> blSetIn, String dir, int size, ConcurrentSkipListSet<Integer> abSetIn, AtomicBoolean twoplin) {
        threadName = name;
        tmsc = q1;
        scdm = q2;
        blSet = blSetIn;
        abSet = abSetIn;
        filesDir = dir;
        bSize = size;
        tableInMemory = new CopyOnWriteArrayList<Client>();
        hashingObject = new HashIndex();
        loadTableIntoMemory("tables/Y.txt");
        dataBuffer = new HashMap<Integer,Client>();
        twopl = twoplin;
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
                ///*if(oper.op == OperationType.Begin)*/ System.out.println("\nDM has received the following operation:\n"+oper);
                
                if(oper.tID == -2) //check for change message
                {
                    System.out.println("Changing modes.");
                    twopl.set(!twopl.get());
                    continue;
                }
                if(oper.op == null)
                {
                    System.out.println("Final operation completed. DM exiting.");
                    return;
                }
                /*OperationType opType = oper.op;
                switch (opType) {
                case Begin:
                    
                    break;
                case Read:
                	int ID = Integer.parseInt(oper.value);
                	readRecordFromBuffer(ID);
                	break;
                case Write:
                	writeRecordToBuffer(oper.value);
                    break;
                case MRead:
                	int areaCode = Integer.parseInt(oper.value);
                	getAllByArea(areaCode);
                    break;
                case Commit:
                	//TODO a transaction can not commit if the transactions it depends on have not commit yet. So dependency graph is needed.
                    break;
                case Abort:
                	//if a transaction is aborted, all the transactions that depend on it must also been aborted. So dependency graph is needed. 
                	//And Before images must be kept for insuring the full recovery of the previous consistent state.
                	//Tell TM immediately after an abortion, stopping issuing new transactions.
                    break;
                case Delete:
                	deleteAllRecords();
                    break;
                }*/
                //This must be the last thing done.
                blSet.remove(oper.tID);
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
    static void loadTableIntoMemory(String tableFileDir){ 
    	File dirFile = new File(tableFileDir);
    	if(!dirFile.exists()){
    		System.out.println("The table script directory does not exist.");
    	}
    	
    	File[] listOfFiles = dirFile.listFiles();
    	for(File file:listOfFiles){
    		String name = file.getName();
    		System.out.println(name);
    	}
    	/*
    	try {
    		ArrayList<Client> temp = new ArrayList<Client>();
    		int idCounter = 0;
    		//open the table script file
        	FileInputStream fstream = new FileInputStream(tableFileDir);
        	BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

        	String tupeLine;
        	//read tuples one by one
			while ((tupeLine = br.readLine()) != null)   {
			  //System.out.println (tupeLine);
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
			  //System.out.println (client.ID+"---"+temp.size());
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
    	*/
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
  //TODO do not modify database before seeing commit operation and write ops down. Ask TA if operations of a transaction could be made to database
    //TODO if operations can only execute the data from its own transaction buffer?????? 
    //TODO should data manager have such a control logic??????
    
    //TODO if every write operation will go to database, 
    //does that mean that all the previous operations of the same transaction will be recovered 
    //as well as transactions that depend on this transaction will also have to be aborted if this transaction is aborted.
    
    //TODO undo when a transaction aborts. keep a before image and which transactions depend on it.
    boolean writeRecordToBuffer(String record){
        record = record.replace("(","");
    	String[] tupeStrs = record.split(",");
		Client tclient = new Client();
		tclient.ID = Integer.parseInt(tupeStrs[0]);
		tclient.ClientName = tupeStrs[1];
		tclient.Phone = tupeStrs[2];
		tclient.areaCode = Integer.parseInt(tclient.Phone.split("-")[0]);
		tclient.leastedUsageTimestamp = System.currentTimeMillis();
    	if(dataBuffer.containsKey(tclient.ID)){
    		//tclient.isDirty = true;
			dataBuffer.put(tclient.ID, tclient);
    		int index = hashingObject.getIndex(tclient.ID);
    		tableInMemory.set(index, tclient);
			return true;
    	}else{
    		//fetch this record from database table
    		int index = hashingObject.getIndex(tclient.ID);
    		if(index>0){
    			
    			checkBufferStatus();
    			
    			dataBuffer.put(tclient.ID, tclient);
        		tableInMemory.set(index, tclient);
    			return true;
    		}else{
    			// no such record, store this record into database table.
    			//store it to the table
    			
  			    if(tableInMemory.size()<tclient.ID){
  			    	int tsize = tableInMemory.size()+1;
  			    	for(int i=tsize;i<tclient.ID;i++){
  			    		tableInMemory.set(i, null);
  			    	}
  			    }
  			    tableInMemory.set(tclient.ID, tclient);
    			//mark its existence in hashing object
  			    hashingObject.insert(tclient.ID, tclient.ID);
    			//bring it to the buffer
  			    checkBufferStatus();
  			    dataBuffer.put(tclient.ID, tclient);
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
    			//System.out.println (areaCode+"-areaCode--"+client.ID);
    		}
    	}
    }
    void checkBufferStatus(){//TODO A fixed page will not be replaced until it is unfixed.
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
    		//System.out.println ("Evict---"+key);
    		dataBuffer.remove(key);
    	}
    }
    void closeLog(){
    	log1Writer.close();
    	log2Writer.close();
    }
//    public static void main (String[] args)
//    {
//    	//DataManager manager =new DataManager(null, null, null, null, null, 1, null, new AtomicBoolean(true));
//    	//manager.loadTableIntoMemory("tables");
//    	loadTableIntoMemory("tables");
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
	
	int areaCode;
	//the flag that indicates whether the record has been updated or not.
	boolean isDirty = false;
	//the time stamp that indicates when this record was mostly recently used.
	long leastedUsageTimestamp = 0;
	//TODO A fixed page will not be replaced until it is unfixed.
	int fix =0;
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
