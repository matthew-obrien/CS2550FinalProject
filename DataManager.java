
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
    private HashMap <String,CopyOnWriteArrayList<Client>> tableInMemory;
    //data buffer object, a hash map with key being ID and value being tuple
    private HashMap<String,Client> dataBuffer;
    private HashMap<String,HashIndex> hashingObject;
    private PrintWriter debugActionLogWriter;
    private PrintWriter transactionLogWriter;
    private long transactionLogSequenceNumber = 1;
    final private AtomicBoolean twopl;

    DataManager(String name, LinkedBlockingQueue<dbOp> q1, LinkedBlockingQueue<dbOp> q2, ConcurrentSkipListSet<Integer> blSetIn, String dir, int size, ConcurrentSkipListSet<Integer> abSetIn, AtomicBoolean twoplin) {
        threadName = name;
        tmsc = q1;
        scdm = q2;
        blSet = blSetIn;
        abSet = abSetIn;
        filesDir = dir;
        bSize = size;
        tableInMemory = new HashMap <String,CopyOnWriteArrayList<Client>>();
        hashingObject = new HashMap<String,HashIndex>();
        loadTableIntoMemory("tables");
        dataBuffer = new HashMap<String,Client>();
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
                /**/
                OperationType opType = oper.op;
                switch (opType) {
                case Begin:
                    //write log
                	writeTransactionLog(oper.type +" "+oper.tID+ " Begin");
                    break;
                case Read:
                	//int ID = Integer.parseInt(oper.value);
                	//readRecordFromBuffer(oper.table,ID);
                	break;
                case Write:
                	//writeRecordToBuffer(oper.value);
                    break;
                case MRead:
                	//int areaCode = Integer.parseInt(oper.value);
                	//getAllByArea(areaCode);
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
                	//deleteAllRecords();
                    break;
                }
                
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
     * Load the table scripts into memory
     */
    void loadTableIntoMemory(String tableFileDir){ 
    	File dirFile = new File(tableFileDir);
    	if(!dirFile.exists()){
    		System.out.println("The table script directory does not exist.");
    	}
    	
    	File[] listOfFiles = dirFile.listFiles();
    	for(File file:listOfFiles){
    		String name = file.getName();
    		name = name.substring(0, name.lastIndexOf("."));
    		System.out.println("Loading table "+name + " into memory.");
    		tableInMemory.put(name, new CopyOnWriteArrayList<Client>());
    		hashingObject.put(name, new HashIndex());
    		try {
        		ArrayList<Client> temp = new ArrayList<Client>();
        		int idCounter = 0;
        		//open the table script file
            	FileInputStream fstream = new FileInputStream(file);
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
    				tableInMemory.get(name).add(null);
    			}
    			
    			for(Client client: temp){
    				tableInMemory.get(name).set(client.ID, client);
    				//add it to the hashing object
    				hashingObject.get(name).insert(client.ID, client.ID);
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
    	/**/
    	System.out.println("Successuflly loaded "+tableInMemory.size()+" table(s) into memory." );
    	for(Entry<String,CopyOnWriteArrayList<Client>> entry: tableInMemory.entrySet()){
    		System.out.println("    Table "+entry.getKey()+" has "+entry.getValue().size()+" tuple(s)." );
    		System.out.println("    The hashing structure was built and it has a maximum bucket size of "
    		+hashingObject.get(entry.getKey()).getMaximumBucketSize()+" and a hash base of "+hashingObject.get(entry.getKey()).getHashBase()+"." );
    	}
    	
    }
    /*
     * Read a specific record from buffer. If buffer does not hold this record at the moment, it will fetch this record from database table.
     * If the buffer is full, it will evict the least recently used record.
     */
    Client readRecordFromBuffer(Short type,String tableName,int ID){
    	if(dataBuffer.containsKey(ID)){
    		dataBuffer.get(ID).leastedUsageTimestamp = System.currentTimeMillis();
    		return dataBuffer.get(ID);
    	}else{
    		//fetch this record from database table
    		int index = hashingObject.get(tableName).getIndex(ID);
    		if(index>0){
    			Client client = tableInMemory.get(tableName).get(index);
    			checkBufferStatus();
    			client.leastedUsageTimestamp = System.currentTimeMillis();
    			dataBuffer.put(tableName+client.ID, client);
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
  
    boolean writeRecordToBuffer(String tableName,String record){
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
			dataBuffer.put(tableName+tclient.ID, tclient);
    		int index = hashingObject.get(tableName).getIndex(tclient.ID);
    		tableInMemory.get(tableName).set(index, tclient);
			return true;
    	}else{
    		//fetch this record from database table
    		int index = hashingObject.get(tableName).getIndex(tclient.ID);
    		if(index>0){
    			
    			checkBufferStatus();
    			
    			dataBuffer.put(tableName+tclient.ID, tclient);
    			tableInMemory.get(tableName).set(index, tclient);
    			return true;
    		}else{
    			// no such record, store this record into database table.
    			//store it to the table
    			
  			    if(tableInMemory.size()<tclient.ID){
  			    	int tsize = tableInMemory.size()+1;
  			    	for(int i=tsize;i<tclient.ID;i++){
  			    		tableInMemory.get(tableName).set(i, null);
  			    	}
  			    }
  			  tableInMemory.get(tableName).set(tclient.ID, tclient);
    			//mark its existence in hashing object
  			    hashingObject.get(tableName).insert(tclient.ID, tclient.ID);
    			//bring it to the buffer
  			    checkBufferStatus();
  			    dataBuffer.put(tableName+tclient.ID, tclient);
    		}
    	}
    	return false;
    }
    void deleteAllRecords(String tableName){
    	//
    	dataBuffer.clear();
    	tableInMemory.get(tableName).clear();
    	hashingObject.get(tableName).clear();
    }
    void getAllByArea(String tableName, int areaCode){
    	//TODO
    	for(Client client:tableInMemory.get(tableName)){
    		if(client.areaCode==areaCode){
    			//System.out.println (areaCode+"-areaCode--"+client.ID);
    		}
    	}
    }
    void checkBufferStatus(){//TODO A fixed page will not be replaced until it is unfixed.
    	if(dataBuffer.size() >= 10){//TODO bSize
    		long time = 0;
    		String key = null;
    		for(Entry<String, Client> entry: dataBuffer.entrySet()){
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
    void writeTransactionLog(String content){
    	transactionLogWriter.println(transactionLogSequenceNumber+" "+content);
    	transactionLogSequenceNumber = transactionLogSequenceNumber+1;
    }
    void closeLog(){
    	debugActionLogWriter.close();
    	transactionLogWriter.close();
    }
    public static void main (String[] args)
    {
    	DataManager manager =new DataManager(null, null, null, null, null, 1, null, new AtomicBoolean(true));
    	//loadTableIntoMemory("tables");
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
	public int getMaximumBucketSize(){
		return MAXIMUM_BUCKET_SIZE;
	}
	public int getHashBase(){
		return HASH_BASE;
	}
}
