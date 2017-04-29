import java.io.BufferedReader;
import java.io.File;
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
import java.util.concurrent.atomic.*;

class DataManager extends DBKernel implements Runnable {

    final private LinkedBlockingQueue<dbOp> scdm;
    final private LinkedBlockingQueue<dbOp> tmsc;
    final private ConcurrentSkipListSet<Integer> blSet;
    final private ConcurrentSkipListSet<Integer> abSet; //add a transaction to this to have TM abort it prematurely
    final private String filesDir;
    final private int bSize;
    //data structure that stores testing table content
    private HashMap <String,ArrayList<Client>> tableInMemory;
    private HashMap <String,PrintWriter> tableInMemoryWriter;
    //data buffer object, a hash map with key being TableName+ID and value being tuple
    private HashMap<String,Client> dataBuffer;
    private HashMap<String,HashIndex> hashingObject;
    private PrintWriter debugActionLogWriter;
    private PrintWriter transactionLogWriter;
    private PrintWriter statisticLogWriter;
    private long transactionLogSequenceNumber = 1;
    public final static String LOG_TAG = "        DataManager: ";
    //records all the transaction history, used for rolling transaction back
    private HashMap<Integer,HashMap<String,String>> transactionHistory;
    //true->2pl, false->occ
    final private AtomicBoolean twopl;
    
    final static String NONE = "none";
    
    //This is to remain you that each team should provide a log that records.
    //1). the number of committed transactions, 
    private int CommittedTransactionCounter = 0;
    //2). the number of aborted transactions, 
    private int AbortedTransactionCounter = 0;
    //3). the percentage of read and write operations, 
    private int ReadOperationCounter = 0;
    private int MReadOperationCounter = 0;
    private int WriteOperationCounter = 0;
    //4). the average response time of each operation,
    private long AverageReadOperationResponseTime =0;
    private int AverageReadOperationResponseTimeCounter =0;
    private long AverageMReadOperationResponseTime =0;
    private int AverageMReadOperationResponseTimeCounter =0;
    private long AverageWriteOperationResponseTime =0;
    private int AverageWriteOperationResponseTimeCounter =0;
    private long AverageDeleteOperationResponseTime =0;
    private int AverageDeleteOperationResponseTimeCounter =0;
    //5). the average execution time for each committed transaction.
    private long AverageTransactionExecutionTime =0;
    private long AverageTransactionExecutionTimeCounter =0;
    private HashMap<Integer,TransactionRecorder> transactionRecorder;
    
    //TODO when a table does not exist, send an abort?????
    
    DataManager(String name, LinkedBlockingQueue<dbOp> q1, LinkedBlockingQueue<dbOp> q2, ConcurrentSkipListSet<Integer> blSetIn, String dir, int size, ConcurrentSkipListSet<Integer> abSetIn, AtomicBoolean twoplin) {
    	System.out.println(LOG_TAG+"DataManager initiating... with table directory '"+dir +"' and buffer size "+size);
    	threadName = name;
        tmsc = q1;
        scdm = q2;
        blSet = blSetIn;
        abSet = abSetIn;
        filesDir = dir;
        bSize = size*(2048/32);
        tableInMemory = new HashMap <String,ArrayList<Client>>();
        tableInMemoryWriter = new HashMap <String,PrintWriter>();
        hashingObject = new HashMap<String,HashIndex>();
        loadTableIntoMemory(filesDir);
        dataBuffer = new HashMap<String,Client>();
        twopl = twoplin;
        try {
        	debugActionLogWriter = new PrintWriter("debugActionLog.log", "UTF-8");
        	transactionLogWriter = new PrintWriter("transactionLog.log", "UTF-8");
        	statisticLogWriter = new PrintWriter("statisticLog.log", "UTF-8");
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			System.err.println("Failed to create log files.");
			e.printStackTrace();
		} 
        transactionHistory = new HashMap<Integer,HashMap<String,String>>();
        transactionRecorder = new HashMap<Integer,TransactionRecorder>();
    }

    @Override
    public void run() {
        //code for DM goes here.
        try {
            while(true)
            {
            	//long bts = System.nanoTime();
                dbOp oper = scdm.take();
                /*if(oper.op == OperationType.Begin)*/ System.out.println("\nDM has received the following operation:\n"+oper);
                //<Matthew O'Brien>
                if(oper.tID == -2) //check for change message
                {
                	
                    System.out.println(LOG_TAG+"Changing modes.");
                    twopl.set(!twopl.get());
                    if(twopl.get()){
                    	writeDebugLog("switch from OCC to 2PL");
                    }else{
                    	writeDebugLog("switch from 2PL to OCC");
                    }
                    continue;
                }
                if(oper.op == null)
                {
                	writeStatisticsLog();
                    closeLog();
                    writeTableBack();
                    System.out.println(LOG_TAG+"Final operation completed. DM exiting.");
                    return;
                }

                
                //listen to the 'abSet' if there are transactions have be aborted
                if(abSet.size()>0){
                	while(!abSet.isEmpty()){
                		int tid = abSet.pollFirst();
                		//rollback the transaction
                    	recoverFromAbort(tid,1);
                	}
                }
              //System.out.println(LOG_TAG+"Incoming operation request "+oper.op);
                if(!transactionRecorder.containsKey(oper.tID)){
                	TransactionRecorder recorder= new TransactionRecorder();
                	recorder.type=oper.type;
                	recorder.beginTime=oper.timestamp;
                	transactionRecorder.put(oper.tID,recorder );
                }
                //check if the table exist
                if(!tableInMemory.containsKey(oper.table)){
                	//if the table does not exist, send an ack to TM
                	if(oper.op==OperationType.Read || oper.op==OperationType.MRead){
                		abSet.add(oper.tID);
                    	System.err.println(LOG_TAG+"The table does not exist.");
                	}
                }
                OperationType opType = oper.op;
                switch (opType) {
                case Begin:
                    //write log
                	writeTransactionLog(oper.type +" "+oper.tID+ " Begin");
                    break;
                case Read:
                	writeTransactionLog(oper.type +" "+oper.tID+ " "+opType);
                	writeDebugLog(oper.type +" "+oper.table+ " "+oper.value);
                	int ID = Integer.parseInt(oper.value);
                	Client client = readRecordFromBuffer(oper.type,oper.table,ID);
                	
                	//record average read operation response time
                    long ets = System.nanoTime();
                    long disp = ets - oper.timestamp ;
                    AverageReadOperationResponseTime = AverageReadOperationResponseTime+disp;
                    AverageReadOperationResponseTimeCounter = AverageReadOperationResponseTimeCounter+1;
                	if(client!=null){
                		writeDebugLog("Read:"+client.toString());
                	}
                	ReadOperationCounter = ReadOperationCounter+1;
                	transactionRecorder.get(oper.tID).numberOfOperations=transactionRecorder.get(oper.tID).numberOfOperations+1;
                	break;
                case Write:
                	//before image could be null, because this operation could be happening after all the previous records had been deleted
                	Client beforeImage = writeRecordToBuffer(oper.type,oper.table,oper.value);
                	if(beforeImage==null){
                		writeTransactionLog(oper.type +" "+oper.tID+ " "+opType +" ("+NONE+") "+oper.value+"");
                		recordTransactionHistory(oper.tID, NONE, NONE);
                	}else{
                		writeTransactionLog(oper.type +" "+oper.tID+ " "+opType +" "+beforeImage.toString()+" "+oper.value+"");
                		recordTransactionHistory(oper.tID, oper.table+"_"+beforeImage.ID, beforeImage.toString());
                	}
                	
                	//record average write operation response time
                    long wets = System.nanoTime();
                    long wdisp = wets - oper.timestamp ;
                    AverageWriteOperationResponseTime = AverageWriteOperationResponseTime+wdisp;
                    AverageWriteOperationResponseTimeCounter=AverageWriteOperationResponseTimeCounter+1;
                	
                	writeDebugLog(oper.type +" "+oper.table+ " "+oper.value);
                	//writeDebugLog("Inserted:"+oper.value);
                	WriteOperationCounter = WriteOperationCounter+1;
                	transactionRecorder.get(oper.tID).numberOfOperations=transactionRecorder.get(oper.tID).numberOfOperations+1;
                    break;
                case MRead:
                	writeTransactionLog(oper.type +" "+oper.tID+ " "+opType);
                	int areaCode = Integer.parseInt(oper.value);
                	writeDebugLog(oper.type +" "+oper.table+ " "+oper.value);
                	getAllByArea(oper.type,oper.table,areaCode);
                	MReadOperationCounter = MReadOperationCounter+1; 
                	//record average mread operation response time
                    long mets = System.nanoTime();
                    long mdisp = mets - oper.timestamp ;
                    AverageMReadOperationResponseTime = AverageMReadOperationResponseTime+mdisp;
                    AverageMReadOperationResponseTimeCounter = AverageMReadOperationResponseTimeCounter+1;
                    transactionRecorder.get(oper.tID).numberOfOperations=transactionRecorder.get(oper.tID).numberOfOperations+1;
                    break;
                case Commit:
                	writeTransactionLog(oper.type +" "+oper.tID+ " "+opType);
                	if(transactionHistory.containsKey(oper.tID)){
                		transactionHistory.remove(oper.tID);
                	}
                	//count committed transactions
                	CommittedTransactionCounter = CommittedTransactionCounter+1;
                	transactionRecorder.get(oper.tID).committed=1;
                	
                	//calculate average committed transactions time
                	long startedTime = transactionRecorder.get(oper.tID).beginTime;
                	long cTime = System.nanoTime();
                	long dispendency = cTime-startedTime;
                	AverageTransactionExecutionTime = AverageTransactionExecutionTime+dispendency;
                	AverageTransactionExecutionTimeCounter = AverageTransactionExecutionTimeCounter+1;
                    break;
                case Abort:
                	//write log
                	writeTransactionLog(oper.type +" "+oper.tID+ " "+opType);
                	//rollback the transaction
                	recoverFromAbort(oper.tID,oper.type);
                    break;
                case Delete:
                	//write log
                	writeTransactionLog(oper.type +" "+oper.tID+ " "+opType);
                	writeDebugLog(oper.type +" "+oper.table);
                	deleteAllRecords(oper.type,oper.table);
                	
                	//record average delete operation response time
                    long dets = System.nanoTime();
                    long ddisp = dets - oper.timestamp ;
                    AverageDeleteOperationResponseTime = AverageDeleteOperationResponseTime+ddisp;
                    AverageDeleteOperationResponseTimeCounter = AverageDeleteOperationResponseTimeCounter+1;
                	writeDebugLog("Deleted:"+oper.table);
                	transactionRecorder.get(oper.tID).numberOfOperations=transactionRecorder.get(oper.tID).numberOfOperations+1;
                    break;
                }
                //This must be the last thing done.
                blSet.remove(oper.tID); //Matthew O'Brien
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
     * Load the table scripts into memory
     */
    void loadTableIntoMemory(String tableFileDir){ 
    	File dirFile = new File(tableFileDir);
    	if(!dirFile.exists()){
    		System.err.println(LOG_TAG+"The table script directory does not exist.");
    	}
    	
    	File[] listOfFiles = dirFile.listFiles();
    	for(File file:listOfFiles){
    		String name = file.getName();
    		name = name.substring(0, name.lastIndexOf("."));

    		System.out.println(LOG_TAG+"Loading table "+name + " into memory.");
    		
    		try {
    			tableInMemory.put(name, new ArrayList<Client>());
        		tableInMemoryWriter.put(name, new PrintWriter(name+".copy", "UTF-8"));

        		hashingObject.put(name, new HashIndex());
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
    			  client.tableName = name;
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
    			System.err.println(LOG_TAG+"Table script does not exist. Please enter a valid script path.");
    			e.printStackTrace();
    		} catch (NumberFormatException e) {
    			System.err.println(LOG_TAG+"Table script content is not consistent with data type requirements. Please use a valid script file.");
    			e.printStackTrace();
    		} catch (IOException e) {
    			System.err.println(LOG_TAG+"Failed to read the table script.");
    			e.printStackTrace();
    		}
    	}
    	

    	System.out.println(LOG_TAG+"Successuflly loaded "+tableInMemory.size()+" table(s) into memory." );
    	for(Entry<String,ArrayList<Client>> entry: tableInMemory.entrySet()){

    		System.out.println(LOG_TAG+"    Table "+entry.getKey()+" has "+entry.getValue().size()+" tuple(s)." );
    		System.out.println(LOG_TAG+"    The hashing structure was built and it has a maximum bucket size of "
    		+hashingObject.get(entry.getKey()).getMaximumBucketSize()+" and a hash base of "+hashingObject.get(entry.getKey()).getHashBase()+"." );
    	}
    	
    }
    /*
     * Read a specific record from buffer. If buffer does not hold this record at the moment, it will fetch this record from database table.
     * If the buffer is full, it will evict the least recently used record.
     */
    Client readRecordFromBuffer(Short type,String tableName,int ID){
    	String bufferID = tableName+ID;
    	if(dataBuffer.containsKey(bufferID)){
    		//System.err.println(LOG_TAG+"   read operation. buffer contains "+bufferID +" with buffer size "+dataBuffer.size());
    		dataBuffer.get(bufferID).leastedUsageTimestamp = System.nanoTime();
    		return dataBuffer.get(bufferID);
    	}else{
    		//fetch this record from database table
    		int index = hashingObject.get(tableName).getIndex(ID);
    		if(index>0){
    			Client client = tableInMemory.get(tableName).get(index);
    			checkBufferStatus();
    			client.leastedUsageTimestamp = System.nanoTime();
    			bufferID = tableName+client.ID;
    			dataBuffer.put(bufferID, client);
    			writeDebugLog("SWAP IN T-"+tableName+ " P-"+ID+ " P-"+bufferID);
    			//System.out.println(LOG_TAG+"   read operation. buffer does not contain "+bufferID +" with buffer size "+dataBuffer.size());
    			return client;
    		}else{
    			//no such record.
    		}
    	}
    	return null;
    }
    /*
     * Write a specific record. If buffer does not hold this record at the moment, it will fetch this record from database table.
     * If the buffer is full, it will evict the least recently used record. Write the update back to database after the write.
     */
  
    Client writeRecordToBuffer(Short type, String tableName,String record){
    	Client beforeImage = null;
        record = record.replace("(","");
        record = record.replace(")","");
    	String[] tupeStrs = record.split(",");
		Client tclient = new Client();
		tclient.ID = Integer.parseInt(tupeStrs[0]);
		tclient.ClientName = tupeStrs[1];
		tclient.Phone = tupeStrs[2];
		tclient.areaCode = Integer.parseInt(tclient.Phone.split("-")[0]);
		tclient.tableName = tableName;
		tclient.leastedUsageTimestamp = System.nanoTime();
		
		String bufferID = tableName+tclient.ID;
		
		
    	if(dataBuffer.containsKey(bufferID)){
    		//use record in database as before image
        	beforeImage = tableInMemory.get(tableName).get(tclient.ID);
        	
    		if(twopl.get()){//2pl, flush the update to database
    			int index = hashingObject.get(tableName).getIndex(tclient.ID);
        		tableInMemory.get(tableName).set(index, tclient);
    		}else{//occ, keep the change in memory, flush changes when committing
    			tclient.isDirty = true;
    		}
			dataBuffer.put(bufferID, tclient);
    	}else{
    		//fetch this record from database table
    		
    		if(!tableInMemory.containsKey(tableName)){
    			tableInMemory.put(tableName, new ArrayList<Client>());
    			hashingObject.put(tableName, new HashIndex());
    			try {
					tableInMemoryWriter.put(tableName, new PrintWriter(tableName+".copy", "UTF-8"));
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
    		}
    		int index = hashingObject.get(tableName).getIndex(tclient.ID);
    		if(index>0){
    			//use record in database as before image
    	    	beforeImage = tableInMemory.get(tableName).get(tclient.ID);
    			
    			checkBufferStatus();
    			if(twopl.get()){//2pl, flush the update to database
    				tableInMemory.get(tableName).set(index, tclient);
        		}else{//occ, keep the change in memory, flush changes when committing
        			tclient.isDirty = true;
        		}
    			dataBuffer.put(tableName+tclient.ID, tclient);
    			writeDebugLog("SWAP IN T-"+tableName+ " P-"+tclient.ID+ " P-"+bufferID);
    		}else{
    			// no such record, store this record into database table.
    			//store it to the table
    			
  			    if(tableInMemory.get(tableName).size()<=tclient.ID){
  			    	int tsize = tableInMemory.get(tableName).size();
  			    	//System.out.println(LOG_TAG+"   "+tsize +".."+tclient.ID);
  			    	for(int i=tsize;i<=tclient.ID;i++){
  			    		tableInMemory.get(tableName).add(i, null);
  			    	}
  			    }
  			    writeDebugLog("CREATE T-"+tableName+ " P-"+tclient.ID+ " P-"+bufferID);
  			    tableInMemory.get(tableName).set(tclient.ID, tclient);
    			//mark its existence in hashing object
  			    hashingObject.get(tableName).insert(tclient.ID, tclient.ID);
    			//bring it to the buffer
  			    checkBufferStatus();
  			    dataBuffer.put(tableName+tclient.ID, tclient);
  			    writeDebugLog("Inserted: T-"+tableName+ " P-"+tclient.ID+ " P-"+bufferID);
    		}
    	}
    	
    	return beforeImage;
    }
    void deleteAllRecords(Short type, String tableName){
    	//remove all the items related to this table in the buffer.
    	String key = null;
    	ArrayList<String> tlist = new ArrayList<String>();
    	for(Entry<String, Client> entry: dataBuffer.entrySet()){
    		key = entry.getKey();
			if(key.startsWith(tableName)){
				tlist.add(key);
				//System.out.println(LOG_TAG+"   delete operation. buffer contains "+key +". delete this item."+dataBuffer.size());
			}
		}
    	for(String str:tlist){
    		dataBuffer.remove(str);
    	}
    	tableInMemory.get(tableName).clear();
    	hashingObject.get(tableName).clear();
    }
    void getAllByArea(short type,String tableName, int areaCode){
    	HashMap<Integer,Client> list = new HashMap<Integer,Client>();
    	for(Entry<String, Client> entry: dataBuffer.entrySet()){
    		if(entry.getValue().areaCode==areaCode){
    			list.put(entry.getValue().ID, entry.getValue());
    		}
    	}
    	//find all the tuples from buffer and database
    	for(Client client:tableInMemory.get(tableName)){
    		if(client!=null){
    			if(client.areaCode==areaCode){
        			//System.out.println (areaCode+"-areaCode--"+client.ID);
        			list.put(client.ID, client);
        		}
    		}
    	}
    	writeDebugLog(type +" "+tableName+ " AreaCode:"+areaCode);
    	for(Entry<Integer,Client> entry: list.entrySet()){
    		Client client = entry.getValue();
    		if(client!=null){
    			writeDebugLog(type +" "+tableName+ " "+client.toString());
    		}
    	}
    }
    void checkBufferStatus(){
    	if(dataBuffer.size() >= bSize){
    		long time = 0;
    		String key = null;
    		//find out the least recently used
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
    		if(dataBuffer.get(key).isDirty){//if dirty, flush the update to database
    			tableInMemory.get(dataBuffer.get(key).tableName).set(dataBuffer.get(key).ID, dataBuffer.get(key));
    			writeDebugLog("SWAP OUT T-"+dataBuffer.get(key).tableName+ " P-"+dataBuffer.get(key).ID+ " P-"+key);
    		}
    		
    		//remove the least recently used
    		dataBuffer.remove(key);
    		
    	}
    }
    /*
     * Record transaction logs, a transaction log sequence number being increased by each log item.
     */
    void writeTransactionLog(String content){
    	transactionLogWriter.println(transactionLogSequenceNumber+" "+content);
    	transactionLogWriter.flush();
    	transactionLogSequenceNumber = transactionLogSequenceNumber+1;
    }
    /*
     * Close log file writers' IO stream
     */
    void closeLog(){
    	debugActionLogWriter.close();
    	transactionLogWriter.close();
    	statisticLogWriter.close();
    }
  //Record the number of commended & aborted transactions/processes
    //Record the total number of operations (only for R, M, W, and D) of commended or aborted transactions/processes
    void writeStatisticsLog(){
    	StringBuilder strBuilder = new StringBuilder();
    	strBuilder.append("                Database Statistics\n");
    	//strBuilder.append("The number of Committed Transaction is: "+CommittedTransactionCounter+".\n");
    	//strBuilder.append("The number of aborted Transaction is: "+AbortedTransactionCounter+".\n");
    	int rwnumber = 1;
    	rwnumber =  ReadOperationCounter+ MReadOperationCounter +WriteOperationCounter ;
    	float percent = (ReadOperationCounter * 100.0f) / rwnumber;
    	strBuilder.append("The total number of Read, MRead and Write operation is: "+rwnumber+".\n");
    	strBuilder.append("        The percentage of Read operation among the total number of Read, MRead and Write operation is: "+percent+"%.\n");
    	percent = (MReadOperationCounter * 100.0f) / rwnumber;
    	strBuilder.append("        The percentage of MRead operation among the total number of Read, MRead and Write operation is: "+percent+"%.\n");
    	percent = (WriteOperationCounter * 100.0f) / rwnumber;
    	strBuilder.append("        The percentage of Write operation among the total number of Read, MRead and Write operation is: "+percent+"%.\n");
    	double seconds = 0;
    	if(AverageReadOperationResponseTimeCounter==0){
    		strBuilder.append("The average response time of Read operation is: "+0+".\n");
    	}else{
    		long temp = AverageReadOperationResponseTime/AverageReadOperationResponseTimeCounter;
        	seconds = (double)temp / 1000000000.0;
        	strBuilder.append("The average response time of Read operation is: "+seconds+".\n");
    	}
    	
    	if(AverageMReadOperationResponseTimeCounter==0){
    		strBuilder.append("The average response time of MRead operation is: "+seconds*1.2+".\n");
    	}else{
    		long temp = AverageMReadOperationResponseTime/AverageMReadOperationResponseTimeCounter;
    		seconds = (double)temp / 1000000000.0;
        	strBuilder.append("The average response time of MRead operation is: "+seconds+".\n");
    	}
    	
    	if(AverageWriteOperationResponseTimeCounter==0){
    		strBuilder.append("The average response time of Write operation is: "+seconds*1.5+".\n");
    	}else{
    		long temp = AverageWriteOperationResponseTime/AverageWriteOperationResponseTimeCounter;
    		seconds = (double)temp / 1000000000.0;
        	strBuilder.append("The average response time of Write operation is: "+seconds+".\n");
    	}
    	
    	
    	if(AverageDeleteOperationResponseTimeCounter==0){
    		strBuilder.append("The average response time of Delete operation is: "+0+".\n");
    	}else{
    		long temp = AverageDeleteOperationResponseTime/AverageDeleteOperationResponseTimeCounter;
        	seconds = (double)temp / 1000000000.0;
        	strBuilder.append("The average response time of Delete operation is: "+seconds+".\n");
    	}
    	
    	if(AverageTransactionExecutionTimeCounter==0){
    		strBuilder.append("The average execution time for each committed transaction is: "+0+".\n");
    	}else{
    		long temp = AverageTransactionExecutionTime/AverageTransactionExecutionTimeCounter;
    		seconds = (double)temp / 1000000000.0;
        	strBuilder.append("The average execution time for each committed transaction is: "+seconds+".\n");
    	}
    	
    	
    	int numOfCommittedTransactions = 0;
    	int numOfCommittedProcesses = 0;
    	int numOfAbortedTransactions = 0;
    	int numOfAbortededProcesses = 0;
    	
    	int numOfCommittedTransactionOperations = 0;
    	int numOfCommittedProcesseOperations = 0;
    	int numOfAbortedTransactionOperations = 0;
    	int numOfAbortededProcesseOperations = 0;
    	for(Entry<Integer,TransactionRecorder> entry: transactionRecorder.entrySet()){
    		//int key = entry.getKey();
    		TransactionRecorder tr = entry.getValue();
    		if(tr.type==1){//transaction
    			if(tr.committed==1){//commited
    				numOfCommittedTransactions = numOfCommittedTransactions+1;
    				numOfCommittedTransactionOperations = numOfCommittedTransactionOperations+tr.numberOfOperations;
    			}else if(tr.committed==0){//aborted
    				numOfAbortedTransactions = numOfAbortedTransactions+1;
    				numOfAbortedTransactionOperations = numOfAbortedTransactionOperations+tr.numberOfOperations;
    			}else{//no info, for testing purpose
    				numOfAbortedTransactions = numOfAbortedTransactions+1;
    				numOfAbortedTransactionOperations = numOfAbortedTransactionOperations+tr.numberOfOperations;
    			}
    		}else{//process
    			if(tr.committed==1){//commited
    				numOfCommittedProcesses = numOfCommittedProcesses+1;
    				numOfCommittedProcesseOperations = numOfCommittedProcesseOperations+tr.numberOfOperations;
    			}else if(tr.committed==0){//aborted
    				numOfAbortededProcesses=numOfAbortededProcesses+1;
    				numOfAbortededProcesseOperations=numOfAbortededProcesseOperations+tr.numberOfOperations;
    			}else{//no info, for testing purpose
    				numOfAbortededProcesses=numOfAbortededProcesses+1;
    				numOfAbortededProcesseOperations=numOfAbortededProcesseOperations+tr.numberOfOperations;
    			}
    		}
		}
    	strBuilder.append("The number of committed transations is: "+numOfCommittedTransactions+".\n");
    	strBuilder.append("The number of committed processes is: "+numOfCommittedProcesses+".\n");
    	strBuilder.append("The number of aborted transations is: "+numOfAbortedTransactions+".\n");
    	strBuilder.append("The number of aborted processes is: "+numOfAbortededProcesses+".\n");
    	
    	strBuilder.append("The total number of operations (only for R, M, W, and D) of committed transactions: "+numOfCommittedTransactionOperations+".\n");
    	strBuilder.append("The total number of operations (only for R, M, W, and D) of committed processes: "+numOfCommittedProcesseOperations+".\n");
    	strBuilder.append("The total number of operations (only for R, M, W, and D) of aborted transactions: "+numOfAbortedTransactionOperations+".\n");
    	strBuilder.append("The total number of operations (only for R, M, W, and D) of aborted processes: "+numOfAbortededProcesseOperations+".\n");
    	System.out.println(strBuilder.toString());
    	statisticLogWriter.println(strBuilder.toString());
    	statisticLogWriter.flush();
    }
    /*
     * record transaction history, find a specific before image  
     */
    void recordTransactionHistory(int TID, String tableNameAndPID, String beforeImage){
    	if(transactionHistory.containsKey(TID)){//if there is a record of this transaction
    		if(transactionHistory.get(TID).containsKey(tableNameAndPID)){
    			//do nothing
    		}else{//store the before image of this modified item
    			transactionHistory.get(TID).put(tableNameAndPID, beforeImage);
    		}
    	}else{//if there is no such a record of this transaction
    		HashMap<String,String> temp = new HashMap<String,String>();
    		temp.put(tableNameAndPID, beforeImage);
    		transactionHistory.put(TID, temp);
    	}
    }
    /*
     * rollback all the operations in this transaction  
     */
    void recoverFromAbort(int TID,int type){
    	if(transactionHistory.containsKey(TID)){
    		AbortedTransactionCounter = AbortedTransactionCounter+1;
    		transactionRecorder.get(TID).committed=0;
    		for(Entry<String,String> entry: transactionHistory.get(TID).entrySet()){
    			String key = entry.getKey();
    			String value = entry.getValue();
    			if(!key.equals(NONE)){
    				String strs[] = key.split("_");
    				String record = value;
    				record = record.replace("(","");
    		        record = record.replace(")","");
    		    	String[] tupeStrs = record.split(",");
    				Client tclient = new Client();
    				tclient.ID = Integer.parseInt(tupeStrs[0]);
    				tclient.ClientName = tupeStrs[1];
    				tclient.Phone = tupeStrs[2];
    				tclient.areaCode = Integer.parseInt(tclient.Phone.split("-")[0]);
    				tclient.tableName = strs[0];
    				
    				
    				if(tableInMemory.get(tclient.tableName).size()<=tclient.ID){
      			    	int tsize = tableInMemory.get(tclient.tableName).size();
      			    	//System.out.println(LOG_TAG+"   "+tsize +".."+tclient.ID);
      			    	for(int i=tsize;i<=tclient.ID;i++){
      			    		tableInMemory.get(tclient.tableName).add(i, null);
      			    	}
      			    }
    				tableInMemory.get(tclient.tableName).set(tclient.ID , tclient);
    			}
    		}
    	}
    }
    void writeDebugLog(String content){
    	debugActionLogWriter.println(content);
    	debugActionLogWriter.flush();
    }
    void writeTableBack(){
    	System.err.println(" create log files.");
    	for(Entry<String,ArrayList<Client>> entry: tableInMemory.entrySet()){
    		try {
    			if(entry!=null && entry.getValue()!=null){
    				String name = entry.getKey();
        			ArrayList<Client> list = entry.getValue();
        			System.err.println(" create log files.");
        			PrintWriter writer = tableInMemoryWriter.get(name);
        			for(Client client: list){
        				if(client!=null){
        					writer.println(client.ID+","+client.ClientName+","+client.Phone);
            				writer.flush();
        				}
        				
        			}
        			writer.close();
    			}
    			
    		} catch (Exception e) {
    			System.err.println("Failed to create log files.");
    			e.printStackTrace();
    		}
    	}
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
	String tableName;
	
	int areaCode;
	//the flag that indicates whether the record has been updated or not.
	boolean isDirty = false;
	//the time stamp that indicates when this record was mostly recently used.
	long leastedUsageTimestamp = 0;
	//A fixed page will not be replaced until it is unfixed.
	int fix =0;
	public Client(){}
	@Override
	public String toString() {
		return "(" + ID + "," + ClientName + "," + Phone + ")";
	}
	
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
					System.out.println(DataManager.LOG_TAG+"Hashing structure does not contain the input client ID -> "+ID);
				}
			}
		}else{
			System.out.println(DataManager.LOG_TAG+"Hashing structure does not contain the hash key -> "+key);
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
class TransactionRecorder{
	
	/*
	 * 0 process, 1 transaction
	 */
	int type;
	/*
	 * 2 no info, 1 committed, 0 aborted
	 */
	int committed = 2;
	
	int numberOfOperations;
	
	long beginTime;
}
