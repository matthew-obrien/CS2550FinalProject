public class dbOp
{
    public int tID; //transaction or process ID. Strictly increasing starting at 0 with the first transaction.
    public short type; //0 or 1. O indicates a process, 1 indicates a transaction.
    public OperationType op; //indicates the operation. Different values listed in comment table below.
    public String table; //indicates what table the oepration acts upon, if any. Null if not applicable.
    public String value; //A string representation of the value/areacode/(t) used by the application. Null if N/A.
    public Long timestamp = System.nanoTime();; //A string representation of the value/areacode/(t) used by the application. Null if N/A.
    
    
    dbOp(){}
    
    dbOp(int tID, short type, OperationType op, String table, String value)
    {
        this.tID = tID;
        this.type = type;
        this.op = op;
        this.table = table;
        this.value = value;
        timestamp = System.nanoTime();
    }
    
    public String toString()
    {
        return "TID:\t"+tID+"\nTYPE:\t"+type+"\nOP:\t"+op+"\nTABLE:\t"+table+"\nVALUE:\t"+value+"\n";
    }
}

/*
||APPENDIX||

|OPERATION VALUE TABLE|
0:B - Of form "B EMode". Begins a transaction/process of type EMode.
1:C - Of form "C". Commits the current transaction/process.
2:A - Of form "A". Aborts the current transaction/process (for processes, this is identical to commit).
3:R - Of form "R table value". Reads value from table.
4:M - Of form "M table areacode". Reads all values with matching area-code from table.
5:W - Of form "W table (t)". Writes tuple t to table W. If the primary key in t is already in the table, the existing tuple is overwritten with the new one.
6:D - Of form "D table". Deletes table from the database.

*/