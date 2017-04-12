import java.util.*;
import java.lang.Thread;

public class myPTA
{

    
    public static void main (String[] args)
    {
        TransactionManager tm = new TransactionManager("TM");
        Scheduler sc = new Scheduler("SC");
        DataManager dm = new DataManager("DM");
        tm.start();
        sc.start();
        dm.start();
    }
    
}