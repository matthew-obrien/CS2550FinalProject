
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import org.jgraph.graph.DefaultEdge;
import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.CycleDetector;
import org.jgrapht.graph.ListenableDirectedGraph;

public class TwoPhaseLock {

    final private short EXCLUSIVE_ROW_LOCK = 1;
    final private short SHARED_ROW_LOCK = 0;

    final private short SHARED_INTENTION_TABLE_LOCK = 1;
    final private short SHARED_TABLE_LOCK = 2;
    final private short EXCLUSIVE_INTENTION_TABLE_LOCK = 3;
    final private short EXCLUSIVE_TABLE_LOCK = 4;

    final private DirectedGraph waitForGraph = new ListenableDirectedGraph(DefaultEdge.class);

    final private LinkedBlockingQueue<dbOp> scdm;
    final private HashMap<String, LockInfo> lockTable;

    final private ArrayList<dbOp> operationsWaitingforLocks = new ArrayList<>();

    public TwoPhaseLock(LinkedBlockingQueue<dbOp> scdm) {
        this.scdm = scdm;
        this.lockTable = new HashMap<>();
    }

    public void processOperation(dbOp op) {
        //check if a waiting opearion is free to go.
        for (Iterator<dbOp> i = operationsWaitingforLocks.iterator(); i.hasNext();) {
            dbOp waitingOper = (dbOp) i.next();
            HashSet<Integer> tidLocks = scheduleOperation(waitingOper);
            if (tidLocks == null) {
                i.remove();
                waitForGraph.removeVertex(waitingOper.tID);
            }
        }
        HashSet<Integer> tidLocks = scheduleOperation(op);
        if (tidLocks != null && !tidLocks.isEmpty()) {
            setToWaitQueue(tidLocks, op);
            evaluateForCycle(op);
        }
    }

    private HashSet<Integer> scheduleOperation(dbOp op) {
        //locks that may hold the op execution
        HashSet<Integer> tidLocks = null;
        LockInfo lock = lockTable.get(op.table);
        waitForGraph.addVertex(op.tID);
        switch (op.op) {
            case Begin:
                scdm.add(op);
                break;
            case Write:
                tidLocks = scheduleWrite(op, lock);
                break;
            case Read:
                tidLocks = scheduleReads(op, lock);
                break;
            case MRead:
                tidLocks = scheduleMReads(op, lock);
                break;
            case Delete:
                tidLocks = scheduleDelete(op, lock);
                break;
            case Commit:
                clearAllTransactionLocks(op.tID);
                scdm.add(op);
                break;
            case Abort:
                clearAllTransactionLocks(op.tID);
                scdm.add(op);
                break;
        }
        return tidLocks;
    }

    private HashSet<Integer> scheduleWrite(dbOp op, LockInfo lock) {
        if (lock != null) {
            HashSet<Integer> tidLocks = lock.isFreeForWrite(getDataRowPK(op), op.tID);
            if (!tidLocks.isEmpty()) {
                return tidLocks;
            }
        }
        addLockInfo(op, EXCLUSIVE_ROW_LOCK);
        scdm.add(op);
        return null;

    }

    private HashSet<Integer> scheduleReads(dbOp op, LockInfo lock) {
        if (lock != null) {
            HashSet<Integer> tidLocks = lock.isFreeForRead(getDataRowPK(op), op.tID);
            if (!tidLocks.isEmpty()) {
                return tidLocks;
            }
        }
        addLockInfo(op, SHARED_ROW_LOCK);
        scdm.add(op);
        return null;
    }

    private HashSet<Integer> scheduleMReads(dbOp op, LockInfo lock) {
        if (lock != null) {
            HashSet<Integer> tidLocks = lock.isFreeForMReads(op.tID);
            if (!tidLocks.isEmpty()) {
                return tidLocks;
            }
        }
        addLockInfo(op, SHARED_TABLE_LOCK);
        scdm.add(op);
        return null;
    }

    private HashSet<Integer> scheduleDelete(dbOp op, LockInfo lock) {
        if (lock != null) {
            HashSet<Integer> tidLocks = lock.isFreeForDelete(op.tID);
            if (!tidLocks.isEmpty()) {
                return tidLocks;
            }
        }
        addLockInfo(op, SHARED_TABLE_LOCK);
        scdm.add(op);
        return null;
    }

    private void evaluateForCycle(dbOp op) {
        CycleDetector<Integer, Integer> cDetector = new CycleDetector(waitForGraph);
        if (cDetector.detectCycles()) {
            waitForGraph.removeVertex(op.tID);
            clearAllTransactionLocks(op.tID);
            operationsWaitingforLocks.remove(op);
        }
    }

    private void setToWaitQueue(HashSet<Integer> tidLocks, dbOp op) {
        addEdgeToEachTransaction(tidLocks, op.tID);
        operationsWaitingforLocks.add(op);
    }

    private void addLockInfo(dbOp op, short lockType) {
        if (op.op == OperationType.Write || op.op == OperationType.Read) {
            if (!lockTable.containsKey(op.table)) {
                LockInfo newLock = new LockInfo(op.tID, getDataRowPK(op), lockType);
                lockTable.put(op.table, newLock);
            } else {
                LockInfo lInfo = lockTable.get(op.table);
                lInfo.addRowLock(lockType, op.tID, getDataRowPK(op));
            }

        } else if (!lockTable.containsKey(op.table)) {
            LockInfo newLock = new LockInfo(op.tID, getDataRowPK(op), lockType);
            lockTable.put(op.table, newLock);
        } else {
            LockInfo lInfo = lockTable.get(op.table);
            lInfo.addTableLock(op.tID, lockType);
        }
    }

    private int getDataRowPK(dbOp op) {
        int rowPrimaryKey = 0;
        if (!op.value.isEmpty()) {
            String writeContent = op.value;
            if (op.op == OperationType.Read) {
                rowPrimaryKey = Integer.parseInt(writeContent);
            } else {
                writeContent = writeContent.substring(1, writeContent.length() - 2);
                String fieldValues[] = writeContent.split(",");
                rowPrimaryKey = Integer.parseInt(fieldValues[0]);
            }
        }
        return rowPrimaryKey;
    }

    public void clearAllTransactionLocks(int tID) {
        for (Iterator<LockInfo> i = lockTable.values().iterator(); i.hasNext();) {
            LockInfo lock = i.next();
            lock.clearTableTransacationLocks(tID);
        }
    }

    private void addEdgeToEachTransaction(Collection<Integer> tids, int currentTID) {
        for (Integer tidWithLocks : tids) {
            waitForGraph.addEdge(currentTID, tidWithLocks);
        }
    }

    public void clearTable() {

    }

    public class OperationComparator implements Comparator<dbOp> {

        @Override
        public int compare(dbOp x, dbOp y) {

            if (x.tID < y.tID) {
                return -1;
            }
            if (x.tID > y.tID) {
                return 1;
            }
            return 0;
        }
    }
}
