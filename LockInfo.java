import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

public final class LockInfo {

    final private short SHARED_INTENTION_TABLE_LOCK = 1;
    final private short SHARED_TABLE_LOCK = 2;
    final private short EXCLUSIVE_INTENTION_TABLE_LOCK = 3;
    final private short EXCLUSIVE_TABLE_LOCK = 4;

    final private short EXCLUSIVE_ROW_LOCK = 1;
    final private short SHARED_ROW_LOCK = 0;

    //0 is no locks 1 is shared and 2 is exclusive 3 is shared intention and 4 is exclusive intention
    private HashMap<Integer, Short> sharedLockTIDs = new HashMap<>();
    private HashMap<Integer, RowLock> dataRowsLock = new HashMap<>();//0 is shared and 1 is exclusive

    public LockInfo() {
    }

    public LockInfo(int tID, int rowPK, short rowLockType) {
        addRowLock(rowLockType, tID, rowPK);
    }

    public LockInfo(int tID, short tableLockType) {
        addTableLock(tID, tableLockType);
    }

    private void addOrUpdateRowsLock(int tID, short rowLockType, int rowPK) {
        if (dataRowsLock.containsKey(rowPK)) {
            RowLock rl = dataRowsLock.get(rowPK);
            rl.transactionsSharedLock.add(tID);
            if (rowLockType > 0) {
                rl.rowLockType = 1;
            }
        } else {
            RowLock newRowl = new RowLock(tID, rowLockType);
            dataRowsLock.put(rowPK, newRowl);
        }
    }

    //returns 0 if it is free for writes else returns the transacation Id it must wait on
    public HashSet<Integer> isFreeForWrite(int rowPK, int tID) {
        HashSet<Integer> transactions = new HashSet<>();
        for (Map.Entry<Integer, Short> entry : sharedLockTIDs.entrySet()) {
            Integer key = entry.getKey();
            Short value = entry.getValue();
            //any lock on table level also prevents a write
            if (key != tID) {
                if (value == SHARED_TABLE_LOCK || value == EXCLUSIVE_TABLE_LOCK) {
                    transactions.add(key);
                }
            }
        }
        RowLock dataRowLock = dataRowsLock.get(rowPK);
        if (dataRowLock != null && !dataRowLock.transactionsSharedLock.contains(tID)) {
            transactions.addAll(dataRowLock.transactionsSharedLock);
        }
        return transactions;
    }

    public HashSet<Integer> isFreeForRead(int rowPK, int tID) {
       
        HashSet<Integer> transactions = new HashSet<>();
        for (Map.Entry<Integer, Short> entry : sharedLockTIDs.entrySet()) {
            Integer key = entry.getKey();
            Short value = entry.getValue();
            if (tID != key) {
                if (value == EXCLUSIVE_TABLE_LOCK) {
                    transactions.add(key);
                }
            }
        }

        RowLock dataRowLock = dataRowsLock.get(rowPK);
        if (dataRowLock != null && !dataRowLock.transactionsSharedLock.contains(tID) && dataRowLock.rowLockType == EXCLUSIVE_ROW_LOCK) {
            transactions.addAll(dataRowLock.transactionsSharedLock);
        }
        return transactions;
    }

    public HashSet<Integer> isFreeForDelete(int tID) {
        HashSet<Integer> transactions = new HashSet<>();
        for (Map.Entry<Integer, Short> entry : sharedLockTIDs.entrySet()) {
            Integer key = entry.getKey();
            Short value = entry.getValue();
            //any lock on table level also prevents a write
            if (key != tID) {
                transactions.add(key);
            }
        }
        return transactions;
    }

    public HashSet<Integer> isFreeForMReads(int tID) {
        HashSet<Integer> transactions = new HashSet<>();
        for (Map.Entry<Integer, Short> entry : sharedLockTIDs.entrySet()) {
            Integer key = entry.getKey();
            Short value = entry.getValue();
            if (tID != key && (value == EXCLUSIVE_INTENTION_TABLE_LOCK || value == EXCLUSIVE_TABLE_LOCK)) {
                transactions.add(key);
            }
        }
        return transactions;
    }

    public void addTableLock(int tID, short lockType) {
        if (sharedLockTIDs.containsKey(tID)) {
            Short currentLockType = sharedLockTIDs.get(tID);
            if (currentLockType < lockType) {
                sharedLockTIDs.put(tID, lockType);
            }
        } else {
            sharedLockTIDs.put(tID, lockType);
        }
    }

    public void addRowLock(short rowLockType, int tID, int rowPK) {
        //0 is shared lock on the data item
        if (rowLockType == 0) {
            addTableLock(tID, SHARED_INTENTION_TABLE_LOCK);
        } else {
            addTableLock(tID, EXCLUSIVE_INTENTION_TABLE_LOCK);
        }
        addOrUpdateRowsLock(tID, rowLockType, rowPK);
    }

    public void clearTableTransacationLocks(int tID) {
        for (Iterator<RowLock> i = dataRowsLock.values().iterator(); i.hasNext();) {
            RowLock row = i.next();
            if (row.transactionsSharedLock.contains(tID)) {
                row.transactionsSharedLock.remove(tID);
            }
            if (row.transactionsSharedLock.isEmpty()) {
                i.remove();
            }
        }
        sharedLockTIDs.remove(tID);
    }

    class RowLock {

        public HashSet<Integer> transactionsSharedLock = new HashSet();
        public short rowLockType;//0 for shared lock and 1 for exclusive

        public RowLock(int transactionID, short rowLockType) {
            transactionsSharedLock.add(transactionID);
            this.rowLockType = rowLockType;
        }
    }

}
