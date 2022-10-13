package simpledb;

import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;


/**
 * LockManager manages the the lock/unlock when a thread is reading/writing
 * pages from/to disk.
 */
public class LockManager {

    private ConcurrentHashMap<PageId, Lock> lockMap;
    private ConcurrentHashMap<TransactionId, HashSet<Lock>> transactionLockList;
    private ConcurrentHashMap<TransactionId, HashSet<TransactionId>> LockWaitList;


    LockManager(){
        lockMap = new ConcurrentHashMap<>();
        transactionLockList = new ConcurrentHashMap<>();
        LockWaitList = new ConcurrentHashMap<>();
    }

    synchronized boolean acquireLock (TransactionId TID, PageId PID, Permissions p) throws InterruptedException, TransactionAbortedException {
        if (!lockMap.containsKey(PID)) {
            makeNewLock(PID);
        }
        Lock lock = lockMap.get(PID);
        boolean res;
        if (p.equals(Permissions.READ_ONLY)){
            res = acquireSharedLock(TID, lock);
            if (res) putToLiST(transactionLockList, TID, lock);
        } else {
            res = acquireExclusiveLock(TID, lock);
            if (res) putToLiST(transactionLockList, TID, lock);
        }
        return res;
    }

    synchronized void releaseLock (TransactionId TID, PageId PID){
        if (!lockMap.containsKey(PID)) return;
        Lock lock = lockMap.get(PID);
        lock.releaseLock(TID);
        if (transactionLockList.containsKey(TID)) transactionLockList.get(TID).remove(lock);
        notifyAll();
    }


    private synchronized boolean acquireSharedLock(TransactionId TID, Lock lock) throws InterruptedException, TransactionAbortedException {
        while(!lock.acquireSharedLock(TID)){
            // This means there is another transaction holding the exclusiveLock of this lock.
            putToWait(TID, lock);
            if (detectDeadLock(TID)){
                removeFromWait(TID, lock);
                notifyAll();
                return false;
            }
            wait();
            removeFromWait(TID, lock);
        }

        return true;
    }

    private synchronized boolean acquireExclusiveLock(TransactionId TID, Lock lock) throws InterruptedException, TransactionAbortedException {
        while(!lock.acquireExclusiveLock(TID)){
            // This means there is another transaction holding the exclusiveLock of this lock.
            // Or, at least one another transaction is holding the sharedLock of this lock.
            putToWait(TID, lock);
            if (detectDeadLock(TID)){
                removeFromWait(TID, lock);
                notifyAll();
                return false;
            }
            wait();
            removeFromWait(TID, lock);
        }

        return true;
    }

    private synchronized void putToWait(TransactionId tid, Lock lock) {
        HashSet<TransactionId> TIDWaitFor = new HashSet<>();
        if (LockWaitList.containsKey(tid)){
            TIDWaitFor = LockWaitList.get(tid);
        }
        if (lock.exclusiveLock != null){
            TIDWaitFor.add(lock.exclusiveLock);
        } else {
            for (TransactionId waitTID: lock.sharedLock){
                TIDWaitFor.add(waitTID);
            }
        }
        LockWaitList.put(tid, TIDWaitFor);
    }

    private synchronized void removeFromWait(TransactionId tid, Lock lock) {
        if (lock.exclusiveLock != null){
            LockWaitList.get(tid).remove(lock.exclusiveLock);
        } else {
            for (TransactionId waitTID: lock.sharedLock){
                LockWaitList.get(tid).remove(waitTID);
            }
        }
    }

    private synchronized void makeNewLock(PageId PID){
        assert !lockMap.containsKey(PID);
        Lock newLock = new Lock(PID);
        lockMap.put(PID, newLock);
    }

    private synchronized void putToLiST(ConcurrentHashMap<TransactionId, HashSet<Lock>> list, TransactionId TID, Lock lock){
        HashSet<Lock> TIDLocks = new HashSet<>();
        if (list.containsKey(TID)){
            TIDLocks = list.get(TID);
        }
        TIDLocks.add(lock);
        list.put(TID, TIDLocks);
    }

    synchronized boolean holdsLock (TransactionId TID, PageId PID){
        Lock lock = lockMap.get(PID);
        if (lock == null) return false;
        if (TID.equals(lock.exclusiveLock)) return true;
        if (lock.sharedLock.contains(TID)) return true;
        return false;
    }

    synchronized boolean detectDeadLock(TransactionId tid) throws TransactionAbortedException {
        HashSet<Lock> currentLocks = transactionLockList.get(tid);

        if (currentLocks == null) return false;
        if (currentLocks.isEmpty()) return false;
        HashSet<TransactionId> waitTIDs = LockWaitList.get(tid);
        if (waitTIDs == null) return false;
        if (waitTIDs.isEmpty()) return false;

        for (TransactionId waitTid: waitTIDs){
            if (waitTid.equals(tid)) continue;
            HashSet<TransactionId> waitTidWaits = LockWaitList.get(waitTid);
            if (waitTidWaits == null) continue;
            for (TransactionId waitTidWait: waitTidWaits){
                if (waitTidWait.equals(tid)){
                    return true;
                }
            }
        }
        return false;
    }

    public HashSet<Lock> getTransactionLocks(TransactionId tid){
        return (HashSet<Lock>) this.transactionLockList.get(tid).clone();
    }









    /**
     * Inner Class Lock:
     * Each page(PID) associate with a Lock class
     * sharedLock: A set records which TransactionIds that have a shared lock on this PID.
     * exclusiveLock: Records which transactionId that has an exclusive lock on this PID.
     *                (exclusiveLock = null if exclusiveLock is free).
     */
    public class Lock{
        private PageId PID;
        private HashSet<TransactionId> sharedLock;
        private TransactionId exclusiveLock;


        /**
         * Make a Lock on this PID
         */
        private Lock(PageId PID){
            this.PID = PID;
            this.sharedLock = new HashSet<>();
            this.exclusiveLock = null;
        }

        /**
         * Transaction "TID" wants to acquire SharedLock on this.PID.
         * TID can only obtain SharedLock if exclusiveLock is free.
         * @return true if TID obtained SharedLock on this.PID.
         */
        private synchronized boolean acquireSharedLock(TransactionId TID){
            if (this.sharedLock.contains(TID)) return true;
            if (this.exclusiveLock != null){
                if (exclusiveLock.equals(TID)) {
                    assert sharedLock.size() == 0;
                    releaseLock(TID);
                } else {
                    return false;
                }
            }
            return sharedLock.add(TID);
        }

        /**
         * Transaction "TID" wants to acquire ExclusiveLock on this.PID.
         * TID can only obtain SharedLock if exclusiveLock is free AND
         * no other Transactions currently held a SharedLock. (SharedLock.size() == 0)
         * (In case if exclusiveLock is free AND transaction TID is the only transaction
         * holding a shared lock on an this.PID, then TID may upgrade its lock to exclusive lock)
         * @return true if TID obtained exclusiveLock on this.PID.
         */
        private synchronized boolean acquireExclusiveLock(TransactionId TID){
            if (TID.equals(exclusiveLock)) return true;
            if (exclusiveLock != null) return false;
            if (sharedLock.size() > 1) return false;
            if (sharedLock.size() == 1){
                if (!sharedLock.contains(TID)) return false;
                releaseLock(TID);
            }
            exclusiveLock = TID;
            return true;
        }

        /**
         * TransactionId "TID" wants to release the lock on this.PID.
         */
        private synchronized void releaseLock (TransactionId TID){
            if (TID.equals(this.exclusiveLock)) {
                exclusiveLock = null;
            } else {
                sharedLock.remove(TID);
            }
        }

        public PageId getPID (){
            return this.PID;
        }

    }

}
