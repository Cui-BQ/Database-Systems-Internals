package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private static ConcurrentHashMap<PageId, Page> bufferPoll;
    private int maxPage;

    private Queue<PageId> pageIdQueue;
    private LockManager lock;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        bufferPoll = new ConcurrentHashMap<>();
        maxPage = numPages;
        pageIdQueue = new LinkedList<>();
        this.lock = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        if (bufferPoll.size() >= maxPage){
            evictPage();
        }

        synchronized (lock) {
            try {
                if (!lock.acquireLock(tid, pid, perm)){
                    //System.out.println("Throw");
                    throw new TransactionAbortedException();
                }
            } catch (InterruptedException e) {
                System.out.println(e);
            }


            if (!bufferPoll.containsKey(pid)) {
                Page currentPage = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                bufferPoll.put(pid, currentPage);
                pageIdQueue.add(pid);
            }
            return bufferPoll.get(pid);
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lock.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lock.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2

        for (LockManager.Lock lock: lock.getTransactionLocks(tid)) {
            PageId pid = lock.getPID();
            if (bufferPoll.containsKey(pid)) {
                Page page = bufferPoll.get(pid);
                Database.getLogFile().logWrite(tid, page.getBeforeImage(), page);
                Database.getLogFile().force();
                page.setBeforeImage();
                if (!commit){
                    Page restorePage = page.getBeforeImage();
                    bufferPoll.replace(pid, restorePage);
                }
            }
            if (holdsLock(tid, pid)){
                releasePage(tid, pid);
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirtyPages = file.insertTuple(tid, t);
        for (Page page: dirtyPages){
            page.markDirty(true, tid);
            if (bufferPoll.size() >= maxPage && !bufferPoll.contains(page)){
                evictPage();
            }
            bufferPoll.put(page.getId(), page);
            pageIdQueue.remove(page.getId());
            pageIdQueue.add(page.getId());
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirtyPages = file.deleteTuple(tid, t);
        for (Page page: dirtyPages){
            page.markDirty(true, tid);
            if (bufferPoll.size() >= maxPage && !bufferPoll.contains(page)){
                evictPage();
            }
            bufferPoll.put(page.getId(), page);
            pageIdQueue.remove(page.getId());
            pageIdQueue.add(page.getId());
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pid: bufferPoll.keySet()){
            flushPage(pid);
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        bufferPoll.remove(pid);
        pageIdQueue.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        if (!bufferPoll.containsKey(pid)) throw new IOException("BufferPoll doesn't contain this page");
        Page page = bufferPoll.get(pid);
        TransactionId tid = page.isDirty();
        if (tid != null){
            Database.getLogFile().logWrite(tid, page.getBeforeImage(), page);
            Database.getLogFile().force();
            HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
            file.writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (Page page: bufferPoll.values()){
            if (tid.equals(page.isDirty())){
                flushPage(page.getId());
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        Iterator<PageId> IdItor = pageIdQueue.iterator();
        assert (pageIdQueue.size() == bufferPoll.size());

        while(IdItor.hasNext()){
            PageId currentID = IdItor.next();
            if (bufferPoll.get(currentID).isDirty() == null){
                try{
                    flushPage(currentID);
                } catch (IOException e){
                    e.printStackTrace();
                }
                discardPage(currentID);
                return;
            }
        }

        // All pages are dirty... In this case, evict the first page in page queue.
        try{
            flushPage(pageIdQueue.peek());
        } catch (IOException e){
            e.printStackTrace();
        }
        discardPage(pageIdQueue.peek());
        return;
    }

}
