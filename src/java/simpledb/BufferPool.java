package simpledb;

//import javax.xml.crypto.Data;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

//import java.util.Objects;
//import java.util.concurrent.ConcurrentHashMap;

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
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private static int SLEEP_LOCK_WAIT = 100;
    private int numPages;
    private AtomicLong counter = new AtomicLong();
//    public Page[] pages;
//    public PageId[] pids;
//    public TransactionId[] tids;
    private ConcurrentHashMap<PageId, Page> pageMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<PageId, Long> lruMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<PageId, Set<TransactionId>> shareSet = new ConcurrentHashMap<>();
    private ConcurrentHashMap<PageId, TransactionId> exclusive = new ConcurrentHashMap<>();
    private ConcurrentHashMap<TransactionId, PageId> request = new ConcurrentHashMap<>();

//    private long timeMark = System.currentTimeMillis();
//    private ConcurrentHashMap<PageId, Page> pageMap;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
//        tids = new TransactionId[numPages];
//        pages = new Page[numPages];
//        pids = new PageId[numPages];
//        pageMap = new ConcurrentHashMap<>();
//        lruMap = new ConcurrentHashMap<>()
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
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        long timeMark = System.currentTimeMillis();
        int epy = numPages;
            if (!pageMap.containsKey(pid)){
                if (pageMap.size() == numPages){
                    evictPage();
                }
                pageMap.put(pid, Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid));
                lruMap.put(pid,counter.incrementAndGet());
                if (perm == Permissions.READ_ONLY){
                    shareSet.put(pid, new HashSet<>());
                    shareSet.get(pid).add(tid);
                    return pageMap.get(pid);
                }else{
                    exclusive.put(pid,tid);
                    return pageMap.get(pid);
                }
            }
            if (perm == Permissions.READ_ONLY){
                if (exclusive.containsKey(pid)){
                    if (exclusive.get(pid) == tid){
                        lruMap.put(pid, counter.incrementAndGet());
                        return pageMap.get(pid);
                    }else{
                        // block
                        while (exclusive.containsKey(pid)){
                            if (System.currentTimeMillis()-timeMark > 5000){
                                throw new TransactionAbortedException();
                            }
                            try{
                                Thread.sleep(SLEEP_LOCK_WAIT);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        return getPage(tid,pid,perm);
                    }
                } else{
                    if (shareSet.containsKey(pid)){
                        shareSet.get(pid).add(tid);
                    }else{
                        shareSet.put(pid, new HashSet<>());
                    }
                    lruMap.put(pid, counter.incrementAndGet());
                    return pageMap.get(pid);
                }
            }else{
                if (exclusive.containsKey(pid)){
                    if (exclusive.get(pid) == tid){
                        lruMap.put(pid, counter.incrementAndGet());
                        return pageMap.get(pid);
                    }else{
                        // block
                        while (exclusive.containsKey(pid)){
                            if (System.currentTimeMillis()-timeMark > 5000){
                                throw new TransactionAbortedException();
                            }
                            try{
                                Thread.sleep(SLEEP_LOCK_WAIT);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        return getPage(tid,pid,perm);
                    }
                }else{
                    if (shareSet.containsKey(pid)){
                        if (shareSet.get(pid).contains(tid) && shareSet.get(pid).size() == 1){
                            lruMap.put(pid, counter.incrementAndGet());
                            shareSet.remove(pid);
                            exclusive.put(pid,tid);
                            return pageMap.get(pid);
                        }else{
                            while (shareSet.containsKey(pid) || exclusive.containsKey(pid)){
                                if (System.currentTimeMillis()-timeMark > 5000){
                                    throw new TransactionAbortedException();
                                }
                                try{
                                    Thread.sleep(SLEEP_LOCK_WAIT);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                            return getPage(tid,pid,perm);
                        }
                    }else{
                        exclusive.put(pid,tid);
                        pageMap.put(pid,Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid));
                        lruMap.put(pid, counter.incrementAndGet());
                        return pageMap.get(pid);
                    }
                }
            }



//        for (int i = 0; i < this.numPages; i++) {
//            if (pages[i] == null){
//                epy = i;
//            }
//            if (pid.equals(pids[i])){
//                tids[i] = tid;
//                return pages[i];
//            }
//        }
//        if (epy == numPages){
//            evictPage();
//            for (int i = 0; i < numPages ; i++) {
//                if (pages[i] == null){
//                    epy = i;
//                }
//            }
//        }
//
//        tids[epy] = tid;
//        pids[epy] = pid;
//        pages[epy] = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
//        Lock lock;
//        if (perm == Permissions.READ_ONLY){
//            lock = new ReentrantReadWriteLock().readLock();
//        }else{
//            lock =  new ReentrantReadWriteLock().writeLock();
//        }
//
//        lock.lock();
//        return pages[epy];


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
//        lockMap.get(pid)
        if (exclusive.containsKey(pid)){
            assert exclusive.get(pid) == tid;
            exclusive.remove(pid);
        }
        if (shareSet.containsKey(pid)){
            shareSet.get(pid).remove(tid);
            if (shareSet.get(pid).size() == 0){
                shareSet.remove(pid);
            }
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        if (exclusive.containsKey(p) && exclusive.get(p) == tid){
            return true;
        }else if (shareSet.containsKey(p) && shareSet.get(p).contains(tid)){
            return true;
        }else{
            return false;
        }
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit){
            flushPages(tid);
            for (Map.Entry<PageId, TransactionId> entry : exclusive.entrySet()) {
                if (entry.getValue() == tid){
                    exclusive.remove(entry.getKey());
                }
            }

            for (Map.Entry<PageId, Set<TransactionId>> entry : shareSet.entrySet()) {
                entry.getValue().remove(tid);
                if (entry.getValue().size() == 0){
                    shareSet.remove(entry.getKey());
                }
            }
        }else{
            for (Map.Entry<PageId, TransactionId> entry : exclusive.entrySet()) {
                if (entry.getValue() == tid){
                    exclusive.remove(entry.getKey());
                }
                discardPage(entry.getKey());
            }

            for (Map.Entry<PageId, Set<TransactionId>> entry : shareSet.entrySet()) {
                entry.getValue().remove(tid);
                if (entry.getValue().size() == 0){
                    shareSet.remove(entry.getKey());
                }
                discardPage(entry.getKey());
            }
        }

//        transactionComplete(tid);
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
        ArrayList dirtys =  Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid,t);
        Iterator<Page> dirty = dirtys.iterator();
        while (dirty.hasNext()){
            Page modified = dirty.next();
            modified.markDirty(true, tid);
            updatePage(modified, tid);

        }
    }

    public void updatePage(Page newVersion, TransactionId tid) throws DbException {
//        int epy = numPages;
        pageMap.put(newVersion.getId(), newVersion);
//        for (int i = 0; i < this.numPages; i++) {
//            if (pages[i] == null){
//                epy = i;
//            }
//            if (newVersion.getId().equals(pids[i])){
//                tids[i] = tid;
//                pages[i] = newVersion;
//                return;
//            }
//        }
//        if (epy == numPages){
//            evictPage();
//            for (int i = 0; i < numPages ; i++) {
//                if (pages[i] == null){
//                    epy = i;
//                }
//            }
//        }
//
//        tids[epy] = tid;
//        pids[epy] = newVersion.getId();
//        pages[epy] = newVersion;
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
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        ArrayList dirtys =  Database.getCatalog().getDatabaseFile(tableId).deleteTuple(tid,t);
        Iterator<Page> dirty = dirtys.iterator();
        while (dirty.hasNext()){
            Page modified = dirty.next();
            modified.markDirty(true, tid);
            updatePage(modified, tid);
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
        for (Map.Entry<PageId, Page> pageEntry : pageMap.entrySet()) {
            if (pageEntry.getValue().isDirty() != null){
                flushPage(pageEntry.getKey());
            }
        }
//        for (int i = 0; i < numPages; i++) {
//            if (pages[i]!= null && pages[i].isDirty() != null){
//                flushPage(pids[i]);
//            }
//        }

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
//        for (int i = 0; i < numPages; i++) {
//            if (pid.equals(pids[i])) {
//                pids[i] = null;
//                tids[i] = null;
//                pages[i] = null;
//            }
//        }
        pageMap.remove(pid);
        lruMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
//        for (int i = 0; i < numPages; i++) {
//            if (pid.equals(pids[i])){
//                Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(pages[i]);
//            }
//        }
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(pageMap.get(pid));


    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (Map.Entry<PageId, TransactionId> pageEntry : exclusive.entrySet()) {
            if (pageEntry.getValue()== tid){
                flushPage(pageEntry.getKey());
            }
        }
//        for (int i = 0; i < numPages; i++) {
//            if (tid.equals(tids[i])){
//                flushPage(pids[i]);
//            }
//        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        Map.Entry<PageId, Long> lruEntry = null;
        if (lruMap.size() == 0){
            throw new DbException("no page in buffer pool");
        }
        for (Map.Entry<PageId, Long> entry : lruMap.entrySet()) {
            Page page = pageMap.get(entry.getKey());
            if (page.isDirty() != null){
                continue;
            }
            if (lruEntry == null){
                lruEntry = entry;
            }else if (lruEntry.getValue() > entry.getValue()){
                lruEntry = entry;
            }
        }
        if (lruEntry == null){
            throw new DbException("All pages are dirty");
        }
        //            flushPage(lruEntry.getKey());
        discardPage(lruEntry.getKey());

//        long tidnum = Long.MAX_VALUE;
//        int k = -1;
//        for (int i = 0; i < numPages; i++) {
//            if ((tids[i] != null) && (tids[i].getId()<tidnum)){
//                tidnum = tids[i].getId();
//                k = i;
//            }
//        }
//        if (k == -1){
//            throw new DbException("no page in buffer pool");
//        }else{
//            try {
//                flushPage(pids[k]);
//                discardPage(pids[k]);
//            }catch (IOException e){
//                throw new DbException("can't flush to disk");
//            }
//        }
    }

}
