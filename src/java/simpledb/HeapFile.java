package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    public File file;
    public TupleDesc td;
    public DbFileIterator dbit;
    private int numPages;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
        this.numPages = numPages();
//        this.dbit = f.iterator();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
//        throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
//        throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "rws");
            int pagesize = BufferPool.getPageSize();
            byte[] data = new byte[pagesize];
            raf.seek(pid.pageNumber()*pagesize);
//            long mark = raf.getFilePointer();
            raf.read(data, 0, pagesize);
            return (Page) new HeapPage((HeapPageId) pid, data);
        }catch (Exception e){
            throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "rws");
            int pagesize = BufferPool.getPageSize();
            byte[] data = new byte[pagesize];
            raf.seek(page.getId().pageNumber()*pagesize);
//            long mark = raf.getFilePointer();
            raf.write(page.getPageData(), 0, pagesize);
            this.numPages = numPages();
        }catch (IOException e){
            throw e;
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return  (int) -Math.floorDiv(-file.length(),BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        HeapPageId pid;
        HeapPage page;
        ArrayList<Page> dirty = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            pid = new HeapPageId(getId(),i);
            page = (HeapPage)Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY);

            if (page.getNumEmptySlots() > 0){
                synchronized (page){
                    if (page.getNumEmptySlots()>0){
                        page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                        page.insertTuple(t);
                        dirty.add(page);
                        return dirty;
                    }
                }
            }
            Database.getBufferPool().releasePage(tid,pid);
        }

        synchronized (this){
            pid = new HeapPageId(getId(),numPages());
            page = new HeapPage(pid, HeapPage.createEmptyPageData());
            writePage(page);
            page = (HeapPage)Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
            page.insertTuple(t);
        }
        dirty.add(page);
        return dirty;

//        int i = 0;
//        ArrayList<Page> modiPages = new ArrayList<Page>();
//        while (i > numPages){
//            PageId pid = new HeapPageId(getId(),i);
//            HeapPage modiPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
//            if (modiPage.getNumEmptySlots() == 0){
//                continue;
//            }else{
//                modiPage.insertTuple(t);
////                modiPage.markDirty(true,tid);
//                modiPages.add(modiPage);
//                return modiPages;
//            }
//        }
        // not necessary for lab1

    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        HeapPage target = (HeapPage)Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(),Permissions.READ_WRITE);
        target.deleteTuple(t);
        ArrayList<Page> change = new ArrayList<>();
        change.add(target);
        return change;

        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        int tableid = this.getId();
        HeapPageId[] pid = new HeapPageId[numPages()];
        for (int i = 0; i < numPages(); i++) {
            pid[i] = new HeapPageId(tableid, i );
        }


        class MyDbfiter implements DbFileIterator{
            /**
             * Constructs an iterator from the specified pids, and the specified
             * descriptor.
             *
             * @param td
             * @param pids HeapPageIds generate based on tableids and numPages
             * @param tid
             */
            public HeapPageId[] pids;
            private int count;
            private int pidLength = 0;
            Iterator<Tuple> i = null;
            Iterator<Tuple> nextiter;
            TransactionId tid;
            TupleDesc td;
            public MyDbfiter(TupleDesc td, HeapPageId[] pids, TransactionId tid) {
                this.pids = pids;
                this.td = td;
                this.count = 0;
                this.pidLength = pids.length;
                this.tid = tid;
            }

            private HeapPage readPage(HeapPageId pid_)
                    throws TransactionAbortedException, DbException {
                return (HeapPage) Database.getBufferPool().getPage(tid,pid_,
                        Permissions.READ_ONLY);
            }

            public void open() throws DbException, TransactionAbortedException{
                count = 0;
                i = readPage(pids[0]).iterator();
            }


            public boolean hasNext() throws TransactionAbortedException,
                    DbException{
                if (i == null){
                    return false;
                }
                if (i.hasNext()){
                    return i.hasNext();
                }
                while ((count+1) < pidLength){
                    count++;
                    i = readPage(pids[count]).iterator();
                    if (i.hasNext()){
                        return i.hasNext();
                    }
                }
                return i.hasNext();

            }

            public Tuple next() throws TransactionAbortedException, DbException, NoSuchElementException {
                if (!this.hasNext()){
                    throw new NoSuchElementException();
                }else{
                    return i.next();
                }

            }

            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            public TupleDesc getTupleDesc() {
                return td;
            }

            public void close() {
                i = null;
                count = 0;
            }


        }

        return new MyDbfiter(td,pid,tid);
    }

}

