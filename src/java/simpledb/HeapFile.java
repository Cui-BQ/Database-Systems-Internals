package simpledb;

import java.io.*;
import java.util.*;

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

    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int startOffset = pid.getPageNumber() * BufferPool.getPageSize();
        byte[] payroll = new byte[BufferPool.getPageSize()];

        try {
            RandomAccessFile access = new RandomAccessFile(f, "r");
            access.seek(startOffset);
            access.read(payroll);
            access.close();
            HeapPageId hpi = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            return new HeapPage(hpi, payroll);
        } catch (Exception e) {
            throw new NoSuchElementException(e.toString());
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int startOffset = page.getId().getPageNumber() * BufferPool.getPageSize();

        try{
            RandomAccessFile access = new RandomAccessFile(f, "rws");
            access.seek(startOffset);
            access.write(page.getPageData());
            access.close();
        } catch (Exception e){
            throw new IOException(e.toString());
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.ceil(f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapPage targetPage = null;
        int pageId = 0;
        //System.out.println(numPages());
        while(pageId < numPages()){
            HeapPage currentPage = (HeapPage) Database.getBufferPool().getPage(
                    tid, new HeapPageId(getId(), pageId), Permissions.READ_WRITE);
            if (currentPage.getNumEmptySlots() > 0) {
                targetPage = currentPage;
                break;
            }
            pageId++;
        }
        if (targetPage == null){
            targetPage = new HeapPage(new HeapPageId(getId(), pageId), new byte[BufferPool.getPageSize()]);
            targetPage.insertTuple(t);
            writePage(targetPage);
            ArrayList<Page> res = new ArrayList<>();
            res.add(targetPage);
            return res;
        } else {
            targetPage.insertTuple(t);
            ArrayList<Page> res = new ArrayList<>();
            res.add(targetPage);
            return res;
        }
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapPage targetPage = (HeapPage) Database.getBufferPool().getPage(
                tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        targetPage.deleteTuple(t);
        ArrayList<Page> res = new ArrayList<>();
        res.add(targetPage);
        return res;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        DbFileIterator iterator = new AbstractDbFileIterator() {
            int currentPageNumber = 0;
            HeapPage currentPage;
            Iterator<Tuple> currentIter;
            Boolean opened = false;

            @Override
            protected Tuple readNext() throws DbException, TransactionAbortedException {
                if (currentPage == null) {
                    currentPage = getNextPage();
                    if (currentPage != null){
                        currentIter = currentPage.iterator();
                    } else {
                        return null;
                    }
                }
                if (currentIter.hasNext()){
                    return currentIter.next();
                } else {
                    currentPage = null;
                    return readNext();
                }
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                opened = true;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                currentPageNumber = 0;
                currentPage = null;
            }

            private HeapPage getNextPage() throws TransactionAbortedException, DbException {
                if (currentPageNumber < numPages() && opened) {
                    currentPageNumber += 1;
                    return (HeapPage) Database.getBufferPool().getPage(
                            tid, new HeapPageId(getId(), currentPageNumber-1), Permissions.READ_ONLY);
                }
                return null;
            }

            @Override
            public void close() {
                super.close();
                opened = false;
                currentPage = null;
                currentIter = null;
            }
        };
        return iterator;
    }

}

