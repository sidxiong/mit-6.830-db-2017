package simpledb;

import java.util.Iterator;

/**
 * @author : Siyadong Xiong (sx225@cornell.edu)
 * @version : 5/22/18
 */
public class HeapFileIterator extends AbstractDbFileIterator {

    private int             pgNo;
    private HeapFile        file;
    private TransactionId   tid;
    private Iterator<Tuple> currTupleIter;

    public HeapFileIterator(HeapFile file, TransactionId tid) {
        this.file = file;
        this.tid = tid;
    }


    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if (currTupleIter == null) {
            return null;
        }
        Tuple nextTuple = null;

        if (currTupleIter.hasNext()) {
            nextTuple = currTupleIter.next();
        } else if (pgNo + 1 < file.numPages()) {
            pgNo++;
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(file.getId(), pgNo), Permissions.READ_ONLY);
            currTupleIter = heapPage.iterator();
            if (currTupleIter.hasNext()) {
                nextTuple = currTupleIter.next();
            }
        }

        return nextTuple;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        pgNo = 0;
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(file.getId(), pgNo), Permissions.READ_ONLY);
        currTupleIter = heapPage.iterator();
    }

    @Override
    public void close() {
        super.close();
        currTupleIter = null;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }
}
