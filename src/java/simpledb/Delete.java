package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator[] children;
    private Tuple count;
    private boolean used;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        this.tid =t;
        this.children = new DbIterator[1];
        children[0] = child;
        Type[] typeAr = new Type[1] ;
        String[] fieldAr = new String[1];
        typeAr[0] = Type.INT_TYPE;
        fieldAr[0] = "Affected";

        count = new Tuple(new TupleDesc(typeAr, fieldAr));

    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return count.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        children[0].open();
        count.setField(0, new IntField(0));

        try {
            while (children[0].hasNext()){
                Database.getBufferPool().deleteTuple(tid, children[0].next());
                count.setField(0, new IntField(count.getField(0).hashCode()+1));
            }
        }catch (IOException e){
        }

    }

    public void close() {
        // some code goes here
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (used){
            return null;
        }else {
            used = true;
            return count;
        }


    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.children = children;
        this.used = false;
    }

}
