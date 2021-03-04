package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator[] children;
    private  int tableid;
    private Tuple count;
    private boolean used = false;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        // some code goes here
        if (!child.getTupleDesc().equals(Database.getCatalog().getDatabaseFile(tableId).getTupleDesc())){
            throw new DbException("TupleDesc is inconsistent");
        }

        this.tableid = tableId;
        this.children = new DbIterator[1];
        children[0] = child;
        this.tid = t;
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
                Database.getBufferPool().insertTuple(tid,tableid,children[0].next());
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
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
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
