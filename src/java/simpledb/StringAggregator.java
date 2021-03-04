package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private  Aggregator.Op op;
    private HashMap<Field, Integer> gblist;
    private TupleDesc td;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT){
            throw new IllegalArgumentException("String Aggregator can only be Count");
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
        gblist = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field Key;
        if (gbfield == NO_GROUPING) {
            Key = new IntField(-1);
        } else {
            Key = tup.getField(gbfield);
        }

        if (this.td == null) {
            setTupleDesc(tup);
        }
        if (gblist.containsKey(Key)){
            gblist.put(Key, 1 + gblist.get(Key));
        }else {
            gblist.put(Key, 1);
        }
    }

    private void setTupleDesc(Tuple tup){
        Type[] typeAr;
        String[] fieldAr;
        if (gbfield == Aggregator.NO_GROUPING){
            typeAr = new Type[1];
            fieldAr = new  String[1];
            typeAr[0] = Type.INT_TYPE;
            fieldAr[0] = tup.getTupleDesc().getFieldName(afield);
        }else {
            typeAr = new  Type[2];
            fieldAr = new String[2];
            typeAr[0] = gbfieldtype;
            fieldAr[0] = tup.getTupleDesc().getFieldName(gbfield);
            typeAr[1] = Type.INT_TYPE;
            fieldAr[1] = tup.getTupleDesc().getFieldName(afield);
        }
        this.td = new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
//        throw new UnsupportedOperationException("please implement me for lab3");
        class StringAggIterator implements DbIterator{
            private HashMap<Field,Integer> iterated;
            private Iterator<Map.Entry<Field,Integer>> i;
            private TupleDesc td;
            public StringAggIterator(HashMap<Field, Integer> iter, TupleDesc td){
                this.iterated =iter;
                this.td = td;
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                i = iterated.entrySet().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return i.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                Map.Entry nextEntry = i.next();
                Tuple nextTuple = new Tuple(getTupleDesc());
                if (td.numFields() == 1){
                    nextTuple.setField(0, new IntField((Integer) nextEntry.getValue()));
                }else{
                    nextTuple.setField(0, (Field) nextEntry.getKey());
                    nextTuple.setField(1,new IntField((Integer) nextEntry.getValue()));
                }
                return nextTuple;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                i = null;
                i = iterated.entrySet().iterator();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return this.td;
            }

            @Override
            public void close() {
                i = null;
            }
        }
        return new StringAggIterator(gblist,td);
    }

}
