package simpledb;

//import java.security.Key;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private  Aggregator.Op op;
    private HashMap<Field, Integer> gblist;
    private HashMap<Field, Integer> countlist;
    private TupleDesc td;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
        gblist = new HashMap<>();
        countlist = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field Key;
        if (gbfield == NO_GROUPING){
            Key = new IntField(-1);
        }else {
            Key = tup.getField(gbfield);
        }
        if (this.td == null){
            setTupleDesc(tup);
        }

        if (gblist.containsKey(Key)){
            countlist.put(Key, calOp(tup.getField(afield).hashCode(),countlist.get(Key),Op.COUNT));
            if (op == Op.AVG){
                gblist.put(Key, calOp(tup.getField(afield).hashCode(),gblist.get(Key),Op.SUM));
            }else {
                gblist.put(Key, calOp(tup.getField(afield).hashCode(),gblist.get(Key),op));
            }
        }else {
            countlist.put(Key, 1);
            if (op == Op.COUNT){
                gblist.put(Key, 1);
            }else {
                gblist.put(Key, tup.getField(afield).hashCode());
            }
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

    private int calOp(int newValue, int oldValue, Op op){
        switch (op){
            case MIN: return Math.min(oldValue, newValue);
            case MAX: return Math.max(oldValue,newValue);
            case SUM: return oldValue + newValue;
            case COUNT: return oldValue + 1;
            default: throw new UnsupportedOperationException("Can't find operator");
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
//        throw new
//        UnsupportedOperationException("please implement me for lab3");
        class IntAggIterator implements DbIterator{
            private HashMap<Field,Integer> iterated;
            private Iterator<Map.Entry<Field,Integer>> i;
            private HashMap<Field,Integer> counter;
            private Iterator<Map.Entry<Field,Integer>> ci;
            private TupleDesc td;
            public IntAggIterator(HashMap<Field, Integer> iter,HashMap<Field, Integer> counter, TupleDesc td){
                this.iterated =iter;
                this.td = td;
                this.counter = counter;
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                i = iterated.entrySet().iterator();
                ci = counter.entrySet().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return i.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                Map.Entry nextEntry = i.next();
                Map.Entry nextCounter = ci.next();
                Tuple nextTuple = new Tuple(getTupleDesc());
                if (op == Op.AVG){
                    if (td.numFields() == 1){
                        nextTuple.setField(0, new IntField((Integer) nextEntry.getValue()/(Integer) nextCounter.getValue()));
                    }else{
                        nextTuple.setField(0, (Field) nextEntry.getKey());
                        nextTuple.setField(1,new IntField((Integer) nextEntry.getValue()/(Integer) nextCounter.getValue()));
                    }
                }else{
                    if (td.numFields() == 1){
                        nextTuple.setField(0, new IntField((Integer) nextEntry.getValue()));
                    }else{
                        nextTuple.setField(0, (Field) nextEntry.getKey());
                        nextTuple.setField(1,new IntField((Integer) nextEntry.getValue()));
                    }
                }

                return nextTuple;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {

                close();
                open();

            }

            @Override
            public TupleDesc getTupleDesc() {
                return this.td;
            }

            @Override
            public void close() {
                i = null;
                ci = null;
            }
        }
        return new IntAggIterator(gblist,countlist,td);
    }

}
