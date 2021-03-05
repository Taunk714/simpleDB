package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate p;
    private  DbIterator[] children;
    private ArrayList<Tuple> tuples = new ArrayList<>();
    private Iterator<Tuple> newtuples;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     *
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.p = p;
        children = new DbIterator[2];
        children[0] = child1;
        children[1] = child2;

    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return children[0].getTupleDesc().getFieldName(p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return children[1].getTupleDesc().getFieldName(p.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(children[0].getTupleDesc(),children[1].getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        children[0].open();
        children[1].open();
        super.open();
//        while (children[0].hasNext()){
//            Tuple currentOuter = children[0].next();
//            while (children[1].hasNext()){
//                Tuple currentInner = children[1].next();
//                if (p.filter(currentOuter,currentInner)) {
//                    Tuple newTuple = new Tuple(getTupleDesc());
//                    Iterator<Field> t1Field = currentOuter.fields();
//                    Iterator<Field> t2Field = currentInner.fields();
//                    for (int i = 0; i < currentOuter.getTupleDesc().numFields(); i++) {
//                        newTuple.setField(i, t1Field.next());
//                    }
//                    for (int i = currentOuter.getTupleDesc().numFields(); i < newTuple.getTupleDesc().numFields(); i++) {
//                        newTuple.setField(i, t2Field.next());
//                    }
//                    tuples.add(newTuple);
//                }
//            }
//            children[1].rewind();
//        }
//        newtuples = tuples.iterator();
        HashMap<Integer, ArrayList<Tuple>> buckets = new HashMap<>();
        Tuple currentLeft;
        Tuple currentRight;
        int key = 0;
        tuples = new ArrayList<>();
        Tuple[] pageLeft = new Tuple[BufferPool.getPageSize()/children[0].getTupleDesc().getSize()];
        ArrayList<Tuple[]> blocks = new ArrayList<>();
        while (children[0].hasNext()) {
            if (key == 0){
                pageLeft = new Tuple[BufferPool.getPageSize()];
            }
            currentLeft = children[0].next();
            pageLeft[key++] = currentLeft;
            if (key == BufferPool.getPageSize() || !children[0].hasNext()) {
                blocks.add(pageLeft);
            }
            if (blocks.size() == BufferPool.DEFAULT_PAGES - 2 || !children[0].hasNext()){
                children[1].rewind();
                while (children[1].hasNext()) {
                    currentRight = children[1].next();
                    for (Tuple[] tupleInPage: blocks){
                        for (Tuple left: tupleInPage){
                            if (left == null){
                                break;
                            }
                            if (p.filter(left, currentRight)) {
                                Tuple newTuple = new Tuple(getTupleDesc());
                                Iterator<Field> t2Field = currentRight.fields();
                                Iterator<Field> t1Field = left.fields();
                                for (int j = 0; j < left.getTupleDesc().numFields(); j++) {
                                    newTuple.setField(j, t1Field.next());
                                }
                                for (int j = left.getTupleDesc().numFields(); j < newTuple.getTupleDesc().numFields(); j++) {
                                    newTuple.setField(j, t2Field.next());
                                }
                                tuples.add(newTuple);
                            }
                        }
                    }

                }
                blocks = new ArrayList<>();
                key = 0;
            }
        }
//            key = currentHash.getField(p.getField1()).hashCode();
//            if (buckets.containsKey(key)){
//                buckets.get(key).add(currentHash);
//            }else {
//                ArrayList<Tuple> buck = new ArrayList<>();
//                buck.add(currentHash);
//                buckets.put(key, buck);
//            }
//        }
//        while (children[1].hasNext()){
//            currentHash = children[1].next();
//            key = currentHash.getField(p.getField2()).hashCode();
//                for(Map.Entry<Integer, ArrayList<Tuple>> entries: buckets.entrySet()){
//                    if (p.filter(entries.getValue().get(0), currentHash)) {
//                        for (Tuple join2: entries.getValue()){
//                            Tuple newTuple = new Tuple(getTupleDesc());
//                            Iterator<Field> t2Field = currentHash.fields();
//                            Iterator<Field> t1Field = join2.fields();
//                            for (int i = 0; i < join2.getTupleDesc().numFields(); i++) {
//                                newTuple.setField(i, t1Field.next());
//                            }
//                            for (int i = join2.getTupleDesc().numFields(); i < newTuple.getTupleDesc().numFields(); i++) {
//                                newTuple.setField(i, t2Field.next());
//                            }
//                            tuples.add(newTuple);
//                        }
//                    }
//                }
//        }
        newtuples = tuples.iterator();
    }

    public void close() {
        // some code goes here
        super.close();
        children[0].close();
        children[1].close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */

    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (newtuples.hasNext()){
            return newtuples.next();
        }else{
            return null;
        }


    }


    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return this.children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.children = children;
    }

}
