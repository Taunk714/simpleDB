package simpledb;

//import com.sun.source.tree.WhileLoopTree;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private int FieldNum = 0;
    private TDItem[] tdAr;
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return Arrays.stream(tdAr).iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here

        FieldNum = typeAr.length;
        tdAr = new TDItem[FieldNum];
        for (int i = 0; i < FieldNum; i++) {
            tdAr[i] = new TDItem(typeAr[i],fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return FieldNum;
    }


    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        try {
            return tdAr[i].fieldName;
        }catch (Exception e){
            throw new NoSuchElementException();
        }

    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        try {
            return tdAr[i].fieldType;
        }catch (Exception e){
            throw new NoSuchElementException();
        }

    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        Iterator<TDItem> tdItr = this.iterator();
        int i = 0;
        while (true){
            TDItem current = tdItr.next();
            try {
                if (name.equals(current.fieldName)){
                    return i;
                }else{
                    i++;
                }
            }catch (Exception e) {
                throw new NoSuchElementException();
            }

        }
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int result = 0;
        for (int i = 0; i < FieldNum; i++) {
            result = result + tdAr[i].fieldType.getLen();
        }
        return result;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int newLength = td1.numFields() + td2.numFields();
        Type[] TypeList = new Type[newLength];
        String[] NameList = new String[newLength];
        for (int i = 0; i < td1.numFields(); i++) {
            TypeList[i] = td1.getFieldType(i);
            NameList[i] = td1.getFieldName(i);
        }
        int k = td1.numFields();
        for (int i = td1.numFields(); i < newLength ; i++) {
            TypeList[i] = td2.getFieldType(i-k);
            NameList[i] = td2.getFieldName(i-k);

        }
        return new TupleDesc(TypeList, NameList);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if (this == o){
            return true;
        }
        if (!(o instanceof TupleDesc)){
            return false;
        }
        TupleDesc cpo = (TupleDesc) o;
        if (this.numFields() != cpo.numFields()){
            return false;
        }
        for (int i = 0; i < FieldNum; i++) {
            if (tdAr[i].fieldType != cpo.getFieldType(i)){
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
//        return Objects.hashCode(tdAr);
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String descriptor = tdAr[0].fieldType + "(" + tdAr[0].fieldName + ")";
        for (int i = 1; i < FieldNum; i++) {
            descriptor = descriptor + ", " + tdAr[i].fieldType + "(" + tdAr[i].fieldName;
        }
        return descriptor;
    }
}
