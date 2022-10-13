package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

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



    private static final long serialVersionUID = 1L;

    private final TDItem[] tupleFields;

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
        this.tupleFields = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++){
            this.tupleFields[i] = new TDItem(typeAr[i], fieldAr[i]);
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
        this.tupleFields = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++){
            this.tupleFields[i] = new TDItem(typeAr[i], null);
        }
    }

    private TupleDesc(TDItem[] tupleFields){
        this.tupleFields = tupleFields;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.tupleFields.length;
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
        if (i < 0 || i > this.tupleFields.length-1) {
            throw new NoSuchElementException();
        }
        return this.tupleFields[i].fieldName;
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
        if (i < 0 || i > this.tupleFields.length) {
            throw new NoSuchElementException();
        }
        return this.tupleFields[i].fieldType;
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
        int index = 0;
        Iterator<TDItem> iterator = this.iterator();
        while (iterator.hasNext()){
            String currentName = iterator.next().fieldName;
            if (currentName != null && currentName.equals(name)) {
                return index;
            } else {
                index += 1;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        Iterator<TDItem> iterator = this.iterator();
        while (iterator.hasNext()) {
            size += iterator.next().fieldType.getLen();
        }
        return size;
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
        TDItem[] tupleFields = new TDItem[td1.numFields() + td2.numFields()];
        int index = 0;

        Iterator<TDItem> iter1 = td1.iterator();
        while (iter1.hasNext()){
            tupleFields[index] = iter1.next();
            index += 1;
        }
        Iterator<TDItem> iter2 = td2.iterator();
        while (iter2.hasNext()){
            tupleFields[index] = iter2.next();
            index += 1;
        }
        return new TupleDesc(tupleFields);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (o instanceof TupleDesc){
            if (((TupleDesc) o).numFields() == this.numFields()){
               Iterator<TDItem> current = this.iterator();
               Iterator<TDItem> other = ((TupleDesc) o).iterator();
               while (current.hasNext()){
                   if (!current.next().fieldType.equals(other.next().fieldType)){
                       return false;
                   }
               }
               return true;
            }
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return Arrays.hashCode(this.tupleFields);
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
        Iterator<TDItem> iterator = this.iterator();
        String res = "";
        int index = 0;
        while(iterator.hasNext()){
            TDItem current = iterator.next();
            String fieldName = current.fieldName == null? "null" : current.fieldName;
            res += String.format("%s[%d](%s[%d]), ", fieldName, index, current.fieldType, index);
        }
        return res.substring(0, res.length()-2);
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return Arrays.stream(this.tupleFields).iterator();
    }
}
