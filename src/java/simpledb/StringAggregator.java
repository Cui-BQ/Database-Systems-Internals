package simpledb;

import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    int gbfield;
    Type gbfieldType;
    int afield;
    Op aoperator;
    HashMap<Field ,Tuple> aggregate;
    TupleDesc td;
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
        if (what != Op.COUNT) throw new IllegalArgumentException();
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.aoperator = what;
        this.aggregate = new HashMap<>();
        Type[] typeAr = gbfield == NO_GROUPING? new Type[]{Type.INT_TYPE} : new Type[]{gbfieldType, Type.INT_TYPE};
        String[] fieldAr = gbfield == NO_GROUPING? new String[]{"aggregateVal"} : new String[]{"groupVal", "aggregateVal"};
        this.td = new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupVal = gbfield != NO_GROUPING? tup.getField(gbfield) : null;
        aggregate.containsKey(groupVal);
        Field aggregateVal = new IntField(1);
        Tuple aggregateTup = new Tuple(td);

        if (aggregate.containsKey(groupVal)) {
            Tuple current = aggregate.get(groupVal);
            int currentValue = Integer.parseInt(current.getField(current.getTupleDesc().numFields()-1).toString());
            aggregateVal = new IntField(currentValue+1);
        }
        if (gbfield != NO_GROUPING){
            aggregateTup.setField(0, groupVal);
            aggregateTup.setField(1, aggregateVal);
        } else {
            aggregateTup.setField(0, aggregateVal);
        }
        aggregate.put(groupVal, aggregateTup);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new TupleIterator(td, aggregate.values());
    }

}
