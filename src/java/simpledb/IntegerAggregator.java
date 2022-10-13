package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    int gbfield;
    Type gbfieldType;
    int afield;
    Op aoperator;
    HashMap<Field ,Tuple> aggregate;
    HashMap<Field ,Integer> count;
    TupleDesc td;


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
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.aoperator = what;
        this.aggregate = new HashMap<>();
        Type[] typeAr = gbfield == NO_GROUPING? new Type[]{Type.INT_TYPE} : new Type[]{gbfieldType, Type.INT_TYPE};
        String[] fieldAr = gbfield == NO_GROUPING? new String[]{"aggregateVal"} : new String[]{"groupVal", "aggregateVal"};
        this.td = new TupleDesc(typeAr, fieldAr);
        this.count = new HashMap<>();
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
        Field groupVal = gbfield != NO_GROUPING? tup.getField(gbfield) : null;
        Field aggregateVal = tup.getField(afield);
        Tuple aggregateTup = new Tuple(td);
        if (aoperator == Op.COUNT) {
           aggregateVal = new IntField(1);
        }

        if (aggregate.containsKey(groupVal)) {
            Tuple current = aggregate.get(groupVal);
            int currentValue = Integer.parseInt(current.getField(current.getTupleDesc().numFields()-1).toString());
            if (aoperator == Op.MIN) aggregateVal = new IntField(Math.min(currentValue, Integer.parseInt(aggregateVal.toString())));
            if (aoperator == Op.COUNT) aggregateVal = new IntField(currentValue+1);
            if (aoperator == Op.AVG) aggregateVal = new IntField(currentValue + Integer.parseInt(aggregateVal.toString()));
            if (aoperator == Op.MAX) aggregateVal = new IntField(Math.max(currentValue, Integer.parseInt(aggregateVal.toString())));
            if (aoperator == Op.SUM) aggregateVal = new IntField(currentValue + Integer.parseInt(aggregateVal.toString()));
        }

        if (gbfield != NO_GROUPING){
            aggregateTup.setField(0, groupVal);
            aggregateTup.setField(1, aggregateVal);
        } else {
            aggregateTup.setField(0, aggregateVal);
        }

        aggregate.put(groupVal, aggregateTup);
        if (count.containsKey(groupVal)){
            count.put(groupVal, count.get(groupVal)+1);
        } else {
            count.put(groupVal, 1);
        }


    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        if (aoperator == Op.AVG){
            ArrayList<Tuple> AVG = new ArrayList<>();
            for(HashMap.Entry<Field, Tuple> aggregate : aggregate.entrySet()) {
                Field k = aggregate.getKey();
                Tuple v = aggregate.getValue();
                Tuple avg = new Tuple(td);
                if (gbfield != NO_GROUPING){
                    avg.setField(0, v.getField(0));
                    avg.setField(1, new IntField(Integer.parseInt(v.getField(1).toString()) / count.get(k)));
                } else {
                    avg.setField(0, new IntField(Integer.parseInt(v.getField(0).toString()) / count.get(k)));
                }
                AVG.add(avg);
            }
            return new TupleIterator(td, AVG);
        }

        return new TupleIterator(td, aggregate.values());
    }

}
