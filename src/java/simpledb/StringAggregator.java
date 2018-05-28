package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfieldIdx;
    private final Type gbfieldtype;
    private final int aggFieldIdx;
    private final Op op;

    private final TupleDesc td;

    private Map<Field, StringAggregateItem> gbMap;

    /**
     * Aggregate constructor
     * @param gbFieldIdx the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param aggFieldIdx the 0-based index of the aggregate field in the tuple
     * @param op aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    public StringAggregator(int gbFieldIdx, Type gbfieldtype, int aggFieldIdx, Op op) {
        // some code goes here
        this.gbfieldIdx = gbFieldIdx;
        this.gbfieldtype = gbfieldtype;
        this.aggFieldIdx = aggFieldIdx;
        this.op = op;

        gbMap = new HashMap<>();

        Type aggFieldType = (op == Op.MIN || op == Op.MAX) ? Type.STRING_TYPE : Type.INT_TYPE;
        td = gbfieldIdx == NO_GROUPING ? new TupleDesc(new Type[]{aggFieldType})
                                       : new TupleDesc(new Type[]{gbfieldtype, aggFieldType});
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbField = gbfieldIdx == NO_GROUPING ? null
                                                  : tup.getField(gbfieldIdx);
        final StringAggregateItem val = gbMap.getOrDefault(gbField, new StringAggregateItem());
        val.update(((StringField)tup.getField(aggFieldIdx)).getValue());
        gbMap.put(gbField, val);
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
        List<Tuple> tuples = new ArrayList<>();

        for (Map.Entry<Field, StringAggregateItem> entry : gbMap.entrySet()) {
            String result = entry.getValue().applyOp(op);
            Field valField = (op == Op.MIN || op == Op.MAX) ? new StringField(result, Type.STRING_LEN)
                                                            : new IntField(Integer.valueOf(result));

            Tuple tuple = new Tuple(td);

            if (gbfieldIdx == NO_GROUPING) {
                tuple.setField(0, valField);
            } else {
                tuple.setField(0, entry.getKey());
                tuple.setField(1, valField);
            }
            tuples.add(tuple);
        }
        return new TupleIterator(td, tuples);
    }

    @Override
    public TupleDesc aggregatedTupleDesc() {
        return td;
    }

    @Override
    public void clear() {
        gbMap.clear();
    }

    private static class StringAggregateItem implements AggregateItem<String> {
        private int count;
        private String min;
        private String max;

        public StringAggregateItem() {
            count = 0;
            min = null;
            max = null;
        }

        @Override
        public void update(String val) {
            count++;
            if (min == null || val.compareTo(min) < 0) {
                min = val;
            }
            if (max == null || val.compareTo(max) > 0) {
                max = val;
            }
        }

        @Override
        public String applyOp(Op op) {
            switch (op) {
                case MIN:
                    return min;
                case MAX:
                    return max;
                case COUNT:
                    return String.valueOf(count); // :(
                case SUM_COUNT:
                case SC_AVG:
                    throw new UnsupportedOperationException();
                case SUM:
                case AVG:
                    throw new IllegalArgumentException();
            }
            throw new IllegalStateException("Unreachable code");
        }
    }

}
