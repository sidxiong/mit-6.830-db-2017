package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final TupleDesc aggregatedTupleDesc;

    private int gbfieldIdx;
    private int aggFieldIdx;
    private Op  op;

    private Map<Field, IntAggregateItem> gbMap;

    /**
     * Aggregate constructor
     *
     * @param gbFieldIdx  the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbFieldType the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param aggFieldIdx the 0-based index of the aggregate field in the tuple
     * @param op          the aggregation operator
     */
    public IntegerAggregator(int gbFieldIdx, Type gbFieldType, int aggFieldIdx, Op op) {
        // some code goes here
        this.gbfieldIdx = gbFieldIdx;
        this.aggFieldIdx = aggFieldIdx;
        this.op = op;

        gbMap = new HashMap<>();

        aggregatedTupleDesc = gbfieldIdx == NO_GROUPING ? new TupleDesc(new Type[]{Type.INT_TYPE})
                                                        : new TupleDesc(new Type[]{gbFieldType, Type.INT_TYPE});
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbField = gbfieldIdx == NO_GROUPING ? null
                                                  : tup.getField(gbfieldIdx);
        final IntAggregateItem val = gbMap.getOrDefault(gbField, new IntAggregateItem());
        val.update(((IntField) tup.getField(aggFieldIdx)).getValue());
        gbMap.put(gbField, val);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, IntAggregateItem> entry : gbMap.entrySet()) {
            Field valField = new IntField(entry.getValue().applyOp(op));

            Tuple tuple   = new Tuple(aggregatedTupleDesc);
            Field gbField = entry.getKey();
            if (gbField == null) {
                tuple.setField(0, valField);
            } else {
                tuple.setField(0, gbField);
                tuple.setField(1, valField);
            }
            tuples.add(tuple);
        }

        return new TupleIterator(aggregatedTupleDesc, tuples);
    }

    @Override
    public TupleDesc aggregatedTupleDesc() {
        return aggregatedTupleDesc;
    }

    @Override
    public void clear() {
        gbMap.clear();
    }

    private static class IntAggregateItem implements AggregateItem<Integer> {
        private int count;
        private int sum;
        private int min;
        private int max;

        IntAggregateItem() {
            count = 0;
            sum = 0;
            min = Integer.MAX_VALUE;
            max = Integer.MIN_VALUE;
        }

        @Override
        public void update(Integer val) {
            count++;
            sum += val;
            if (val < min) {
                min = val;
            }
            if (val > max) {
                max = val;
            }
        }

        @Override
        public Integer applyOp(Op op) {
            switch (op) {
                case MIN:
                    return min;
                case MAX:
                    return max;
                case COUNT:
                    return count;
                case SUM:
                    return sum;
                case AVG:
                    return sum / count;
                case SUM_COUNT:
                case SC_AVG:
                    throw new UnsupportedOperationException();
            }
            throw new IllegalStateException("Unreachable code");
        }


    }

}
