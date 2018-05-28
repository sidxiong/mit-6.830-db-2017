package simpledb;

/**
 * @author : Siyadong Xiong (sx225@cornell.edu)
 * @version : 5/28/18
 */
public interface AggregateItem<T> {
    void update(T val);
    T applyOp(Aggregator.Op op);
}
