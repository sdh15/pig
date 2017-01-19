package util;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.util.List;

/**
 * Created by fuliangliang on 15/8/3.
 */
public class PigUtils {
    private static BagFactory bagFactory = BagFactory.getInstance();
    private static TupleFactory tupleFactory = TupleFactory.getInstance();

    public static <T extends Object> DataBag createBag(List<T> items) throws ExecException {
        DataBag bag = bagFactory.newDefaultBag();

        for (T item : items) {
            Tuple tuple = tupleFactory.newTuple(1);
            tuple.set(0, item);
            bag.add(tuple);
        }
        return bag;
    }

    public static DataBag createBagWithTuples(List<Tuple> tuples) {
        DataBag bag = bagFactory.newDefaultBag();
        for (Tuple tuple : tuples) {
            bag.add(tuple);
        }
        return bag;
    }

    public static <T extends Object> Tuple createTuple(List<T> items) throws ExecException {
        Tuple tuple = tupleFactory.newTuple(items.size());
        for (int i = 0; i < items.size(); i++) {
            tuple.set(i, items.get(i));
        }
        return tuple;
    }
}
