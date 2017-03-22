package com.sdh.stats;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by sundongheng on 17/2/9.
 */
public class Ids2Bag extends EvalFunc<DataBag> {
    private static final Logger logger = LoggerFactory.getLogger(Ids2Bag.class);

    @Override
    public DataBag exec(Tuple input) throws ExecException {
        if (input == null || input.size() == 0) {
            return null;
        }
        String ids = (String) input.get(0);
        DataBag result = BagFactory.getInstance().newDefaultBag();
        if (ids == null || ids.isEmpty()) {
            return result;
        }
        ids = ids.trim().replace("[", "").replace("]", "");
        String idArr[] = ids.split(",");
        for (String id : idArr) {
            id=id.trim().replace("\"", "");
            Tuple fromidScoreTup = TupleFactory.getInstance().newTuple(1);
            fromidScoreTup.set(0, id);
            result.add(fromidScoreTup);
        }
        return result;
    }

    public static void main(String[] args) throws ExecException {
        String str = "[6162331951,6162331967,6162331983,'6162331999',6162332015,6162332031,6162332047,6162332063,6162332079,6162332095,6162332111,6162332127,6162332143,6162332159,6162332175,6162332191,6162332207,6162332223]";
        Ids2Bag ids2Bag = new Ids2Bag();
        Tuple tup = TupleFactory.getInstance().newTuple(1);
        tup.set(0, str);
        ids2Bag.exec(tup);
    }

}
