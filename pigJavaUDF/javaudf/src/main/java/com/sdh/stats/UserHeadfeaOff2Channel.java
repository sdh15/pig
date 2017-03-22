package com.sdh.stats;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;

/**
 * Created by sundongheng on 17/2/17.
 */
public class UserHeadfeaOff2Channel extends EvalFunc<DataBag> {
    private static final Logger logger = LoggerFactory.getLogger(Ids2Bag.class);
    private static Set<String> facets = new HashSet<>(asList("ck_ct", "ck_sct", "ck_fromid"));

    @Override
    public DataBag exec(Tuple input) throws ExecException {
        if (input == null || input.size() == 0) {
            return null;
        }
        DataBag user_headfea_offBag = (DataBag) input.get(0);
        if (user_headfea_offBag == null) {
            return null;
        }
        return generateResDataBag(user_headfea_offBag);
    }

    private DataBag generateResDataBag(DataBag user_headfea_offBag) throws ExecException {
        DataBag result = BagFactory.getInstance().newDefaultBag();
        for (Tuple tup : user_headfea_offBag) {
            String facet = (String) tup.get(0);
            if (facet == null || facet.trim().isEmpty()) {
                continue;
            }
            if (!facets.contains(facet.trim())) {
                continue;
            }
            String channelName = (String) tup.get(1);
            double posCnt = (Double) tup.get(4);
            double negCnt = (Double) tup.get(5);
            if (equalsZero(negCnt)) {
                continue;
            }
            if (posCnt / negCnt < 0.05) {
                continue;
            }
            Tuple resTup = TupleFactory.getInstance().newTuple(1);
            resTup.set(0, channelName);
            result.add(resTup);
        }
        return result;
    }

    private boolean equalsZero(double val) {
        return Math.abs(val) < 0.0000001;
    }

    public static void main(String[] args) throws ExecException {
        DataBag keywordDataBag = BagFactory.getInstance().newDefaultBag();
        Tuple tup = TupleFactory.getInstance().newTuple(7);
        tup.set(0, "ck_ct");
        tup.set(1, "社会");
        tup.set(2, 1.1465140414746353);
        tup.set(3, 0.9146806424421393);
        tup.set(4, 3.561713757159272);
        tup.set(5, 15.698456303924669);
        tup.set(6, null);
        keywordDataBag.add(tup);

        Tuple res=TupleFactory.getInstance().newTuple(1);
        res.set(0,keywordDataBag);
        new UserHeadfeaOff2Channel().exec(res);
    }
}
