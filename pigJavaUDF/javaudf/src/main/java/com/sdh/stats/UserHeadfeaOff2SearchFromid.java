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
 * Created by sundongheng on 17/2/20.
 */
public class UserHeadfeaOff2SearchFromid extends EvalFunc<DataBag> {
    private static final Logger logger = LoggerFactory.getLogger(UserHeadfeaOff2SearchFromid.class);

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
        boolean isSearchFromid=false;
        for (Tuple tup : user_headfea_offBag) {
            String facet = (String) tup.get(0);
            if (facet == null || facet.trim().isEmpty()) {
                continue;
            }
            if ("rc_searchview_s_fromid".contains(facet.trim())) {
                isSearchFromid=true;
            }
        }
        Tuple resTup = TupleFactory.getInstance().newTuple(1);
        resTup.set(0, isSearchFromid);
        result.add(resTup);
        return result;
    }
}
