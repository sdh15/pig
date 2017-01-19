package com.sdh;

import util.HttpRequestService;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

/**
 * Created by sundongheng on 17/1/19.
 */
public class Keywords2FromidScore extends EvalFunc<DataBag> {
    private static final Logger logger = LoggerFactory.getLogger(Keywords2FromidScore.class);
    @Override
    public DataBag exec(Tuple input) throws ExecException {
        if(input == null || input.size() == 0) {
            return null;
        }
        DataBag keywordsBag = (DataBag) input.get(0);
        Map<String,Integer> keywordCntMap=generateKeywordCnt(keywordsBag);
        Map<String,Double> fromidScoreMap=new HashMap<>();
        for(Map.Entry<String,Integer> entry:keywordCntMap.entrySet()){
            add2FromidScoreMap(entry.getKey(),entry.getValue(),fromidScoreMap);
        }
        return generateResDataBag(fromidScoreMap);
    }

    private DataBag generateResDataBag(Map<String,Double> fromidScoreMap) throws ExecException {
        DataBag result = BagFactory.getInstance().newDefaultBag();
        for(Map.Entry<String,Double> entry:fromidScoreMap.entrySet()){
            Tuple fromidScoreTup = TupleFactory.getInstance().newTuple(2);
            fromidScoreTup.set(0,entry.getKey());
            fromidScoreTup.set(1,entry.getValue());
            result.add(fromidScoreTup);
        }
        return result;
    }
    private void add2FromidScoreMap(String keyword,int count,Map<String,Double> fromidScoreMap){
        if(keyword==null||keyword.isEmpty()){
            return;
        }
        List<String> fromids=getFromid(keyword);
        for(String fromid:fromids){
            if(fromidScoreMap.containsKey(fromid)){
                fromidScoreMap.put(fromid,fromidScoreMap.get(fromid)+count*1.0);
            }else{
                fromidScoreMap.put(fromid,count*1.0);
            }
        }
    }
    private Map<String,Integer> generateKeywordCnt(DataBag keywordsBag) throws ExecException {
        Map<String,Integer> keywordCnt=new HashMap<>();
        for(Tuple tup:keywordsBag){
            String keyword=(String)tup.get(0);
            if(keywordCnt.containsKey(keyword)){
                keywordCnt.put(keyword,keywordCnt.get(keyword)+1);
            }else{
                keywordCnt.put(keyword,1);
            }
        }
        return keywordCnt;
    }


    private List<String> getFromid(String keyword){
        List<String> res=new ArrayList<>();
        String url = null;
        try {
            url = "http://lc1.haproxy.yidian.com:8902/service/assistant?word="+ URLEncoder.encode(keyword, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            logger.error("error happened when encode keyword: "+keyword,e);
        }
        HttpRequestService httpRequestService = HttpRequestService.getInstance();
        JsonNode json = httpRequestService.getForJson(url);
        if (json == null) {
            return res;
        }
        JsonNode statusNode=json.get("status");
        if(statusNode==null) {
            return res;
        }
        String status = statusNode.getValueAsText();
        if (status==null||!"success".equals(status)) {
            return res;
        }
        JsonNode channels = json.get("channels");
        for (int i = 0; i < channels.size(); i++) {
            JsonNode channel = channels.get(i);
            if(channel==null) {
                continue;
            }
            JsonNode id = channel.get("id");
            if(id==null){
                continue;
            }
            res.add(id.getTextValue());
            if(res.size()==2) {
                break;
            }
        }
        return res;
    }

    public static void main(String[] args) throws ExecException {
        List<String> keywords=asList("亚洲女人","魔兽世界");
        DataBag keywordDataBag = BagFactory.getInstance().newDefaultBag();
        for(String keyword:keywords){
            Tuple tup=TupleFactory.getInstance().newTuple(1);
            tup.set(0,keyword);
            keywordDataBag.add(tup);
        }
        Tuple res=TupleFactory.getInstance().newTuple(1);
        res.set(0,keywordDataBag);
        DataBag resDataBag=new Keywords2FromidScore().exec(res);

    }
}
