register 'keywordstofromidscore.py' using jython as udf;
--register /home/sundongheng/udf/pig-udf-1.0-SNAPSHOT-jar-with-dependencies.jar;
--define Keywords2FromidScore com.yidian.algorithm.oppo.Keywords2FromidScore;

OPPO_ONLINE_VIEW = load 'yidian.ods_oppobrowser_online_view' using org.apache.hive.hcatalog.pig.HCatLoader();

OPPO_KEYWORD_SEARCH_VIEW = filter OPPO_ONLINE_VIEW by p_day>='$SDATE' and p_day<='$EDATE' and log.action_src=='keywordSearchView';

OPPO_USER_KEYWORD = foreach OPPO_KEYWORD_SEARCH_VIEW generate user_id as userid,log.properties#'keyword' as keyword:chararray;

OPPO_USER_KEYWORDS = foreach (group OPPO_USER_KEYWORD by userid) generate group as userid,$1.keyword;
describe OPPO_USER_KEYWORDS;
OPPO_USER_FROMIDSCORE = foreach OPPO_USER_KEYWORDS generate userid,flatten(udf.keywords2fromidscore($1)) as (fromid:chararray,score:double);

describe OPPO_USER_FROMIDSCORE;
store OPPO_USER_FROMIDSCORE into 'user_fromid_score_pyudf' using PigStorage(':');

--pig -Dpig.additional.jars=jyson-1.0.2.jar -f testPythonUDF.pig  -p SDATE="2017-01-16" -p EDATE="2017-01-17">testPy.info
