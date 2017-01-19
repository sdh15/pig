--register 'yidian_udf_keywordstofromidscore.py' using jython as yidian_udf;
register /home/sundongheng/udf/java-udf-1.0-SNAPSHOT-jar-with-dependencies.jar;
define Keywords2FromidScore com.sdh.Keywords2FromidScore;
OPPO_ONLINE_VIEW = load 'yidian.ods_oppobrowser_online_view' using org.apache.hive.hcatalog.pig.HCatLoader();

OPPO_KEYWORD_SEARCH_VIEW = filter OPPO_ONLINE_VIEW by p_day>='$SDATE' and p_day<='$EDATE' and log.action_src=='keywordSearchView';

OPPO_USER_KEYWORD = foreach OPPO_KEYWORD_SEARCH_VIEW generate user_id as userid,log.properties#'keyword' as keyword;

OPPO_USER_KEYWORDS = foreach (group OPPO_USER_KEYWORD by userid) generate group as userid,OPPO_USER_KEYWORD.keyword;

OPPO_USER_FROMIDSCORE = foreach OPPO_USER_KEYWORDS generate userid,flatten(Keywords2FromidScore($1)) as (fromid:chararray,score:double);

--describe OPPO_USER_FROMIDSCORE;
--store OPPO_USER_FROMIDSCORE into 'user_fromid_score_today' using PigStorage(':');
store OPPO_USER_FROMIDSCORE into '$TODAY_OUTPUT' using PigStorage(':');
