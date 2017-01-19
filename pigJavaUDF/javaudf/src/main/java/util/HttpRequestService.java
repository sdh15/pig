package util;


import org.apache.commons.httpclient.HttpStatus;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.HttpParams;
import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Created by fuliangliang on 15/7/27.
 */
public class HttpRequestService {
    private static final Logger logger = LoggerFactory.getLogger(HttpRequestService.class);
    private static final int TIMEOUT = 3000;
    private static HttpRequestService instance;
    private HttpClient httpClient;

    public static HttpRequestService getInstance() {
        if (instance == null) {
            synchronized (HttpRequestService.class) {
                if (instance == null) {
                    instance = new HttpRequestService();
                }
            }
        }
        return instance;
    }

    private HttpRequestService() {
        PoolingClientConnectionManager manager = new PoolingClientConnectionManager();
        manager.setMaxTotal(500);
        manager.setDefaultMaxPerRoute(100);
        HttpParams params = new BasicHttpParams();
        params.setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, TIMEOUT);
        params.setParameter(CoreConnectionPNames.SO_TIMEOUT, TIMEOUT);
        httpClient = new DefaultHttpClient(manager, params);
    }

    public JsonNode postWithJson(final String url, String jsonData) {
        try {
            ContentType contentType = ContentType.create("application/json", Charset.forName("UTF-8"));

            StringEntity requestEntity = new StringEntity(
                    jsonData, contentType);

            HttpPost httpPost = new HttpPost(url);
            httpPost.setEntity(requestEntity);

            return httpClient.execute(httpPost, new ResponseHandler<JsonNode>() {
                public JsonNode handleResponse(HttpResponse response) throws IOException {
                    int statusCode = response.getStatusLine().getStatusCode();
                    if (statusCode == HttpStatus.SC_OK) {
                        HttpEntity entity = response.getEntity();
                        return JsonUtils.parse(entity.getContent(), Charset.forName("UTF-8"));
                    } else {
                        logger.error("post url={} with httpStatus={}", url, statusCode);
                        return null;
                    }
                }
            });
        } catch (IOException e) {
            logger.error("post url=" + url + " failed", e);
            return null;
        }
    }

    public JsonNode getForJson(final String url) {
        HttpGet get = new HttpGet(url);
        try {
            return httpClient.execute(get, new ResponseHandler<JsonNode>() {
                @Override
                public JsonNode handleResponse(HttpResponse response) throws IOException {
                    int statusCode = response.getStatusLine().getStatusCode();
                    if (statusCode == HttpStatus.SC_OK) {
                        HttpEntity entity = response.getEntity();
                        return JsonUtils.parse(entity.getContent(), Charset.forName("UTF-8"));
                    } else {
                        logger.error("post url={} with httpStatus={}", url, statusCode);
                        return null;
                    }
                }
            });
        } catch (IOException e) {
            logger.error("get url=" + url + " failed!", e);
            return null;
        }
    }

    public static void main(String[] args) {
        String url = "http://10.111.0.134:8038/service/handle";
        HttpRequestService requestService = HttpRequestService.getInstance();
        requestService.postWithJson(url, TestDoc.doc);
    }
}
