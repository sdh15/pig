package com.sdh.util;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

/**
 * Created by fuliangliang on 15/5/20.
 */
public class JsonUtils {
    private static Logger logger = LoggerFactory.getLogger(JsonUtils.class);

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static JsonNode parse(String str) {
        try {
            return objectMapper.readTree(str);
        } catch (IOException e) {
            logger.error("Parse json=" + str + " failed", e);
        }
        return null;
    }

    public static Integer getInt(JsonNode jsonNode, String attr, Integer defaultValue) {
       return getValue(jsonNode, attr, defaultValue, new TypeConverter<Integer>() {
           @Override
           public Integer convert(JsonNode attrValue) {
               return attrValue.getValueAsInt();
           }
       });
    }

    public static Double getDouble(JsonNode jsonNode, String attr, Double defaultValue) {
       return getValue(jsonNode, attr, defaultValue, new TypeConverter<Double>() {
           @Override
           public Double convert(JsonNode attrValue) {
               return attrValue.getDoubleValue();
           }
       });
    }

    public static String getString(JsonNode jsonNode, String attr, String defaultValue) {
        return getValue(jsonNode, attr, defaultValue, new TypeConverter<String>() {
            @Override
            public String convert(JsonNode attrValue) {
                return attrValue.getTextValue();
            }
        });
    }

    public static Boolean getBoolean(JsonNode jsonNode, String attr, Boolean defaultValue) {
        return getValue(jsonNode, attr, defaultValue, new TypeConverter<Boolean>() {
            @Override
            public Boolean convert(JsonNode attrValue) {
                return attrValue.getBooleanValue();
            }
        });
    }

    private interface TypeConverter<T> {
        T convert(JsonNode attrValue);
    }

    public static <T> T getValue(JsonNode jsonNode, String attr, T defaultValue, TypeConverter<T> converter) {
        if (jsonNode == null) {
            return defaultValue;
        }

        JsonNode attrNode = jsonNode.get(attr);
        if (attrNode == null) {
            return defaultValue;
        }

        return converter.convert(attrNode);
    }

    public static JsonNode parse(InputStream input, Charset charset) {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(input, charset))) {
            return objectMapper.readTree(br);
        } catch (IOException e) {
            logger.error("parse json failed!", e);
            return null;
        }
    }
}
