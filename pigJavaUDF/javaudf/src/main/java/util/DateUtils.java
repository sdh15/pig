package util;

import org.apache.commons.lang.time.FastDateFormat;

/**
 * Created by fuliangliang on 15/11/3.
 */
public class DateUtils {
    public static String defaultFormatTime(long timestamp) {
        FastDateFormat fdf = FastDateFormat.getInstance("yyyy-MM-dd hh:mm:ss");
        return fdf.format(timestamp);
    }

    public static void main(String[] args) {
        System.out.println(defaultFormatTime(1446084368262L));
    }
}
