package util;

/**
 * Created by fuliangliang on 15/8/31.
 */
public class LongHash {
    private static final long PRIME = 1125899906842597L;

    public static long strHash(String str) {
        long h = PRIME;

        for (int i = 0; i < str.length(); i++) {
            h = 31 * h + str.charAt(i);
        }

        return h;
    }
}
