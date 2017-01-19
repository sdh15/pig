package util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Created by fuliangliang on 15/8/31.
 */
public class MinHash {
    private List<RandomHash> hashFunctions;

    public MinHash(int numHashes, int seed) {
        this.hashFunctions = genHashFunctions(numHashes, seed);
    }

    static class RandomHash {
        private int w;
        private int b;

        public RandomHash(Random random) {
            this.w = random.nextInt();
            this.b = random.nextInt();
        }

        public long hash(long x) {
            return (w * x + b) % 768614336404564651L;
        }
    }

    public List<Long> calMinHash(Collection<String> items) {
        List<Long> minHashes = new ArrayList<>();
        for (int i = 0; i < hashFunctions.size(); i++) {
            minHashes.add(Long.MAX_VALUE);
        }

        for (String item : items) {
            long hash = LongHash.strHash(item);
            for (int i = 0; i < hashFunctions.size(); ++i) {
                RandomHash hashFunction = hashFunctions.get(i);
                long minHash = hashFunction.hash(hash);
                long preHash = minHashes.get(i);

                if (minHash < preHash) {
                    minHashes.set(i, minHash);
                }
            }
        }
        return minHashes;
    }

    private static List<RandomHash> genHashFunctions(int n, int seed) {
        Random random = new Random(seed);
        List<RandomHash> hashFuctions = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            hashFuctions.add(new RandomHash(random));
        }

        return hashFuctions;
    }

    public static void main(String[] args) {

        
    }
}
