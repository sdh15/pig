package com.sdh.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Created by fuliangliang on 15/8/31.
 */
public class LSHSignature {
    private MinHash minHash;
    private int concatNum;

    public LSHSignature(int numHashes, int concatNum) {
        minHash = new MinHash(numHashes, 1);
        this.concatNum = concatNum;
    }

    public List<String> generateSignature(List<String> items, int signatureCount, int seed) {
        List<String> signatures = new ArrayList<>();
        Random rand = new Random(seed);

        List<Long> hashes = minHash.calMinHash(items);

        for (int i = 0; i < signatureCount; ++i) {
            Set<Long> concatHashes = new LinkedHashSet<>();
            while (concatHashes.size() < concatNum) {
                Long hash = hashes.get(rand.nextInt(hashes.size()));
                if (!concatHashes.contains(hash)) {
                    concatHashes.add(hash);
                }
            }

            StringBuilder sb = new StringBuilder();
            for (Long concatHash : concatHashes) {
                sb.append(concatHash).append("_");
            }
            signatures.add(sb.substring(0, sb.length()-1));
        }
        return signatures;
    }

    public static void main(String[] args) {
        String[] items1 = new String[] {"A", "B", "C", "D", "E", "F"};
        String[] items2 = new String[] {"A", "C", "E", "F"};
        LSHSignature signature = new LSHSignature(80, 3);
        List<String> sigs = signature.generateSignature(Arrays.asList(items1), 125, 1);
        for (String sig : sigs) {
            System.out.println(sig);
        }
    }
}
