package com.oltpbenchmark.util;

import java.util.BitSet;

public class BloomFilter<T> {

    private static final double LN_2 = Math.log(2); // useful constant

    private final BitSet bitSet;                    // bloom filter
    private final long numElems;                    // expected number of elements
    private final long numHashFns;                  // number of hash functions

    // n : numElems, num elements
    // k : numHashFns, num hash fns
    // b : bitSet.size(), num buckets

    public BloomFilter(long numElems, double fpTolerance) {
        this(numElems, calcNumBits(numElems, fpTolerance));
    }

    public BloomFilter(long numElems, int numBits) {
        this.numElems = numElems;
        this.numHashFns = calcNumHashFns(numElems, numBits);
        this.bitSet = new BitSet(numBits);
    }

    private static long calcNumHashFns(long numElems, long numBuckets) {
        double n = numElems;
        double b = numBuckets;
        long k = Math.round(n / b * LN_2);
        return Math.max(1, k);
    }

    private static int calcNumBits(long numElems, double fpTolerance) {
        double n = numElems;
        double p = fpTolerance;
        return (int) ((-n * Math.log(p)) / (LN_2 * LN_2));
    }

    public double getFalsePositiveRate() {
        // P(false positive) = (1 - e^(-kn/b))^k
        double n = numElems;
        double k = numHashFns;
        double b = bitSet.size();
        return 1 - Math.pow(Math.exp(-k * n / b), k);
    }

    /**
     * Adds the element's toString() representation.
     * @param elem element to be added
     */
    public void add(T elem) {
        RandomGenerator rg = new RandomGenerator(elem.hashCode());
        int b = bitSet.size();
        for (int i = 0; i < numHashFns; i++) {
            bitSet.set(rg.nextInt(b), true);
        }
    }

    public void clear() {
        bitSet.clear();
    }

    public boolean contains(T elem) {
        RandomGenerator rg = new RandomGenerator(elem.hashCode());
        int b = bitSet.size();
        for (int i = 0; i < numHashFns; i++) {
            if (!bitSet.get(rg.nextInt(b))) {
                return false;
            }
        }
        return true;
    }

}
