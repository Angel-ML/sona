package org.apache.spark.angel.ml.tree.stats.hash;

import org.apache.spark.angel.ml.tree.util.MathUtil;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

public class HashFactory {
    private static final Int2IntHash[] int2intHashes =
            new Int2IntHash[]{new BJHash(0), new Mix64Hash(0),
            new TWHash(0), new BKDRHash(0, 31), new BKDRHash(0, 131),
            new BKDRHash(0, 267), new BKDRHash(0, 1313), new BKDRHash(0, 13131)};
    private static final Random random = new Random();

    public static void precompute(int maxKey) {
        for (Int2IntHash hash: int2intHashes)
            hash.precompute(maxKey);
    }

    public static Int2IntHash getRandomInt2IntHash(int size) {
        int idx = random.nextInt(int2intHashes.length);
        Int2IntHash res = int2intHashes[idx].clone();
        res.setSize(size);
        return res;
    }

    public static Int2IntHash[] getRandomInt2IntHashes(int hashNum, int size) {
        if (hashNum > int2intHashes.length) {
            throw new RuntimeException(String.format("Currently only %d " +
                    "hash functions are available", int2intHashes.length));
        } else {
            Int2IntHash[] res = new Int2IntHash[hashNum];
            int[] indexes = new int[int2intHashes.length];
            Arrays.setAll(indexes, i -> i);
            MathUtil.shuffle(indexes);
            for (int i = 0; i < hashNum; i++) {
                res[i] = int2intHashes[indexes[i]].clone();
                res[i].setSize(size);
            }
            return res;
        }
    }

    public static void serialize(ObjectOutputStream oos, Int2IntHash hash)
            throws IOException {
        oos.writeObject(hash);
    }

    public static Int2IntHash deserialize(ObjectInputStream ois)
            throws ClassNotFoundException, IOException {
        Int2IntHash res = (Int2IntHash) ois.readObject();
        for (Int2IntHash hash: int2intHashes) {
            if (res.equalsIgnoreSize(hash))
                res.setPrecomputeBuffer(hash.getPrecomputeBuffer());
        }
        return res;
    }
}
