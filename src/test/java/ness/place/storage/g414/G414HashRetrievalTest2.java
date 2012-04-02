package ness.place.storage.g414;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Test;

import com.g414.hash.file2.HashFile2;
import com.g414.hash.file2.HashEntry;
import com.g414.hash.file2.HashFile2Builder;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

public class G414HashRetrievalTest2 {

    File testDir = Files.createTempDir();

    @After
    public final void cleanUp() throws IOException {
        FileUtils.deleteDirectory(testDir);
    }

    @Test
    public void testKeyRetrieval() throws Exception {
        String path = new File(testDir, "data").getAbsolutePath();
        HashFile2Builder hfb = new HashFile2Builder(path, 5000);
        new File(path).deleteOnExit();

        JsonParser jp = new ObjectMapper().getJsonFactory().createJsonParser(new GZIPInputStream(getClass().getResourceAsStream("/g414-test-data")));
        jp.nextToken();
        TreeSet<byte[]> keys = Sets.newTreeSet(new ByteaComparator());

        Iterator<KV> kvs = jp.readValuesAs(KV.class);

        while (kvs.hasNext()) {
            KV kv = kvs.next();
            hfb.add(kv.k, kv.v);
            keys.add(kv.k);
        }

        jp.close();

        hfb.finish();

        TreeSet<byte[]> foundKeys = Sets.newTreeSet(new ByteaComparator());
        for (HashEntry entry : HashFile2.elements(path)) {
            foundKeys.add(entry.getKey());
        }
        Assert.assertEquals(0, Sets.difference(keys, foundKeys).size());

        HashFile2 hf = new HashFile2(path);

        for (byte[] key : keys) {
            Assert.assertTrue(Arrays.toString(key), hf.get(key) != null);
        }

        System.out.println("Matched " + keys.size());
    }

    public static class KV {
        private byte[] k, v;

        @JsonCreator
        public KV (@JsonProperty("k") byte[] k, @JsonProperty("v") byte[] v) {
            this.k = k;
            this.v = v;
        }
    }

    // http://stackoverflow.com/questions/3137395/java-set-of-byte-arrays
    static class ByteaComparator implements Comparator<byte[]> {
        @Override
        public int compare(byte[] left, byte[] right) {
            for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
                int a = (left[i] & 0xff);
                int b = (right[j] & 0xff);
                if (a != b) {
                    return a - b;
                }
            }
            return left.length - right.length;
        }
    }
}
