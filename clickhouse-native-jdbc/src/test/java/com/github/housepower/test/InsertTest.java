package com.github.housepower.test;

import com.tdunning.math.stats.Centroid;
import com.tdunning.math.stats.TDigest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.*;

public class InsertTest {

    @Test
    public void testInsert() throws Exception {
        Properties props = new Properties();
        props.put("user", "default");
        props.put("password", "123456");

        Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
        Connection connection = DriverManager.getConnection("jdbc:clickhouse://192.168.216.86:9001", props);

        PreparedStatement stmt = null;
        try {
            stmt = connection.prepareStatement("INSERT INTO test.test_uniq(datetime) values(?)");

            // 写入默认数据
            stmt.setObject(1, new Timestamp(1706252400000L)); // new Date("2024-01-26 15:00:00").getTime()
            stmt.addBatch();
            stmt.setObject(1, new Timestamp(1706252400000L));
            stmt.addBatch();
            stmt.setObject(1, new Timestamp(1706252400000L));
            stmt.addBatch();

            stmt.executeBatch();
            connection.commit();
        } finally {
            if(stmt != null){
                stmt.close();
            }
            connection.close();
        }

    }

    // 能够成功写入
    @Test
    public void testInsertHll() throws Exception {
        String str1 = "AoABAdQ8Y8S2TIEEbLc+Mqap+wVINoFOQqQZBqcXZn2l/vUHdNAHTmi0TAnqSZggCkH0DmPzU8ZI8kETPzgveaX/Chj58/8/pHoqG6XX2kTR/UkdNRQePmU2jB7IQ8FXXu15IVy55W0QVZUjHHVkusyDLCWjPcB0lLLGKVQo8Vp5kEkr4/1un90O5S6iQB2fqwPdLkG/YnjIf30vQygRay0PSDDBuNChq3p1NVQ606WTnIo27s7+IKPkzjc6GqMG39pYOONsm8eGDGs50/KSM+jOeDweHBVcep3ePMffyrPM+YFAc5L6FeJqckFgbknkOJ+hQi9rgP1N1/NGAW3axeLNj0YQA2iAMLPDSuMTo5GeI0NKZme2wDwGpUqBnYWiLRNCUCvi6iSlu3FTR7v5dJ61w1N9ow8yUT98WJCdtEyFeK9YxKEaRWESBVqdForMmZVMWjf3L6IsvGxeTv7Geu23zV8XN1rePJcTYMMxs8prfuNiJlEUQzBfJ2VJwUnu/NrVZp0o3aqW4kBmV3/RiRhbjGhrnZ4li6XyaSCnZ9jPajVqGFsjJxd6nHFLmm0TnMJDcrCay5/GfTN2t4nc/F9/8Hena7OdIHvyd/gFahyr+QJ3t/btsNGfOnr1xgzk4Z+be8wxHGGxJeh+Vk/7UigbHn+pGLg6Q6sJgIVXR5LxLmx/I+kZvigVO4InrK7PhjLMg5H5fWJtfvmI2CVWlB+Sp4r1FGDA/RNEi6nOf2rDuOqMtuXTbtHqh5DbdPK5s8Y3l8781msp9FqdTmgIMYt3JZ5NMf7byOEJnUTA7k1tZr+gQoxV0MjGb6RTle38J4JHppzhRdg+cMirSAJeS/1fpK7drlCXxQ9Ts6ifwT0P48u0LGb+OX2RO7QaBHzVx7x8us6okDglJ266MMtE/hIPQ72Ln1LcsJ1AvlZIlAXDVLXAjXs1dNG05cGRAdG0L58twb1wmyg0SazDrMbOUXAuGsOrwI5i5+FgzL3O3sK/SbrNdgMR/DOGHdAFSURAa6wN03PJvNgXEdfUuURd1ocvQ9jPNWThzVwf2ObYaWSnD8vdQRqTYZ6O1d3x8ujO5/Zq3xtMXbAvqIDh4TiFxbVT4eHTz2wA4NOS5KOh5kV2sKHlQbKzi9UC4OYMQrDHuf3I5oplGVrSx/TpzBy8bV7GZOq3RydauqYr64ddP9UlzePsMSz111aVeO0fOZcyYxXf7NZvv5RuwtTtcn1YXx1nH+vJydZQwUeu8VDWQs7BZJ7x6oBcbO9ATPM07v5C7j9N9HCQXEJrgQT1RNPlKYbpE+qTXEbMuLVf91R6+/CnbjX4qYZGbYprrPlmR+fZxTyl+gMLtMsUQ7j9zl2bYhTVkg==";
        String str2 = "AwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAABAAADAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAIAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMAAAAAAAAAAAAAAAAAAAAAQAAAAAAAgAAAAAAAAAAAAAAAAAMAAAAAAAAAAAMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAIAAAAQAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAABQAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAEAAAAAAAAQAAAAAEAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAABAAAAAQAAIAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAQAAAAAAAAAABQAAAAAAIABAAAAEAAAAAAAAAAAAAgAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAEAAAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAFAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAGAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAABAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAEABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAgAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMAAAAAAAAAAAMAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMAAAAAwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFAAAAAAABAAAAAEAAAAABAAAAAAAAAAAAMAAAAAAAAAAAAAAAAAAAAAABAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIMPAAA5AAAAIAAAABUAAAAJAAAABQAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIMP";
        String str3 = "AwAABAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAEACAAAAAFgAABAAIAAAAAAAAAAAIEAAAAQAAAQAAAQBAAADAAAAAEAAEAgAAAAAAIAAIAAAAAAAAAgAEAAAAAAAEAAAAAAAEAAAEAACAEAEAAAAAAAAIABCAABAAAAAEAAAAAAAEAAAAAAAAAAAEEQAAAAAAAABAAAAAAAAEAABAIAAAAwAEAACAAAAAIAAAAAAEIQAEEABEAQAAEACIAgAAAAAAAAAAAAAAAAAAAAAAAACAEAAAAAAEAAAAAAAIAADEAAAAAwAAMBAAAAAAAACAAAAAAABAJQAAFAAAEAAEIAAAAAAAAAAAAAAAAAAABQAAAAAAAABAAAAAAAAEAwAAAABIAABAAAAAAAAEAABAEAAAAAAIAAAAEAAAAAAAMADAAAAAAAAAAQAAAAAAAAAAAAAEAAAAAwAAAAAAAAAAAAAAAAAMEAAMAACIAAAMAQAAAAAAEAAAAACAEAGAEAAAAAAAABAIIACIMAAIAAAIAAAEAAFABAAAAAAAAABEAAAAABAAEwAAAAAAAAAIAAAAAAAEIAAMAADAEADAAABIAAAIAAAAAAAAAAAAAAAAAQAAAAAAAAAAEAAAAQAAEAAMAAAIAAAAMAAIAAAEQAAAAABAAAAAAAAAAAAEMAAAAAAAAACAAAAAAAAAAAAAQAAAEAAEAQAAAAAMAAAEAwAAYAFAAAAEAABAAQAAEgAAAgEAAAAEMAAAEwAIAABMAAAAAAAAAAAMAAEAUAAAAwCAMADAAAAAAAAEAAAAEAAIAAAIIQCAAwGAAAAAAQAEIQAAAAAAEAAAAQAMIhAAAAAAAAAAAAAAAAAAEQAAMQAAAQAAEAAIAAEAAAAIAAAAAAAAAACAAAAAEwAAAgAAAQBAEAGAAQCAAAAAUgAAAAAAEACAAAAAEAAAAgAAAwAAAACAAAAAAwAAAAAIAQAAIAAAAQBAEAAAAgAAAAAEAAAAIAAAAwAAAAAAAACAAAAABQAAAAAEBAAAAABAAADAEAAAAgAAAAAAIABAAAAAAQAAAAAEAAAAMgAAAAAEAQAAAAAEAQAAAQAEAAAAFQAAAAAAAQAAAAAIMAAAAAAAAAAAIAAAAAAAAQAAAQAAAAAAAgAAEAAAAACAQAAAAAAEAAAAQAAABQAAAAAMEAAAAAAAAADAAABAAAAAAAAAAACAIQAAAAAAAABAIAAAAAAAAABMAAAEAAAABAAAAAAAIAAEAABAAAAAIQAAAABMAABAAAAAAAAAAAAAAACAAAAAEAAEEAAAAQAAAQCIAAAAAAAAAAAEEABAAwAAAAAAIABABQAAAAAAAAAEAAAAAQAAAAAAAAAAAACAAAAAAACABQAAAAAAEAAAAAEAAABAEACAAQAAEAAMQQAAAAAAAQCAAAEAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAMAAAAAAAAAAAIgAAAAAAAAAAAAAAEAAAUwAAAAAAAAAEAQAAAAFAAAAAAACAAAAAAgAIMADEAAAAMAAMAAAAAABABwDAAAAEAACAAABAAAAAAAAAAAEAAAAMAAAAAAAAMAAAMAAAAQAAAAAAMAAAAgAAAgAAAAAAAQBEAAAAEAAAAgAAAgBAAwCAYAAAAQAEAAAAAAAAAQAAAQAAAAAAAABAAAAAEQAAAAAAAgAAEAAAAAAAAADAAABAAAFAIAAIQAAIAAAAAAAEAQAABgEAAAAAAAAEAACAAAAAAABAIAAAAAAEAAAAAAAAAgAIMACAIAAAAAAIAAAAIAEAAQBAAgAAEAAAAAAAAACAAAAAAAAAAQAAAAAAAAAAAAAAAAAAEAAAAAAIAAAAMwAAAAAAAAAAAAAAAAAEBADEAAAAAQBAAABAEwBEFQAAAAAAIABAEAAEAQCAAAAAAAAAAgAAAQAAAAAAAQAAAAAAAAAAAAAAAAAIQADAABBAAAAAAAAEAAAAAAAAAAAAIgAIAAAAAABAABBAAAAAAAAAIAAEAAAAEBAAIAAABQAAAwEAAACAAAAAEAAAAQAAIgCAAAAMEABAIQBAEAFAAABABAAAAAAAEwAAEAAAAAAIAADAAAAAAAAAAAAAAAAEAAAAARAAAAAAAgBEAAAAEAAAAQAAYAAIAAAIAAAABAAAAQAAEAAAAAAAAgBAEAAAAAAAAAAAAABAEQAAIQAAAAAAAAEEAAAABgAAAQAAAAAEAADABABEAACAIAAAAAAAAAAAAgFEAQAAAAAAAAAEAAAAAAAAAADAEAAIAAAAAAAAIAAAAACIAAAAAAAAAAAAAAAEAQBIAAAABgAAAAAAAAAAEAAEIAAAIAAEAACAAAAAAQAAAAAAAACAEAAAAwAAIAAAAADAAAAAAAAAAAAIAQAEIAAAAgAAAAAAAgAMAAAAAAAAAwAAAADAEAAAIQAAAACAAAAAAAAAAQEAAAAAAAAEAAAAQAAAAAAAMAAAAAAAAAAAAAAAAQAAAAAAAwDAAAAAEAAEAAAAAAAAAAAAAAAEEAAIAAAIAACMABAAIgAAAADAAAAIAgAAAAAAIAAAAAAAAAAABAAIAABAAQAEAAAAAACAAAAAAAAAAQCAAQAAAAAAAAAAIAAIAABAAAAAAABEAQAAAQAAEQBAAAAAAAAAAAAAAAAAABAMABAEAAAAAgBAMAAAAAAAEABAAAAAIAAAAABAIAAAAAAIIAAAEAAAAADAABAAAAAAAAAAAAAAMAAAAACAAAAAEAGEMAAAAAAAAQAAAAAAAgAAAABAAAAEIBAAAAAAAACAAADANAAAAAAEAAFIMAAAIAAAAAAEIAAAAwAAAAAAAAAEAAAAIAAAAAAAEAAAAAAAAACAAgAAAACAAAAAAAAEAAFAAAAAQQAAAAAIAAAAAAAAAgAAIAAABAAAAgAAAAAAAAAAEAAAIABAAABMEAAAQQAEAAAAAAAEAAAAAQAAQABAAAAAEAAAAAAAMAAAAAAEAAAAAABAAAAAAAAAQABAAAAAAAAAABAAAACAAAAAQAAAEAFAEACAAAAAAAAAAABAAADAAACAIAAEAAAIAAAAIADIEAAAAAAAAgAAIABIAAAIJABAAAAAAAAMAAAAAACEAABAAAAAAAAEAAAIAAAAAAAAAAAAAAAAAABAAACAAAAAAAAAAAAIAAAIAAAEAAAEEAAAAAAAEAAAAAAAEAAAAABAAADAIABABgBAAABAAABAAAAEAADAEAAAIAAAAQAAAABIAQAAAACAAAAAAAAAIAAMAAAAAAAAAQCEAAAABADAAAHAMAAAAAAAAACAAAAMAAAAFgAMAAAAAAAAAACAAAAEABAEBwCAAABAAAAIAADAAAAAIAAAMAAAAQDEAAAAFgAAAABAAAAEQAAEQQAAAQAAAgAAAABAAAAAAAAMAAAAAAAAAABAAAAAEQAAAAAAAAAAAhAAAgAEAAAABQAAEAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAADIAwAAAABAEAAEIAAAAQAAAABAAAAAAQAAUwAIQAAAAACEAAEAEAAAAAAIAAAAAAAAAACMAxDAAAAAAAAAAAAAAAAAEAAAAAAAAAAEAAAAUAAAAwAAMAAAAAAAAAAAMAAAAABAAAAAAQAEABAAIQAAAAAAAgEAAAAAEAAAAAAAAAAAAAAAIQAEAABAAAAAAAAAAwAAABAAABAAAACAAAAAAAAAYAAAIAAAAAAAIAAIAAAAAAAAAAAAAABAEQDAAgAMAABAIwAAAAAAAAAEAwAAAAAEAAAAAwAAAAAAAAAMEAAAIAAAAAAAAQAAAAAAAQFAAAAAAABAEADAEAAAIABAEAAAAAAAAAAMAAAEAAAAAAAAAAAAAAFAEABAAACAAAAAIQAAAAAMAABAAAAAEAEAAAAAAABAAQAAAgCAAgAAAAAAAAAAEAAAAAAAAQAAAACAAwAAAAAAAAAAAAAAAAAAAAAAAAAEEAAAAACIAwAAAwEAABAAAAAAAABIAAAEAAAAIAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAEAAAAAMACAAAAAAAAAEAAABAAEAAAAAQDAAAAAAAAMAAAJEMAACJAQAA9QAAAH4AAAA/AAAAHQAAABAAAAAHAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAJEM";

        Properties props = new Properties();
        props.put("user", "default");
        props.put("password", "123456");

        Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
        Connection connection = DriverManager.getConnection("jdbc:clickhouse://192.168.216.86:9001", props);

        PreparedStatement stmt = null;
        try {
            stmt = connection.prepareStatement("INSERT INTO test.test_uniq(datetime, dinctinct_count) values(?, ?)");

            stmt.setObject(1, new Timestamp(1706259600000L)); // new Date("2024-01-26 15:00:00").getTime()
            stmt.setObject(2, new byte[]{1, 0}); // hll默认值
            stmt.addBatch();
            stmt.setObject(1, new Timestamp(1706259600000L));
            stmt.setObject(2, Base64.getDecoder().decode(str1.getBytes(StandardCharsets.UTF_8)));
            stmt.addBatch();
            stmt.setObject(1, new Timestamp(1706259600000L));
            stmt.setObject(2, Base64.getDecoder().decode(str2.getBytes(StandardCharsets.UTF_8)));
            stmt.addBatch();
            stmt.setObject(1, new Timestamp(1706259600000L));
            stmt.setObject(2, Base64.getDecoder().decode(str3.getBytes(StandardCharsets.UTF_8)));
            stmt.addBatch();

            stmt.executeBatch();
            connection.commit();
        } finally {
            if(stmt != null){
                stmt.close();
            }
            connection.close();
        }

    }

    @Test
    public void testInsertQuantileTDigest() throws Exception {
        Properties props = new Properties();
        props.put("user", "default");
        props.put("password", "galaxy2019");

        Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
        Connection connection = DriverManager.getConnection("jdbc:clickhouse://192.168.41.30:9001", props);

        PreparedStatement stmt = null;
        try {
            stmt = connection.prepareStatement("INSERT INTO tsg_galaxy_v3.test_quantile_tdigest(datetime, quantile_stage) values(?, ?)");

            stmt.setObject(1, new Timestamp(1706259600000L)); // new Date("2024-01-26 17:00:00").getTime()
            stmt.setObject(2, new byte[]{0}); // QuantileTDigest默认值
            stmt.addBatch();
            stmt.setObject(1, new Timestamp(1706259600000L));
            stmt.setObject(2, geneQuantileTDigest(80, 200));
            stmt.addBatch();
            stmt.setObject(1, new Timestamp(1706259600000L));
            stmt.setObject(2, geneQuantileTDigest(80, 200));
            stmt.addBatch();
            stmt.setObject(1, new Timestamp(1706259600000L));
            stmt.setObject(2, geneQuantileTDigest(80, 200));
            stmt.addBatch();

            stmt.executeBatch();
            connection.commit();
        } finally {
            if(stmt != null){
                stmt.close();
            }
            connection.close();
        }

    }

    @Test
    public void testInsertQuantileTDigest2() throws Exception {
        Properties props = new Properties();
        props.put("user", "default");
        props.put("password", "galaxy2019");

        Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
        Connection connection = DriverManager.getConnection("jdbc:clickhouse://192.168.41.30:9001", props);

        PreparedStatement stmt = null;
        try {
            stmt = connection.prepareStatement("INSERT INTO tsg_galaxy_v3.test_quantile_tdigest(datetime, quantile_stage) values(?, ?)");

            stmt.setObject(1, new Timestamp(1706259600000L)); // new Date("2024-01-26 17:00:00").getTime()
            stmt.setObject(2, new byte[]{0}); // QuantileTDigest默认值
            stmt.addBatch();
            stmt.setObject(1, new Timestamp(1706259600000L + 3600000));
            stmt.setObject(2, geneQuantileTDigest(160, 200));
            stmt.addBatch();
            stmt.setObject(1, new Timestamp(1706259600000L + 3600000 * 2));
            stmt.setObject(2, geneQuantileTDigest(160, 200));
            stmt.addBatch();
            stmt.setObject(1, new Timestamp(1706259600000L + 3600000 * 3));
            stmt.setObject(2, geneQuantileTDigest(160, 200));
            stmt.addBatch();

            stmt.executeBatch();
            connection.commit();
        } finally {
            if(stmt != null){
                stmt.close();
            }
            connection.close();
        }

    }


    @Test
    public void testInsertQuantileTDigest3() throws Exception {
        Properties props = new Properties();
        props.put("user", "default");
        props.put("password", "galaxy2019");

        Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
        Connection connection = DriverManager.getConnection("jdbc:clickhouse://192.168.41.30:9001", props);

        PreparedStatement stmt = null;
        try {
            stmt = connection.prepareStatement("INSERT INTO tsg_galaxy_v3.test_quantile_tdigest(datetime, quantile_stage) values(?, ?)");

            stmt.setObject(1, new Timestamp(1706259600000L)); // new Date("2024-01-26 17:00:00").getTime()
            stmt.setObject(2, new byte[]{0}); // QuantileTDigest默认值
            stmt.addBatch();
            stmt.setObject(1, new Timestamp(1706259600000L + 3600000));
            stmt.setObject(2, geneQuantileTDigest(100000, 10000));
            stmt.addBatch();
            stmt.setObject(1, new Timestamp(1706259600000L + 3600000 * 2));
            stmt.setObject(2, geneQuantileTDigest(100000, 10000));
            stmt.addBatch();
            stmt.setObject(1, new Timestamp(1706259600000L + 3600000 * 3));
            stmt.setObject(2, geneQuantileTDigest(100000, 10000));
            stmt.addBatch();

            stmt.executeBatch();
            connection.commit();
        } finally {
            if(stmt != null){
                stmt.close();
            }
            connection.close();
        }

    }

    @Test
    public void testTDigest() throws Exception {
        TDigest tDigest1 = TDigest.createMergingDigest(100);
        Random random = new Random();
        for (int i = 1; i <= 100000; i++) {
            int v = random.nextInt(10000) + 1;
            tDigest1.add(v);
        }

        TDigest tDigest2 = TDigest.createMergingDigest(100);
        random = new Random();
        for (int i = 1; i <= 100000; i++) {
            int v = random.nextInt(10000) + 1;
            tDigest2.add(v);
        }

        TDigest tDigest3 = TDigest.createMergingDigest(100);
        random = new Random();
        for (int i = 1; i <= 100000; i++) {
            int v = random.nextInt(10000) + 1;
            tDigest3.add(v);
        }

        TDigest tDigestMerge = TDigest.createMergingDigest(100);
        tDigestMerge.add(tDigest1);
        tDigestMerge.add(tDigest2);
        tDigestMerge.add(tDigest3);

        TDigest[] tDigests = new TDigest[]{tDigest1, tDigest2, tDigest3, tDigestMerge};
        // 0, 0.1, 0.3, 0.5, 0.8, 0.9, 1
        double[] ps = new double[]{0, 0.1, 0.3, 0.5, 0.8, 0.9, 1};
        for (TDigest tDigest : tDigests) {
            List<Double> datas = new ArrayList<>();
            for (double p : ps) {
                datas.add(tDigest.quantile(p));
            }
            System.out.println(datas);
        }

    }

    private byte[] geneQuantileTDigest(int count, int max)  throws IOException {
        TDigest tDigest = TDigest.createMergingDigest(200);
        Random random = new Random();
        for (int i = 1; i <= count; i++) {
            int v = random.nextInt(max) + 1;
            //System.out.println(String.format("('2024-01-27 10:00:00', %d),", v));
            tDigest.add(v);
        }
        //System.out.println("----");

        Collection<Centroid> centroids = tDigest.centroids();
        byte[] bytes = new byte[computeVarIntSize(centroids.size()) + centroids.size() * 8];
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        writeVarInt(buffer, centroids.size());
        for (Centroid centroid : centroids) {
            buffer.putFloat((float) centroid.mean());
            buffer.putFloat((float) centroid.count());
        }
        return bytes;
    }


    @Test
    public void testVarInt()throws IOException{
        ByteBuffer buffer = ByteBuffer.allocate(10).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < 100000; i+=100) {
            int size = computeVarIntSize(i);
            buffer.limit(size);
            writeVarInt(buffer, i);
            assert ! buffer.hasRemaining();
            buffer.flip();
            assert i == readVarInt(buffer);
            buffer.clear();
        }
    }
    public static long readVarInt(ByteBuffer buffer) throws IOException {
        int number = 0;
        for (int i = 0; i < 9; i++) {
            int byt = buffer.get();

            number |= (byt & 0x7F) << (7 * i);

            if ((byt & 0x80) == 0) {
                break;
            }
        }
        return number;
    }

    public void writeVarInt(ByteBuffer buffer, long x) throws IOException {
        for (int i = 0; i < 9; i++) {
            byte byt = (byte) (x & 0x7F);

            if (x > 0x7F) {
                byt |= 0x80;
            }

            x >>= 7;
            buffer.put(byt);

            if (x == 0) {
                return;
            }
        }
    }

    public static int computeVarIntSize(long value) {
        // handle two popular special cases up front ...
        if ((value & (~0L << 7)) == 0L) {
            return 1;
        }
        if (value < 0L) {
            return 10;
        }
        // ... leaving us with 8 remaining, which we can divide and conquer
        int n = 2;
        if ((value & (~0L << 35)) != 0L) {
            n += 4;
            value >>>= 28;
        }
        if ((value & (~0L << 21)) != 0L) {
            n += 2;
            value >>>= 14;
        }
        if ((value & (~0L << 14)) != 0L) {
            n += 1;
        }
        return n;
    }


}
