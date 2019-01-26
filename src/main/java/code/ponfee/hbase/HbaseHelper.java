package code.ponfee.hbase;

import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.util.Assert;

import code.ponfee.commons.collect.ByteArrayWrapper;

/**
 * Hbase helper
 * 
 * @author Ponfee
 */
public class HbaseHelper {

    private static final int DEFAULT_PARTITION_COUNT = 100;
    public static final byte[] EMPTY_BYTES = {}; // new byte[0]

    public static String partition(Object source) {
        return partition(source, DEFAULT_PARTITION_COUNT);
    }

    public static String partition(Object source, int partition) {
        Assert.isTrue(partition > 0, "Hbase partition number must be greater than 0.");
        int len = String.valueOf(partition - 1).length();
        if (source == null) {
            return StringUtils.repeat('0', len);
        }

        int salt = Math.abs(source.hashCode());
        return StringUtils.leftPad(String.valueOf(salt % partition), len, '0');
    }

    // -----------------------------------------------------------next row key
    public static byte[] nextStartRowKey(byte[] thisStartRowKey) {
        return ArrayUtils.add(thisStartRowKey, (byte) 0x00);
    }

    public static byte[] nextStartRowKey(String thisStartRowKey) {
        return paddingStartRowKey(thisStartRowKey, 1);
    }

    public static byte[] paddingStartRowKey(String rowKeyPrefix, int paddingLength) {
        return paddingRowKey(rowKeyPrefix, paddingLength, (byte) 0x00);
    }

    public static byte[] paddingStopRowKey(String rowKeyPrefix, int paddingLength) {
        return paddingRowKey(rowKeyPrefix, paddingLength, (byte) 0xFF);
    }

    private static byte[] paddingRowKey(String rowKeyPrefix, 
                                        int paddingLength, byte padding) {
        byte[] rowKeyBytes = Bytes.toBytes(rowKeyPrefix);
        int fromIndex = rowKeyBytes.length;
        int toIndex = fromIndex + paddingLength;
        rowKeyBytes = Arrays.copyOf(rowKeyBytes, toIndex);
        Arrays.fill(rowKeyBytes, fromIndex, toIndex, padding);
        return rowKeyBytes;
    }

    public static byte[] toBytes(String str) {
        if (str == null) {
            return null;
        } else if (str.isEmpty()) {
            return EMPTY_BYTES;
        } else {
            return Bytes.toBytes(str);
        }
    }

    public static byte[] toBytes(Object obj) {
        if (obj == null) {
            return null;
        } else if (obj instanceof byte[]) {
            return (byte[]) obj;
        } else if (obj instanceof Byte[]) {
            return ArrayUtils.toPrimitive((Byte[]) obj);
        } else if (obj instanceof ByteArrayWrapper) {
            return ((ByteArrayWrapper) obj).getArray();
        } else if (obj instanceof ByteBuffer) {
            return ((ByteBuffer) obj).array();
        } else if (obj instanceof InputStream) {
            try (InputStream input = (InputStream) obj) {
                return IOUtils.toByteArray(input);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (obj instanceof Date) {
            return Bytes.toBytes(((Date) obj).getTime());
        } else {
            String str; // first to string and then to byte array
            if (isEmpty(str = obj.toString())) {
                return EMPTY_BYTES;
            }
            return Bytes.toBytes(str);
        }
    }
}
