package code.ponfee.hbase;

import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.Assert;

import code.ponfee.commons.math.Maths;
import code.ponfee.commons.serial.WrappedSerializer;
import code.ponfee.commons.util.Bytes;

/**
 * Hbase utility class
 * 
 * @author Ponfee
 */
public final class HbaseUtils {

    private static final int DEFAULT_PARTITION_COUNT = 100;
    private static final WrappedSerializer SERIALIZER = WrappedSerializer.WRAPPED_TOSTRING_SERIALIZER;

    public static String partition(Object source) {
        return partition(source, DEFAULT_PARTITION_COUNT);
    }

    public static String partition(Object source, int partition) {
        Assert.isTrue(partition > 0, "Hbase partition number must be greater than 0.");
        int len = String.valueOf(partition - 1).length();
        if (source == null) {
            return StringUtils.repeat('0', len);
        }

        int hash = Maths.abs(source.hashCode());
        return StringUtils.leftPad(Integer.toString(hash % partition), len, '0');
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

    public static byte[] toBytes(String s) {
        return s == null ? null : s.isEmpty() ? ArrayUtils.EMPTY_BYTE_ARRAY : Bytes.toBytes(s);
    }

    public static byte[] toBytes(Object value) {
        return SERIALIZER.serialize(value);
    }

    public static <T> T fromBytes(byte[] bytes, Class<T> clazz) {
        return SERIALIZER.deserialize(bytes, clazz);
    }

    // ---------------------------------------------------------------private methods
    private static byte[] paddingRowKey(String rowKeyPrefix, 
                                        int paddingLength, byte padding) {
        byte[] rowKeyBytes = Bytes.toBytes(rowKeyPrefix);
        int fromIndex = rowKeyBytes.length;
        int toIndex = fromIndex + paddingLength;
        rowKeyBytes = Arrays.copyOf(rowKeyBytes, toIndex);
        Arrays.fill(rowKeyBytes, fromIndex, toIndex, padding);
        return rowKeyBytes;
    }

}
