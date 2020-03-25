package code.ponfee.hbase;

import static code.ponfee.commons.serial.WrappedSerializer.WRAPPED_TOSTRING_SERIALIZER;

import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.Assert;

import code.ponfee.commons.math.Maths;
import code.ponfee.commons.math.Numbers;
import code.ponfee.commons.util.Bytes;

/**
 * Hbase utility class
 * 
 * @author Ponfee
 */
public final class HbaseUtils {

    public static final int DEFAULT_PARTITIONS = 100;

    public static String partition(Object source) {
        return partition(source, DEFAULT_PARTITIONS);
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
        return WRAPPED_TOSTRING_SERIALIZER.serialize(value);
    }

    public static <T> T fromBytes(byte[] bytes, Class<T> clazz) {
        return WRAPPED_TOSTRING_SERIALIZER.deserialize(bytes, clazz);
    }

    // ---------------------------------------------------------------private methods
    private static byte[] paddingRowKey(String rowKeyPrefix, 
                                        int paddingLength, byte padding) {
        byte[] prefixBytes = Bytes.toBytes(rowKeyPrefix);
        int length = prefixBytes.length + paddingLength;
        byte[] rowKeyBytes = Arrays.copyOf(prefixBytes, length);
        if (padding != Numbers.BYTE_ZERO) {
            Arrays.fill(rowKeyBytes, prefixBytes.length, length, padding);
        }
        return rowKeyBytes;
    }

}
