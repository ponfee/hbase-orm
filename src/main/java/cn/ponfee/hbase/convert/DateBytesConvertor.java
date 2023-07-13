package cn.ponfee.hbase.convert;

import cn.ponfee.commons.util.Bytes;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Date;

/**
 * Date and byte array convert
 * 
 * @author Ponfee
 */
public abstract class DateBytesConvertor {

    public static final DateBytesConvertor TIMESTAMP = new DateBytesConvertor() {
        @Override
        protected byte[] toBytes0(Date date) {
            return Bytes.toBytes(date.getTime());
        }

        @Override
        protected Date toDate0(byte[] bytes) {
            return new Date(Bytes.toLong(bytes));
        }
    };

    protected abstract byte[] toBytes0(Date date);

    protected abstract Date toDate0(byte[] bytes);

    public final byte[] toBytes(Date date) {
        return date == null ? null : toBytes0(date);
    }

    public final Date toDate(byte[] bytes) {
        return ArrayUtils.isEmpty(bytes) ? null : toDate0(bytes);
    }
}
