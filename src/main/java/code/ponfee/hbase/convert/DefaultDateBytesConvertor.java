package code.ponfee.hbase.convert;

import java.text.ParseException;
import java.util.Date;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.FastDateFormat;

import code.ponfee.commons.util.Bytes;

/**
 * The default convert based FastDateFormat
 * 
 * @author Ponfee
 */
public class DefaultDateBytesConvertor extends DateBytesConvertor {

    private final FastDateFormat dateFormat;
    private final String[] patterns;

    public DefaultDateBytesConvertor(String... patterns) {
        this.dateFormat = FastDateFormat.getInstance(patterns[0].trim());
        this.patterns = patterns;
    }

    @Override
    protected byte[] toBytes0(Date date) {
        return Bytes.toBytes(this.dateFormat.format(date));
    }

    @Override
    protected Date toDate0(byte[] bytes) {
        String date = Bytes.toString(bytes);
        try {
            return DateUtils.parseDate(date, this.patterns); // date format
        } catch (ParseException e) {
            throw new RuntimeException("Invalid date format: " + date);
        }
    }
}
