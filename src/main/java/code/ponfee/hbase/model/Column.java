/* __________              _____                                          *\
** \______   \____   _____/ ____\____   ____        Ponfee's code         **
**  |     ___/  _ \ /    \   __\/ __ \_/ __ \       (c) 2017-2019, MIT    **
**  |    |  (  <_> )   |  \  | \  ___/\  ___/       http://www.ponfee.cn  **
**  |____|   \____/|___|  /__|  \___  >\___  >                            **
**                      \/          \/     \/                             **
\*                                                                        */

package code.ponfee.hbase.model;

import java.lang.reflect.Field;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.FastDateFormat;

import com.google.common.collect.ImmutableList;

import code.ponfee.commons.serial.Serializer;
import code.ponfee.commons.util.Bytes;
import code.ponfee.hbase.annotation.HbaseField;

/**
 * Config java bean & hbase column mapping configuration
 * 
 * @author Ponfee
 */
public class Column implements java.io.Serializable {

    private static final long serialVersionUID = 3502285331674687682L;

    private final Field field;
    private final String family;
    private final String qualifier;
    private final Serializer serializer;
    private final List<String> format;

    private final byte[] familyBytes;
    private final byte[] qualifierBytes;
    private final DateBytesConvertor dateBytesConvert;

    public Column(Field field, String family, String qualifier,
                  Serializer serializer, String[] format) {
        this.field = field;
        this.family = family;
        this.qualifier = qualifier;
        this.serializer = serializer;
        this.format = ImmutableList.copyOf(
            Arrays.stream(format == null ? ArrayUtils.EMPTY_STRING_ARRAY : format)
                  .filter(StringUtils::isNotBlank).map(String::trim).distinct()
                  .toArray(String[]::new)
        );

        this.familyBytes = family.getBytes();
        this.qualifierBytes = qualifier.getBytes();

        String pattern = this.format.isEmpty() ? null : this.format.get(0);
        if (   StringUtils.isNotBlank(pattern) 
            && Date.class.isAssignableFrom(field.getType())
        ) {
            pattern = pattern.trim();
            this.dateBytesConvert = HbaseField.FORMAT_TIMESTAMP.equals(pattern) 
                                  ? DateBytesConvertor.TIMESTAMP 
                                  : new DefaultDateBytesConvertor(this.format.toArray(new String[0]));
        } else {
            this.dateBytesConvert = null;
        }
    }

    public Field getField() {
        return field;
    }

    public String getFamily() {
        return family;
    }

    public String getQualifier() {
        return qualifier;
    }

    public Serializer getSerializer() {
        return serializer;
    }

    public List<String> getFormat() {
        return format;
    }

    public byte[] getFamilyBytes() {
        return familyBytes;
    }

    public byte[] getQualifierBytes() {
        return qualifierBytes;
    }

    public DateBytesConvertor getDateBytesConvert() {
        return dateBytesConvert;
    }

    public static abstract class DateBytesConvertor {
        public static DateBytesConvertor TIMESTAMP = new DateBytesConvertor() {
            @Override
            protected byte[] toBytes0(Date date) {
                System.out.println(date.getTime());
                return Bytes.toBytes(date.getTime());
            }

            @Override
            protected Date toDate0(byte[] bytes) {
                System.out.println(Bytes.toLong(bytes));
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

    public static class DefaultDateBytesConvertor extends DateBytesConvertor {
        private final FastDateFormat dateFormat;
        private final String[] patterns;

        public DefaultDateBytesConvertor(String[] patterns) {
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

}
