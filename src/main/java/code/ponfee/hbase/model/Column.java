/* __________              _____                                          *\
** \______   \____   _____/ ____\____   ____        Ponfee's code         **
**  |     ___/  _ \ /    \   __\/ __ \_/ __ \       (c) 2017-2019, MIT    **
**  |    |  (  <_> )   |  \  | \  ___/\  ___/       http://www.ponfee.cn  **
**  |____|   \____/|___|  /__|  \___  >\___  >                            **
**                      \/          \/     \/                             **
\*                                                                        */

package code.ponfee.hbase.model;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableList;

import code.ponfee.commons.serial.Serializer;
import code.ponfee.hbase.annotation.HbaseField;
import code.ponfee.hbase.convert.DateBytesConvertor;
import code.ponfee.hbase.convert.DefaultDateBytesConvertor;

/**
 * Config java bean & hbase column mapping configuration
 * 
 * @author Ponfee
 */
public class Column {

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
            Arrays.stream(ObjectUtils.defaultIfNull(format, ArrayUtils.EMPTY_STRING_ARRAY))
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
                                  : new DefaultDateBytesConvertor(this.format.toArray(new String[this.format.size()]));
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

}
