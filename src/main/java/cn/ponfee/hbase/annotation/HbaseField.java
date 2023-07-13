package cn.ponfee.hbase.annotation;

import cn.ponfee.commons.serial.NullSerializer;
import cn.ponfee.commons.serial.Serializer;

import java.lang.annotation.*;

/**
 * Entity field mapping to hbase column
 * 
 * @author Ponfee
 */
@Documented
@Inherited
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface HbaseField {

    /** ignore as hbase qualifier */
    boolean ignore() default false;

    /** field value serializer */
    Class<? extends Serializer> serializer() default NullSerializer.class;

    /** the column-level hbase family name. */
    String family() default "";

    /** the hbase qualifier name, default LOWER_CAMEL.to(field.getName()). */
    String qualifier() default "";

    /** the hbase value format, to compatible a field has 
     * multiple format date value so this is array. */
    String[] format() default {};

    // ---------------------------------------public static final field

    String FORMAT_TIMESTAMP = "timestamp";
}
