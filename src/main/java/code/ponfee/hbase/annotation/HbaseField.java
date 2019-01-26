package code.ponfee.hbase.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import code.ponfee.commons.serial.Serializer;
import code.ponfee.commons.serial.GeneralSerializer;

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
    Class<? extends Serializer> serializer() default GeneralSerializer.class;

    /** the column-level hbase family name. */
    String family() default "";

    /** the hbase qualifier name. */
    String qualifier() default "";

    /** the hbase value format, to compatible a field has 
     * multiple format date value so this is array. */
    String[] format() default {};

    // ---------------------------------------public static final field
    String FORMAT_TIMESTAMP = "timestamp";
}
