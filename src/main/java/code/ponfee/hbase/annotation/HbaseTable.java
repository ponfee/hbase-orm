package code.ponfee.hbase.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import code.ponfee.commons.serial.Serializer;
import code.ponfee.commons.serial.ToStringSerializer;

/**
 * Mapped by hbase table name
 * 
 * @author Ponfee
 */
@Documented
@Inherited
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface HbaseTable {

    /** hbase table namespace */
    String namespace() default "";

    /** hbase table name, default LOWER_UNDERSCORE(Class.getSimpleName()) */
    String tableName() default "";

    /** the table-level hbase family name */
    String family() default "";

    /** row key serializer */
    Class<? extends Serializer> serializer() default ToStringSerializer.class;
}
