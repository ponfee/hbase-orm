package code.ponfee.hbase.annotation;

import java.lang.annotation.*;

import code.ponfee.commons.serial.GeneralSerializer;
import code.ponfee.commons.serial.Serializer;

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
    Class<? extends Serializer> serializer() default GeneralSerializer.class;
}
