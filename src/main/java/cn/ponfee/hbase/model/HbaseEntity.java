package cn.ponfee.hbase.model;

import cn.ponfee.hbase.HbaseUtils;
import org.apache.commons.lang3.builder.CompareToBuilder;

import java.beans.Transient;
import java.io.Serializable;
import java.util.Objects;

/**
 * The base entity(ORM) class for mapped hbase table
 * 
 * @author Ponfee
 * @param <R> the row key type
 */
public interface HbaseEntity<R extends Comparable<? super R> & Serializable>
    extends Comparable<HbaseEntity<R>>, Serializable {

    /**
     * Returns the hbase row key
     * 
     * @return a hbase row key
     */
    R getRowKey();

    /**
     * Returns the row number for current page result
     * 
     * @return a int row number of page
     */
    int getRowNum();

    /**
     * Sets row key to hbase entity
     * 
     * @param rowKey the hbase row key
     */
    void setRowKey(R rowKey);

    /**
     * Sets row number for page data list
     * 
     * @param rowNum the current page data list row number
     */
    void setRowNum(int rowNum);

    //int getTimestamp();
    //int getSequenceId();

    // ----------------------------------------------default methods
    /**
     * Returns the data object hbase rowkey, 
     * sub class can override this methods
     * 
     * @return a rowkey
     */
    default R buildRowKey() {
        return this.getRowKey();
    }

    /**
     * Returns a string of row key,
     * Sub class can override this method
     * 
     * @return row key as string
     */
    @Transient
    default String getRowKeyAsString() {
        return Objects.toString(getRowKey(), null);
    }

    /**
     * Returns a byte array of row key,
     * Sub class can override this method
     * 
     * @return row key as byte array
     */
    @Transient
    default byte[] getRowKeyAsBytes() {
        return HbaseUtils.toBytes(getRowKey());
    }

    // -------------------------------------------Comparable & Object
    @Override
    default int compareTo(HbaseEntity<R> other) {
        return new CompareToBuilder()
            .append(this.getRowKey(), other.getRowKey())
            .toComparison();
    }

    default int hashCode0() {
        R rowKey;
        return (rowKey = this.getRowKey()) == null 
               ? 0 : rowKey.hashCode();

        /*return new HashCodeBuilder()
            .append(this.getRowKey())
            .toHashCode();*/
    }

    @SuppressWarnings("unchecked")
    default boolean equals0(Object obj) {
        if (!this.getClass().isInstance(obj)) {
            return false;
        }

        R tkey, okey;
        return (tkey = this.getRowKey()) != null
            && (okey = ((HbaseEntity<R>) obj).getRowKey()) != null
            && tkey.equals(okey);

        /*return new EqualsBuilder()
                .append(this.getRowKey(), ((HbaseMap<?, R>) obj)
                .getRowKey()).isEquals();*/
    }

    default String toString0() {
        return new StringBuilder(this.getClass().getName())
            .append("@").append(this.getRowKey()).toString();
        //return new ToStringBuilder(this).toString();
    }

}
