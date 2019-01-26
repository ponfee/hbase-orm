package code.ponfee.hbase.model;

import java.beans.Transient;
import java.io.Serializable;

import code.ponfee.hbase.HbaseHelper;

/**
 * The base bean class for mapped by hbase table
 * 
 * @author Ponfee
 * @param <R> the row key type
 */
public interface HbaseBean<R extends Comparable<? super R> & Serializable>
    extends Comparable<HbaseBean<R>>, Serializable {

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
     * Sets row key to hbase bean
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
        R rowKey;
        return (rowKey = getRowKey()) == null
               ? null : rowKey.toString();
    }

    /**
     * Returns a byte array of row key,
     * Sub class can override this method
     * 
     * @return row key as byte array
     */
    @Transient
    default byte[] getRowKeyAsBytes() {
        return HbaseHelper.toBytes(getRowKey());
    }

    // -------------------------------------------Comparable & Object
    default @Override int compareTo(HbaseBean<R> other) {
        R tkey, okey;
        if ((tkey = this.getRowKey()) == null) {
            return 1; // null rowkey last
        } else if ( ( other                      == null)
                 || ( (okey = other.getRowKey()) == null)
        ) {
            return -1;
        } else {
            return tkey.compareTo(okey);
        }
        /*return new CompareToBuilder()
            .append(this.getRowKey(), other.getRowKey())
            .toComparison();*/
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
        return (tkey = this.getRowKey()) == null
            || (okey = ((HbaseBean<R>) obj).getRowKey()) == null
            ? false : tkey.equals(okey);

        /*return new EqualsBuilder()
                .append(this.getRowKey(), ((HbaseMap<?, R>) obj)
                .getRowKey()).isEquals();*/
    }

    default String toString0() {
        return this.getClass().getName() + "@" + this.getRowKey();
        //return new ToStringBuilder(this).toString();
    }

}
