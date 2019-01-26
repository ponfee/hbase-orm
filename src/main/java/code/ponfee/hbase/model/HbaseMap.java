package code.ponfee.hbase.model;

import java.io.Serializable;
import java.util.HashMap;

/**
 * The Map class for mapped by hbase table
 * 
 * @author Ponfee
 * @param <R> the row key type
 */
public abstract class HbaseMap<R extends Comparable<? super R> & Serializable>
    extends HashMap<String, Object> implements HbaseBean<R> {

    private static final long serialVersionUID = 2482090979352032846L;

    /** The hbase row key name */
    public static final String ROW_KEY_NAME = "rowKey";
    public static final String ROW_NUM_NAME = "rowNum";
    //public static final String TIMESTAMP_NAME = "timestamp";
    //public static final String SEQUENCE_ID_NAME = "sequenceId";

    @Override @SuppressWarnings("unchecked")
    public final R getRowKey() {
        return (R) this.get(ROW_KEY_NAME);
    }

    @Override
    public final int getRowNum() {
        Object rowNum = this.get(ROW_NUM_NAME);
        if (rowNum == null) {
            return 0;
        } else if (rowNum instanceof Number) {
            return ((Number) rowNum).intValue();
        } else {
            try {
                return Integer.parseInt(rowNum.toString());
            } catch (NumberFormatException e) {
                return 0;
            }
        }
    }

    @Override
    public final void setRowKey(R rowKey) {
        this.put(ROW_KEY_NAME, rowKey);
    }

    @Override
    public final void setRowNum(int rowNum) {
        this.put(ROW_NUM_NAME, rowNum);
    }

    @Override
    public int hashCode() {
        return HbaseBean.super.hashCode0();
    }

    @Override
    public boolean equals(Object obj) {
        return HbaseBean.super.equals0(obj);
    }

    @Override
    public String toString() {
        return HbaseBean.super.toString0();
    }

}
