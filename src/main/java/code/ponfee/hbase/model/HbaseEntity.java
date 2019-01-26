package code.ponfee.hbase.model;

import java.io.Serializable;

import code.ponfee.hbase.annotation.HbaseField;

/**
 * the Entity Class for mapped by hbase table
 * 
 * @author Ponfee
 * @param <R> the row key type
 */
public abstract class HbaseEntity<R extends Serializable & Comparable<? super R>>
    implements HbaseBean<R> {

    private static final long serialVersionUID = 2467942701509706341L;

    @HbaseField(ignore = true)
    protected R rowKey;

    @HbaseField(ignore = true)
    protected int rowNum;

    /*@HbaseField(ignore = true)
    protected int sequenceId;
    @HbaseField(ignore = true)
    protected int timestamp;*/

    @Override
    public final R getRowKey() {
        return rowKey;
    }

    @Override
    public final int getRowNum() {
        return rowNum;
    }

    @Override
    public final void setRowKey(R rowKey) {
        this.rowKey = rowKey;
    }

    @Override
    public final void setRowNum(int rowNum) {
        this.rowNum = rowNum;
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
