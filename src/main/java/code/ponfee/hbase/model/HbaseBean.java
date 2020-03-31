package code.ponfee.hbase.model;

import java.io.Serializable;

import code.ponfee.hbase.annotation.HbaseField;

/**
 * The Java bean class for mapped hbase table
 * 
 * @author Ponfee
 * @param <R> the row key type
 */
public abstract class HbaseBean<R extends Serializable & Comparable<? super R>>
    implements HbaseEntity<R> {

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
        return HbaseEntity.super.hashCode0();
    }

    @Override
    public boolean equals(Object obj) {
        return HbaseEntity.super.equals0(obj);
    }

    @Override
    public String toString() {
        return HbaseEntity.super.toString0();
    }

}
