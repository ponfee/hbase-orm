package cn.ponfee.hbase.test;

import cn.ponfee.hbase.Constants;
import cn.ponfee.hbase.annotation.HbaseTable;
import cn.ponfee.hbase.model.HbaseMap;

@HbaseTable(namespace = Constants.HBASE_NAMESPACE, tableName = "test_hbase_map", family = "cf1")
public class CustomHbaseMap extends HbaseMap<String> {

    private static final long serialVersionUID = 1L;

    @Override
    public String buildRowKey() {
        return super.getRowKeyAsString();
    }

}