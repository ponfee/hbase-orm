package code.ponfee.hbase.other;

import code.ponfee.hbase.Constants;
import code.ponfee.hbase.annotation.HbaseTable;
import code.ponfee.hbase.model.HbaseMap;

@HbaseTable(namespace = Constants.HBASE_NAMESPACE, tableName = "t_test_map", family = "cf1")
public class ExtendsHbaseMap extends HbaseMap<String> {

    private static final long serialVersionUID = 1L;

    @Override
    public String buildRowKey() {
        return super.getRowKeyAsString();
    }

}
