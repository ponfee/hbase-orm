package code.ponfee.hbase.test1;

import code.ponfee.hbase.Constants;
import code.ponfee.hbase.annotation.HbaseTable;
import code.ponfee.hbase.model.HbaseMap;

@HbaseTable(namespace = Constants.HBASE_NAMESPACE, tableName = "t_bean_map", family = "cf1")
public class BeanMap extends HbaseMap<String> {

    private static final long serialVersionUID = 1L;

    @Override
    public String buildRowKey() {
        return super.getRowKeyAsString();
    }

}