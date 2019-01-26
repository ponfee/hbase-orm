package code.ponfee.hbase.other;

import code.ponfee.commons.util.ObjectUtils;
import code.ponfee.hbase.Constants;
import code.ponfee.hbase.annotation.HbaseTable;

@HbaseTable(namespace = Constants.HBASE_NAMESPACE, tableName = "t_copy_order_info", family="cf1")
public class CopyOrderInfo extends BasOrderInfo {

    private static final long serialVersionUID = 1L;

    private int modelId;
    private String rowKey0;

    @Override
    public String buildRowKey() {
        this.rowKey0 = super.buildRowKey();
        return super.rowKey = String.join(
            "_", String.valueOf(modelId), ObjectUtils.uuid32()
        );
    }

    public int getModelId() {
        return modelId;
    }

    public void setModelId(int modelId) {
        this.modelId = modelId;
    }

    public String getRowKey0() {
        return rowKey0;
    }

    public void setRowKey0(String rowKey0) {
        this.rowKey0 = rowKey0;
    }

}
