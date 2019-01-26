package code.ponfee.hbase.other;

import org.springframework.stereotype.Repository;

import code.ponfee.hbase.HbaseBatchDao;

@Repository("copyOrderInfoDao")
public class CopyOrderInfoDao extends HbaseBatchDao<CopyOrderInfo, String> {

}
