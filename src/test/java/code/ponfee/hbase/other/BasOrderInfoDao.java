package code.ponfee.hbase.other;

import org.springframework.stereotype.Repository;

import code.ponfee.hbase.HbaseBatchDao;

@Repository("basOrderInfoDao")
public class BasOrderInfoDao extends HbaseBatchDao<BasOrderInfo, String> {

}
