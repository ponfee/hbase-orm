package code.ponfee.hbase.other;

import org.springframework.stereotype.Repository;

import code.ponfee.hbase.HbaseBatchDao;

@Repository("extendsHbaseMapDao")
public class ExtendsHbaseMapDao extends HbaseBatchDao<ExtendsHbaseMap, String> {

}
