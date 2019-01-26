package code.ponfee.hbase.other;

import org.springframework.stereotype.Repository;

import code.ponfee.hbase.HbaseBatchDao;

@Repository("extendsHbaseEntityDao")
public class ExtendsHbaseEntityDao extends HbaseBatchDao<ExtendsHbaseEntity, String> {

}
