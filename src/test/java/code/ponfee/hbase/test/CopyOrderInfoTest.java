package code.ponfee.hbase.test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Resource;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import code.ponfee.hbase.BaseTest;
import code.ponfee.hbase.model.PageQueryBuilder;
import code.ponfee.hbase.other.BasOrderInfoDao;
import code.ponfee.hbase.other.CopyOrderInfo;
import code.ponfee.hbase.other.CopyOrderInfoDao;

public class CopyOrderInfoTest extends BaseTest<CopyOrderInfoDao> {

    private static final int PAGE_SIZE = 5000;
    private @Resource BasOrderInfoDao basHbaseDao;

    @Test
    public void tableExists() {
        System.out.println(getBean().tableExists());
    }

    @Test
    public void dropTable() {
        System.out.println(getBean().dropTable());
    }

    @Test
    public void createTable() {
        System.out.println(getBean().createTable());
    }

    @Test
    public void descTable() {
        System.out.println(getBean().descTable());
    }

    @Test
    public void get() {
        consoleJson(getBean().get("abc"));
    }

    @Test
    public void first() {
        consoleJson(getBean().first());
    }

    @Test
    public void last() {
        consoleJson(getBean().last());
    }

    @Test
    public void range() {
        consoleJson(getBean().range(null, null));
    }

    @Test
    public void find() {
        consoleJson(getBean().find("abc", 20));
    }

    @Test
    public void nextPageAll() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE);
        query.addColumns("cf1",  "name");
        List<CopyOrderInfo> data = new ArrayList<>();
        int count = 1;
        List<CopyOrderInfo> list = (List<CopyOrderInfo>) getBean().nextPage(query);
        while (CollectionUtils.isNotEmpty(list) && list.size() == query.pageSize()) {
            count++;
            data.addAll(list);
            consoleJson(list);
            consoleJson((String) query.nextPageStartRow(list).getRowKey());
            query.startRowKey((String) query.nextPageStartRow(list).getRowKey());
            list = (List<CopyOrderInfo>) getBean().nextPage(query);
        }
        if (CollectionUtils.isNotEmpty(list)) {
            data.addAll(list);
        }
        Set<String> set = new LinkedHashSet<>();
        //Set<String> set = new TreeSet<>();
        data.stream().forEach(m -> set.add((String) m.getRowKey()));
        System.out.println("======================count: " + count);
        System.out.println("======================" + set.size());
        consoleJson(set);
    }

    @Test
    public void count() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE);
        consoleJson("======================" + getBean().count(query));
    }

    @Test
    public void copy() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE);
        query.prefixRowKey(Bytes.toBytes("a"));
        query.startRowKey("a20160401_S1603310002862_03.21.3213102-T", true);
        query.stopRowKey("a20160401_S1603310004352_03.21.3211104-W");
        basHbaseDao.copy(query, getBean(), t -> {
            t.setModelId(1);
            t.buildRowKey();
        });
    }

    @Test
    public void copy2() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE);
        query.prefixRowKey(Bytes.toBytes("a"));
        String startRow = basHbaseDao.nextRowKey("a", "a20160401_"); // 可以直接写a20160401_
        String stopRow = basHbaseDao.previousRowKey("a", "a20160403_");
        query.startRowKey(startRow, true);
        query.stopRowKey(stopRow);
        basHbaseDao.copy(query, getBean(), t -> {
            t.setModelId(1);
            t.buildRowKey();
        });
    }

    @Test
    public void nextRowKey() {
        // abc
        consoleJson(basHbaseDao.nextRowKey("a", "a20160401_"));
    }

    @Test
    public void previousRowKey() {
        // a20160402_S1604050001672_03.24.3241104-P
        consoleJson(basHbaseDao.previousRowKey("a", "a20160403_"));

        // a20180926_S1809260015952_07.07.7705017
        consoleJson(basHbaseDao.previousRowKey("a", "a20360403_"));
    }

    @Test
    public void previousRowKey2() {
        // a20160402_S1604050001672_03.24.3241104-P
        consoleJson(basHbaseDao.maxRowKey("a", "a20160402_", 50));

        // a20180926_S1809260015952_07.07.7705017
        consoleJson(basHbaseDao.maxRowKey("a", "a20460400_", 50));

        // a20000402_
        consoleJson(basHbaseDao.maxRowKey("a", "a20000402_", 50));
    }

    @Test
    public void previousRowKey3() {
        // 6_57095810-6_20180926_CK2018092691_JXJ605060038
        consoleJson(basHbaseDao.previousRowKey(null, null));

        consoleJson(basHbaseDao.previousRowKey("a", null)); // 全表扫描
    }

}
