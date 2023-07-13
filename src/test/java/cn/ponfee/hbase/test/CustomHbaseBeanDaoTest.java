package cn.ponfee.hbase.test;

import cn.ponfee.commons.date.Dates;
import cn.ponfee.commons.json.Jsons;
import cn.ponfee.commons.model.SortOrder;
import cn.ponfee.hbase.HbaseBatchDao;
import cn.ponfee.hbase.SpringBaseTest;
import cn.ponfee.hbase.model.PageQueryBuilder;
import cn.ponfee.hbase.test.CustomHbaseBeanDaoTest.CustomHbaseBeanDao;
import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class CustomHbaseBeanDaoTest extends SpringBaseTest<CustomHbaseBeanDao> {

    @Repository("customHbaseBeanDao")
    public static class CustomHbaseBeanDao extends HbaseBatchDao<CustomHbaseBean, String> {
    }

    private static final int PAGE_SIZE = 50;

    @Test
    //@Ignore
    public void dropTable() {
        System.out.println(getBean().dropTable());
    }

    @Test
    //@Ignore
    public void createTable() {
        System.out.println(getBean().createTable());
    }

    @Test
    public void descTable() {
        System.out.println(getBean().descTable());
    }

    @Test
    //@Ignore
    public void batchPut() {
        List<CustomHbaseBean> batch = new ArrayList<>();
        for (int i = 0; i < 1; i++) {
            CustomHbaseBean entity = new CustomHbaseBean();
            entity.setFirstName("fu");
            entity.setLastName("ponfee");
            entity.setAge(ThreadLocalRandom.current().nextInt(60) + 10);
            entity.setBirthday(Dates.random(Dates.ofTimeMillis(0), Dates.toDate("20000101", "yyyyMMdd")));
            entity.buildRowKey();
            batch.add(entity);
        }
        consoleJson(getBean().put(batch));
    }

    @Test
    public void get() {
        consoleJson(getBean().get("fu_ponfee_20181009"));
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
    public void nextRowKey() {
        consoleJson(getBean().nextRowKey("fu_ponfee_2007", "fu_ponfee_2009"));
    }

    @Test
    public void previousRowKey() {
        consoleJson(getBean().previousRowKey("fu_ponfee_200", "fu_ponfee_2001"));
    }

    @Test
    public void range() {
        consoleJson(getBean().range("fu_ponfee_2001", "fu_ponfee_2002"));
    }

    @Test
    public void find() {
        consoleJson(getBean().find("fu_ponfee_20000101", "fu_ponfee_20090101", 2000));
        consoleJson(getBean().find("fu_ponfee_20000101", "fu_ponfee_20090101", 2));
        consoleJson(getBean().find("fu_ponfee_20090101", "fu_ponfee_20000101", 2000, true));
        consoleJson(getBean().find("fu_ponfee_20090101", "fu_ponfee_20000101", 2, true));
    }

    @Test
    public void findAll() {
        List<CustomHbaseBean> list = getBean().range(null, null);
        System.out.println("======================" + list.size());
        consoleJson(list);
    }

    @Test
    public void nextPage() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(5, SortOrder.ASC);
        query.addColumns("cf1", "first_name");
        query.addColumns("cf1", "age");
        //query.startRowKey("fu_ponfee_20070309");
        query.prefixRowKey("fu_ponfee_200703".getBytes());
        //query.regexpRowKey("^fu_ponfee_2\\d{2}1.*1$");
        consoleJson(getBean().nextPage(query));
    }

    @Test
    public void nextPageAll() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE, SortOrder.ASC);
        query.addColumns("cf1", "first_name");
        //Set<String> set = new TreeSet<>();
        Set<String> set = new LinkedHashSet<>();
        getBean().scrollQuery(query, (pageNum, data) -> {
            System.err.println("======================pageNum: " + pageNum);
            consoleJson(data);
            data.stream().forEach(m -> set.add(m.getRowKey()));
        });
        System.err.println("======================" + set.size());
        consoleJson(set);
    }

    @Test
    public void previousPage() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(5, SortOrder.DESC);
        query.startRowKey("fu_ponfee_20121019");
        consoleJson(getBean().previousPage(query));
    }

    @Test
    public void previousPageAll() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(11, SortOrder.DESC);
        //query.setStartRow("fu_ponfee_20181128");
        //query.setFamQuaes(ImmutableMap.of("cf1", new String[] { "first_name" }));
        List<CustomHbaseBean> data = new ArrayList<>();
        int count = 1;
        List<CustomHbaseBean> list = getBean().previousPage(query);
        while (CollectionUtils.isNotEmpty(list) && list.size() == query.pageSize()) {
            count++;
            data.addAll(list);
            consoleJson(list);
            consoleJson(query.previousPageStartRow(list).getRowKey());
            query.startRowKey(query.previousPageStartRow(list).getRowKey());
            list = getBean().previousPage(query);
        }
        if (CollectionUtils.isNotEmpty(list)) {
            data.addAll(list);
            consoleJson(list);
            consoleJson(query.previousPageStartRow(list).getRowKey());
        }
        Set<String> set = new LinkedHashSet<>();
        //Set<String> set = new TreeSet<>();
        data.stream().forEach(m -> set.add(m.getRowKey()));
        System.out.println("======================round: " + count);
        System.out.println("======================" + set.size());
        consoleJson(set);
    }

    @Test
    public void deletePage() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(3);
        query.prefixRowKey(Bytes.toBytes("fu_ponfee_2009"));
        consoleJson(getBean().delete(query));
    }

    @Test
    public void count() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE);
        query.prefixRowKey(Bytes.toBytes("fu_ponfee_201"));
        consoleJson("======================" + getBean().count(query));
    }

    // -------------------------------------------------------------------------------
    @Test
    public void prefix() {
        consoleJson(getBean().prefix("fu_ponfee_201", PAGE_SIZE));
    }

    @Test
    public void regexp() {
        consoleJson(getBean().regexp("^fu_ponfee_2\\d{2}1.*1$", 2));
    }

    @Test
    public void all() {
        consoleJson(getBean().all());
    }

    // -------------------------------------------------------------------------------page query
    @Test
    public void page1() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(2000);
        //query.addColumns("cf1", "age");
        //query.startRowKey("fu_ponfee_20010105");
        //query.stopRowKey( "fu_ponfee_20010313");
        //query.startRowKey("fu_ponfee_20010105", true);
        //query.stopRowKey( "fu_ponfee_20010313", true);
        //query.requireRowNum(false);
        //query.equals("cf1", "age", "26".getBytes());
        //query.notEquals("cf1", "age", "26".getBytes());
        //query.in("cf1", "age", new byte[][] {"26".getBytes(), "24".getBytes()});
        //query.notIn("cf1", "age", new byte[][] {"26".getBytes(), "24".getBytes()});
        //query.exists("cf1", "nonce");
        //query.notExists("cf1", "nonce");
        //query.greater("cf1", "age", "55".getBytes(), true);
        //query.greater("cf1", "age", "55".getBytes(), false);
        //query.less("cf1", "age", "10".getBytes(), true);
        //query.less("cf1", "age", "10".getBytes(), false);
        //query.regexp("cf1", "birthday", "2\\d{2}1\\d{4}");
        //query.notRegexp("cf1", "birthday", "2\\d{2}1\\d{4}");
        //query.prefix("cf1", "birthday", "2015".getBytes());
        //query.notPrefix("cf1", "birthday", "2015".getBytes());
        //query.like("cf1", "birthday", "060");
        //query.notLike("cf1", "birthday", "060");
        //query.rowKeyOnly();
        //query.likeRowKey("060");
        //query.notLikeRowKey("060");
        //query.regexpRowKey("^fu_ponfee_2\\d{2}1.*1$");
        //query.notRegexpRowKey("^fu_ponfee_2\\d{2}1.*1$");
        //query.prefixRowKey("fu_ponfee_2006".getBytes());
        //query.notPrefixRowKey("fu_ponfee_2006".getBytes());
        //query.equalsRowKey("fu_ponfee_20000214".getBytes());
        //query.notEqualsRowKey("fu_ponfee_20000214".getBytes());

        query.range("cf1", "birthday", "20000213".getBytes(), "20000501".getBytes());
        //query.notRange("cf1", "birthday", "20000213".getBytes(), "20000501".getBytes());
        printJson(getBean().nextPage(query));
    }

    @Test
    //@Ignore
    public void delete() {
        consoleJson(getBean().get("fu_ponfee_19840415"));
        consoleJson(getBean().delete(Lists.newArrayList("fu_ponfee_19840415", "fu_ponfee_20110531")));
        consoleJson(getBean().get("fu_ponfee_19840415"));
    }

    private static void printJson(Object obj) {
        System.err.println(Jsons.toJson(obj));
    }
}
