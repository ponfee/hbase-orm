package code.ponfee.hbase.test;

import static code.ponfee.hbase.model.HbaseMap.ROW_KEY_NAME;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

import code.ponfee.commons.model.PageSortOrder;
import code.ponfee.commons.util.Dates;
import code.ponfee.hbase.BaseTest;
import code.ponfee.hbase.model.PageQueryBuilder;
import code.ponfee.hbase.other.ExtendsHbaseMap;
import code.ponfee.hbase.other.ExtendsHbaseMapDao;

public class HbaeDaoMapTest extends BaseTest<ExtendsHbaseMapDao> {

    private static final int PAGE_SIZE = 11;

    @Test
    @Ignore
    public void dropTable() {
        System.out.println(getBean().dropTable());
    }

    @Test
    @Ignore
    public void createTable() {
        System.out.println(getBean().createTable());
    }

    @Test
    public void descTable() {
        System.out.println(getBean().descTable());
    }

    @Test
    @Ignore
    public void batchPut() {
        int count = 50;
        List<ExtendsHbaseMap> batch = new ArrayList<>();
        Date date = Dates.toDate("20000101", "yyyyMMdd");
        for (int start = 3, i = start; i < count + start; i++) {
            ExtendsHbaseMap map = new ExtendsHbaseMap();
            map.put("age", 1 + ThreadLocalRandom.current().nextInt(60));
            map.put("name", RandomStringUtils.randomAlphanumeric(5));
            map.put("rowKey", Dates.format(Dates.random(date), "yyyyMMddHHmmss"));
            batch.add(map);
        }
        consoleJson(getBean().put(batch));
    }

    @Test
    public void get() {
        consoleJson(getBean().get("20000201211046"));
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
        consoleJson(getBean().range("20000201211046", "20060201211046"));
    }

    @Test
    public void find() {
        consoleJson(getBean().find("20041014150203", "20050828085930", 20));
        consoleJson(getBean().find("20041014150203", "20050828085930", 2));
        consoleJson(getBean().find("20050828085930", "20041014150203", 20, true));
        consoleJson(getBean().find("20050828085930", "20041014150203", 2, true));
    }

    @Test
    public void findAll() {
        List<ExtendsHbaseMap> list = (List<ExtendsHbaseMap>) getBean().range(null, null);
        System.out.println("======================" + list.size());
        consoleJson(list);
    }

    @Test
    public void nextPage() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE, PageSortOrder.DESC);
        query.requireRowNum(false);
        //query.rowKeyOnly();
        consoleJson(getBean().nextPage(query));
    }

    @Test
    public void nextPageAll() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(111, PageSortOrder.ASC);
        query.startRowKey("00000000");
        //query.setRowKeyPrefix("fu_ponfee_2009");
        //Set<String> set = new TreeSet<>();
        Set<String> set = new LinkedHashSet<>();
        getBean().scrollQuery(query, (pageNum, data)->{
            System.err.println("======================pageNum: " + pageNum);
            consoleJson(data);
            data.stream().forEach(m -> set.add((String)m.getRowKey()));
        });
        System.err.println("======================" + set.size());
        consoleJson(set);
    }

    @Test
    public void previousPage() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE);
        query.startRowKey("20050828085930");
        consoleJson(getBean().previousPage(query));
    }
    
    @Test
    public void previousPageDesc() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE, PageSortOrder.DESC);
        query.startRowKey("20050828085930");
        consoleJson(getBean().previousPage(query));
    }

    @Test
    public void previousPageAll() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(111);
        query.addColumns("cf1", "name");
        query.startRowKey("20181004162958");
        List<ExtendsHbaseMap> data = new ArrayList<>();
        int count = 1;
        List<ExtendsHbaseMap> list = (List<ExtendsHbaseMap>) getBean().previousPage(query);
        while (CollectionUtils.isNotEmpty(list) && list.size() == query.pageSize()) {
            count ++;
            data.addAll(list);
            consoleJson(list);
            consoleJson((String) query.previousPageStartRow(list).get(ROW_KEY_NAME));
            query.startRowKey((String) query.previousPageStartRow(list).get(ROW_KEY_NAME));
            list = (List<ExtendsHbaseMap>) getBean().previousPage(query);
        }
        if (CollectionUtils.isNotEmpty(list)) {
            data.addAll(list);
        }
        Set<String> set = new LinkedHashSet<>();
        //Set<String> set = new TreeSet<>();
        data.stream().forEach(m -> set.add((String)m.get(ROW_KEY_NAME)));
        System.out.println("======================count: " + count);
        System.out.println("======================" + set.size());
        consoleJson(set);
    }

    @Test
    public void page() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(1);
        consoleJson(getBean().nextPage(query));
    }

    @Test
    public void count() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(111);
        consoleJson("======================" + getBean().count(query));
    }

    // -------------------------------------------------------------------------------
    @Test
    public void prefix() {
        consoleJson(getBean().prefix("2018", PAGE_SIZE));
    }

    @Test
    public void regexp() {
        consoleJson(getBean().regexp("^20[0-1]{1}8.*1$", 20));
    }

    @Test
    @Ignore
    public void delete() {
        consoleJson(getBean().delete(Lists.newArrayList("20171231050359","20170922213037" )));
    }

}
