package code.ponfee.hbase.test;


import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

import code.ponfee.commons.model.PageSortOrder;
import code.ponfee.hbase.BaseTest;
import code.ponfee.hbase.model.PageQueryBuilder;
import code.ponfee.hbase.other.BasOrderInfo;
import code.ponfee.hbase.other.BasOrderInfoDao;

public class BasOrderInfoTest extends BaseTest<BasOrderInfoDao> {
    
    private static final int PAGE_SIZE = 20;

    @Test
    public void tableExists() {
        System.out.println(getBean().tableExists());
    }
    
    @Test
    @Ignore
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
    public void batchPut() {
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
        consoleJson(getBean().range("abc", "def"));
    }

    @Test
    public void find() {
        consoleJson(getBean().find("abc", 20));
    }

    @Test
    public void nextPage() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE, PageSortOrder.DESC);
        consoleJson(getBean().nextPage(query));
    }

    @Test
    public void nextPageAll() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE);
        query.addColumns("cf1", "name");
        List<BasOrderInfo> data = new ArrayList<>();
        int count = 1;
        List<BasOrderInfo> list = (List<BasOrderInfo>) getBean().nextPage(query);
        while (CollectionUtils.isNotEmpty(list) && list.size() == query.pageSize()) {
            count ++;
            data.addAll(list);
            consoleJson(list);
            consoleJson((String) query.nextPageStartRow(list).getRowKey());
            query.startRowKey((String) query.nextPageStartRow(list).getRowKey());
            list = (List<BasOrderInfo>) getBean().nextPage(query);
        }
        if (CollectionUtils.isNotEmpty(list)) {
            data.addAll(list);
        }
        Set<String> set = new LinkedHashSet<>();
        //Set<String> set = new TreeSet<>();
        data.stream().forEach(m -> set.add((String)m.getRowKey()));
        System.out.println("======================count: " + count);
        System.out.println("======================" + set.size());
        consoleJson(set);
    }

    @Test
    public void nextPageAllDESC() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE, PageSortOrder.DESC);
        query.addColumns("cf1", "name" );
        List<BasOrderInfo> data = new ArrayList<>();
        int count = 1;
        List<BasOrderInfo> list = (List<BasOrderInfo>) getBean().nextPage(query);
        while (CollectionUtils.isNotEmpty(list) && list.size() == query.pageSize()) {
            count ++;
            data.addAll(list);
            consoleJson(list);
            consoleJson((String) query.nextPageStartRow(list).getRowKey());
            query.startRowKey((String) query.nextPageStartRow(list).getRowKey());
            list = (List<BasOrderInfo>) getBean().nextPage(query);
        }
        if (CollectionUtils.isNotEmpty(list)) {
            data.addAll(list);
        }
        Set<String> set = new LinkedHashSet<>();
        //Set<String> set = new TreeSet<>();
        data.stream().forEach(m -> set.add((String)m.getRowKey()));
        System.out.println("======================count: " + count);
        System.out.println("======================" + set.size());
        consoleJson(set);
    }
    
    @Test
    public void previousPage() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE);
        query.startRowKey("name85");
        consoleJson(getBean().previousPage(query));
    }
    
    @Test
    public void previousPageDesc() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE, PageSortOrder.DESC);
        query.startRowKey("name85");
        consoleJson(getBean().previousPage(query));
    }

    @Test
    public void previousPageAll() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE);
        query.addColumns("cf1", "name" );
        query.startRowKey("ponfee2");
        List<BasOrderInfo> data = new ArrayList<>();
        int count = 1;
        List<BasOrderInfo> list = (List<BasOrderInfo>) getBean().previousPage(query);
        while (CollectionUtils.isNotEmpty(list) && list.size() == query.pageSize()) {
            count ++;
            data.addAll(list);
            consoleJson(list);
            consoleJson((String) query.previousPageStartRow(list).getRowKey());
            query.startRowKey((String) query.previousPageStartRow(list).getRowKey());
            list = (List<BasOrderInfo>) getBean().previousPage(query);
        }
        if (CollectionUtils.isNotEmpty(list)) {
            data.addAll(list);
        }
        Set<String> set = new LinkedHashSet<>();
        //Set<String> set = new TreeSet<>();
        data.stream().forEach(m -> set.add((String)m.getRowKey()));
        System.out.println("======================count: " + count);
        System.out.println("======================" + set.size());
        consoleJson(set);
    }

    @Test
    public void previousPageAllDesc() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE, PageSortOrder.DESC);
        query.addColumns("cf1",  "name" );
        query.startRowKey("name10");
        List<BasOrderInfo> data = new ArrayList<>();
        int count = 1;
        List<BasOrderInfo> list = (List<BasOrderInfo>) getBean().previousPage(query);
        while (CollectionUtils.isNotEmpty(list) && list.size() == query.pageSize()) {
            count ++;
            data.addAll(list);
            consoleJson(list);
            consoleJson((String) query.previousPageStartRow(list).getRowKey());
            query.startRowKey((String) query.previousPageStartRow(list).getRowKey());
            list = (List<BasOrderInfo>) getBean().previousPage(query);
        }
        if (CollectionUtils.isNotEmpty(list)) {
            data.addAll(list);
        }
        Set<String> set = new LinkedHashSet<>();
        //Set<String> set = new TreeSet<>();
        data.stream().forEach(m -> set.add((String)m.getRowKey()));
        System.out.println("======================count: " + count);
        System.out.println("======================" + set.size());
        consoleJson(set);
    }

    @Test
    public void count() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE);
        consoleJson("======================" + getBean().count(query));
    }

    // -------------------------------------------------------------------------------
    @Test
    public void prefix() {
        //consoleJson(extendsgetBean()1.prefix("name10", "name10", PAGE_SIZE));
        consoleJson(getBean().prefix("ab_", PAGE_SIZE));
    }

    @Test
    public void regexp() {
        consoleJson(getBean().regexp("^4_.*_20160101_.*1$", 2));
    }

    @Test
    public void delete() {
        consoleJson(getBean().get("abc"));
        consoleJson(getBean().delete(Lists.newArrayList("def")));
        consoleJson(getBean().get("abc"));
    }

}
