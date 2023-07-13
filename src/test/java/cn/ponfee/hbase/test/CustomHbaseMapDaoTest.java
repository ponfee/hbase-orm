package cn.ponfee.hbase.test;

import cn.ponfee.commons.date.Dates;
import cn.ponfee.commons.reflect.BeanMaps;
import cn.ponfee.hbase.HbaseBatchDao;
import cn.ponfee.hbase.SpringBaseTest;
import cn.ponfee.hbase.test.CustomHbaseMapDaoTest.CustomHbaseMapDao;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class CustomHbaseMapDaoTest extends SpringBaseTest<CustomHbaseMapDao> {

    @Repository("customHbaseMapDao")
    public static class CustomHbaseMapDao extends HbaseBatchDao<CustomHbaseMap, String> {
    }

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
        List<CustomHbaseMap> batch = new ArrayList<>();
        for (int i = 0; i < 1; i++) {
            CustomHbaseBean entity = new CustomHbaseBean();
            entity.setFirstName("fu");
            entity.setLastName("ponfee");
            entity.setAge(ThreadLocalRandom.current().nextInt(60) + 10);
            entity.setBirthday(Dates.random(Dates.ofTimeMillis(0), Dates.toDate("20000101", "yyyyMMdd")));
            entity.buildRowKey();

            CustomHbaseMap beanMap= new CustomHbaseMap();
            beanMap.putAll(BeanMaps.CGLIB.toMap(entity));

            beanMap.put("empty_string", "");
            beanMap.put("blank_string", "   ");
            beanMap.put("null_string", null);
            batch.add(beanMap);
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
    //@Ignore
    public void delete() {
        consoleJson(getBean().get("fu_ponfee_19760624"));
        consoleJson(getBean().delete(Lists.newArrayList("fu_ponfee_19760624")));
        consoleJson(getBean().get("fu_ponfee_19760624"));
    }

}
