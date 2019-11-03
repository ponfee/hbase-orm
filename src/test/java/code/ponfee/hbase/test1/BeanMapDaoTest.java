package code.ponfee.hbase.test1;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;
import org.springframework.stereotype.Repository;

import com.google.common.collect.Lists;

import code.ponfee.commons.reflect.BeanMaps;
import code.ponfee.commons.util.Dates;
import code.ponfee.hbase.HbaseBatchDao;
import code.ponfee.hbase.SpringBaseTest;
import code.ponfee.hbase.test1.BeanMapDaoTest.BeanMapDao;

public class BeanMapDaoTest extends SpringBaseTest<BeanMapDao> {

    @Repository("beanMapDao")
    public static class BeanMapDao extends HbaseBatchDao<BeanMap, String> {
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
        List<BeanMap> batch = new ArrayList<>();
        for (int i = 0; i < 1; i++) {
            BeanEntity entity = new BeanEntity();
            entity.setFirstName("fu");
            entity.setLastName("ponfee");
            entity.setAge(ThreadLocalRandom.current().nextInt(60) + 10);
            entity.setBirthday(Dates.random(Dates.ofMillis(0), Dates.toDate("20000101", "yyyyMMdd")));
            entity.buildRowKey();

            BeanMap beanMap= new BeanMap();
            beanMap.putAll(BeanMaps.CGLIB.toMap(entity));
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
        consoleJson(getBean().get("fu_ponfee_19840415"));
        consoleJson(getBean().delete(Lists.newArrayList("fu_ponfee_19840415", "fu_ponfee_20110531")));
        consoleJson(getBean().get("fu_ponfee_19840415"));
    }

}
