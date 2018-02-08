package com.ruijie.giant.db.kuduTest.dao;

import com.ruijie.giant.db.kuduTest.MetricsTable;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by admin on 2018/1/23.
 */
public class KuduBaseDaoTest {
    public static void main(String args[]) {
        KuduBaseDaoExtends dao = new KuduBaseDaoExtends();
        //testFindAll(dao);
        /*
        testInsertBatch(dao);
        testFindOne(dao);
        testUpdate(dao);
        testFindOne(dao);
        testDelete(dao);
        testFindOne(dao);
        */
    }

    static void testFindOne(KuduBaseDaoExtends dao) {
        MetricsTable mt = new MetricsTable();
        mt.setHost("localhost");
        mt.setMetric("mem-2free1-333");
        mt.setTimestamp(1516679404);
        long start = System.currentTimeMillis();
        MetricsTable ret = dao.findOne(mt);
        long end = System.currentTimeMillis();
        if (ret != null) {
            System.out.println(ret.toString());
        }
        System.out.println("Test testFindOne use time " + (end-start) );
    }

    static void testInsert(KuduBaseDaoExtends dao) {
        MetricsTable mt = new MetricsTable();
        mt.setHost("localhost");
        mt.setMetric("mem-2free1-333");
        mt.setTimestamp(1516679404);
        mt.setValue(2564);
        dao.insert(mt);
    }

    static void testInsertBatch(KuduBaseDaoExtends dao) {
        List<MetricsTable> list = new ArrayList<MetricsTable>();

        for (int i=0; i<1; i++) {
            MetricsTable mt = new MetricsTable();
            mt.setHost("localhost");
            mt.setMetric("mem-2free1-333");
            mt.setTimestamp(1516679404);
            mt.setValue(i);
            list.add(mt);
        }
        dao.insetBatch(list);
    }

    static void testUpdate(KuduBaseDaoExtends dao) {
        MetricsTable mt = new MetricsTable();
        mt.setHost("localhost");
        mt.setMetric("mem-2free1-333");
        mt.setTimestamp(1516679404);
        mt.setValue(9528);
        long start = System.currentTimeMillis();
        dao.update(mt);
        long end = System.currentTimeMillis();
        System.out.println("Test testUpdate use time " + (end-start) );
    }

    static void testDelete(KuduBaseDaoExtends dao) {
        MetricsTable mt = new MetricsTable();
        mt.setHost("localhost");
        mt.setMetric("mem-2free1-333");
        mt.setTimestamp(1516679404);
        mt.setValue(8695);

        long start = System.currentTimeMillis();
        dao.delete(mt);
        long end = System.currentTimeMillis();
        System.out.println("Test testDelete use time " + (end-start) );
    }

    public static void testFindAll(KuduBaseDaoExtends dao) {
        long start = System.currentTimeMillis();
        List<MetricsTable> ret = dao.findAll();

        for (MetricsTable mt : ret) {
            System.out.println(mt.toString());
        }
        long end = System.currentTimeMillis();
        System.out.println("kudu table find record "+ret.size() +"  use " + (end - start));

    }
}
