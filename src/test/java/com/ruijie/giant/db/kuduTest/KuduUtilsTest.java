package com.ruijie.giant.db.kuduTest;

import com.ruijie.giant.db.kudu.utils.KuduTemplate;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by admin on 2018/1/18.
 */
public class KuduUtilsTest {
    public static void main(String args[]) throws NoSuchMethodException {
        //testCreateTable();
        /*
        testInsert();
        testFindOne();

        testUpdate();
        testFindOne();

        testDelete();
        testFindOne();

        //long tm = testInsertBatch();
        testFindAll();                    //OK
        */
    }



    public static void testFindAll() {
        long start = System.currentTimeMillis();
        List<MetricsTable> ret = KuduTemplate.getInstance().findAll(MetricsTable.class);

        for (MetricsTable mt : ret) {
            System.out.println(mt.toString());
        }
        long end = System.currentTimeMillis();
        System.out.println("kudu table find record "+ret.size() +"  use " + (end - start));

    }

    public static void testFindOne() {
        MetricsTable mt = new MetricsTable();
        mt.setHost("localhost");
        mt.setMetric("mem.free.103");
        mt.setTimestamp(1516278488);
        long start = System.currentTimeMillis();
        MetricsTable ret = (MetricsTable) KuduTemplate.getInstance().findOne(mt);
        long end = System.currentTimeMillis();
        if (ret != null) {
            System.out.println("kudu table find one record "+ret.toString());
        }
    }

    public static void testDelete() {
        MetricsTable mt = new MetricsTable();
        mt.setHost("localhost");
        mt.setMetric("mem.free.103");
        mt.setTimestamp(1516278488);
        KuduTemplate.getInstance().delete(mt);

    }

    public static void testUpdate() {
        MetricsTable mt = new MetricsTable();
        mt.setHost("localhost");
        mt.setMetric("mem.free.103");
        mt.setTimestamp(1516278488);
        mt.setValue(4567);
        KuduTemplate.getInstance().update(mt);
    }

    public static void testInsert() {
        MetricsTable mt = new MetricsTable();
        mt.setHost("localhost");
        mt.setMetric("mem.free.103");
        mt.setTimestamp(1516278488);
        mt.setValue(9527);
        KuduTemplate.getInstance().insert(mt);
    }

    public static long testInsertBatch() {

        List<MetricsTable> list = new ArrayList<MetricsTable>();

        for (int i=0; i<1000000; i++) {
            MetricsTable mt = new MetricsTable();
            mt.setHost("localhost");
            mt.setMetric("mem.free.59"+i);
            mt.setTimestamp((int)(System.currentTimeMillis()/1000));
            mt.setValue(i);
            list.add(mt);
        }
        long start = System.currentTimeMillis();
        KuduTemplate.getInstance().insertBatch(list);
        long end = System.currentTimeMillis();
        return end - start;
    }

    /*
    public static void testCreateTable() {
        List<ColumnMetaData> list = new ArrayList<ColumnMetaData>();
        list.add(new ColumnMetaData("hello", Type.STRING, true, ColumnSchema.Encoding.DICT_ENCODING, 0));
        list.add(new ColumnMetaData("world", Type.STRING, true, ColumnSchema.Encoding.DICT_ENCODING, 0));

        KuduTemplate.createTable("helloworldtable", list);

    }*/
}


