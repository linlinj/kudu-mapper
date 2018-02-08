package com.ruijie.giant.db.kuduTest.dao;

import com.ruijie.giant.db.kudu.KuduBaseDao;
import com.ruijie.giant.db.kuduTest.MetricsTable;

/**
 * Created by admin on 2018/1/23.
 */
public class KuduBaseDaoExtends extends KuduBaseDao<MetricsTable> {
    public KuduBaseDaoExtends() {
        System.out.println("KuduBaseDaoExtends start up");
    }


}
