package com.ruijie.giant.db.kuduTest;

import com.ruijie.giant.db.kudu.annotation.KuduColumnAnnotation;

/**
 * Created by admin on 2018/1/19.
 */
public class BaseEntity {
    @KuduColumnAnnotation(columnName = "id")
    private Integer id;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }
}
