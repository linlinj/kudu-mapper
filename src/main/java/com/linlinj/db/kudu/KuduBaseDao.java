package com.linlinj.db.kudu;

import com.linlinj.db.kudu.utils.KuduTemplate;

import java.lang.reflect.ParameterizedType;
import java.util.List;

/**
 * Created by jianglinlin on 2018/1/23.
 *
 * 封装kudu  DAO 基类方法，其他想与kudu进行增、删、改、查的实体，在DAO层都应继承此抽象类
 */
public abstract class KuduBaseDao<ENTITY> {

    protected Class<ENTITY> entityClass;

    private KuduTemplate kuduTemplate = KuduTemplate.getInstance();

    public KuduBaseDao(){
        entityClass = (Class<ENTITY>) ((ParameterizedType) getClass()
                .getGenericSuperclass()).getActualTypeArguments()[0];
    }

    /**
     * findOne - 根据主键查找实体
     *
     * @param query 设置了主键额实体类字段，主键字段都需要设置
     *
     * @return 返回查找到的实体，否则返回null
     * */
    public ENTITY findOne(ENTITY query) {
        return kuduTemplate.findOne(query);
    }

    /**
     * findAll - 获取全表记录
     *           不建议在大表上用此方法
     *
     * @return 返回查找到的所有记录
     * */
    public List<ENTITY> findAll() {
        return kuduTemplate.findAll(entityClass);
    }

    /**
     * query - 根据字段查询
     *
     * @return 返回查找到的所有记录
     * */
    public List<ENTITY> query() {
        return null;
    }

    /**
     * insert - 向kudu表中保存一条数据
     *          插入的实体主键若重复，插入失败
     *
     * @param entity 需要被入库保存的记录
     * */
    public void insert(ENTITY entity) {
        kuduTemplate.insert(entity);
    }

    /**
     * insetBatch - 向kudu表中保存一批数据
     *          插入的实体主键若重复，插入失败
     *
     * @param entitys 需要被入库保存的记录
     * */
    public void insetBatch(List<ENTITY> entitys) {
        kuduTemplate.insertBatch(entitys);
    }

    /**
     * update - 更新kudu表中一条数据
     *
     * @param entity 需要被更新的实体，主键字段都需要被设置
     * */
    public void update(ENTITY entity) {
        kuduTemplate.update(entity);
    }

    /**
     * delete - 删除kudu表一条数据
     *
     * @param entity 需要被删除的实体，主键字段都需要被设置
     * */
    public void delete(ENTITY entity) {
        kuduTemplate.delete(entity);
    }
}
