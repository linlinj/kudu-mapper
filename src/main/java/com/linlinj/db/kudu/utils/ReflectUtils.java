package com.linlinj.db.kudu.utils;

import com.linlinj.db.kudu.annotation.KuduTableAnnotation;
import com.linlinj.db.kudu.metadata.FieldColumnMapper;
import com.linlinj.db.kudu.annotation.KuduColumnAnnotation;
import com.linlinj.db.kudu.metadata.JavaTypeMapper;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jianglinlin on 2018/1/20.
 */
public class ReflectUtils {

    public static <T> String getTableNameFromClass(Class<T> cls) {
        KuduTableAnnotation annotation = (KuduTableAnnotation) cls.getAnnotation(KuduTableAnnotation.class);
        if (annotation == null) {
            throw new IllegalArgumentException(cls.getName() + " has no KuduTableAnnotation.class annotation");
        }
        return annotation.tableName();
    }

    public static <T> Map<String, FieldColumnMapper> getAnnotationedColumns(Class<T> cls){
        if(cls == null) {
            return null;
        } else {
            Map<String, FieldColumnMapper> map;
            if ((map = getAnnotationedColumns(cls.getSuperclass())) == null) {
                map = new HashMap<String, FieldColumnMapper>();
            }
            Field[] fields = cls.getDeclaredFields();
            for (int i = 0; i < fields.length; i++) {
                KuduColumnAnnotation columnAnnotation = fields[i].getAnnotation(KuduColumnAnnotation.class);
                if (columnAnnotation != null) {
                    if (columnAnnotation.columnName() == null || "".equals(columnAnnotation.columnName())) {
                        continue;
                    }
                    FieldColumnMapper mapper = new FieldColumnMapper();
                    mapper.setFieldName(fields[i].getName());
                    mapper.setFieldType(JavaTypeMapper.fromTypeName(fields[i].getType().getName()));
                    //TODO 对boolean型的字段进行优化
                    String upper = fields[i].getName().substring(0, 1).toUpperCase();
                    String end = fields[i].getName().substring(1);
                    mapper.setFieldSetMethod("set"+upper+end);
                    mapper.setFieldGetMethod("get"+upper+end);
                    map.put(columnAnnotation.columnName(), mapper);
                }
            }

            return map;
        }
    }
}
