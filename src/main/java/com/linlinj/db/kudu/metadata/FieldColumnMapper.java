package com.linlinj.db.kudu.metadata;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;

/**
 * Created by jianglinlin on 2018/1/20.
 */
public class FieldColumnMapper {
    // column attribute
    private volatile int columnIndex;
    private volatile  Type columnType;
    private volatile  Boolean columnIsKey;
    private volatile  String columnName;
    private volatile  ColumnSchema.Encoding columnEncoding;

    //field attribute
    private volatile String fieldName;
    private volatile JavaTypeMapper fieldType;
    private volatile String fieldSetMethod;
    private volatile String fieldGetMethod;

    public int getColumnIndex() {
        return columnIndex;
    }

    public Type getColumnType() {
        return columnType;
    }

    public Boolean getColumnIsKey() {
        return columnIsKey;
    }

    public void setColumnIsKey(Boolean columnIsKey) {
        this.columnIsKey = columnIsKey;
    }

    public String getColumnName() {
        return columnName;
    }


    public String getFieldName() {
        return fieldName;
    }

    public JavaTypeMapper getFieldType() {
        return fieldType;
    }

    public String getFieldSetMethod() {
        return fieldSetMethod;
    }

    public String getFieldGetMethod() {
        return fieldGetMethod;
    }

    public void setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    public void setColumnType(Type columnType) {
        this.columnType = columnType;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public ColumnSchema.Encoding getColumnEncoding() {
        return columnEncoding;
    }

    public void setColumnEncoding(ColumnSchema.Encoding columnEncoding) {
        this.columnEncoding = columnEncoding;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public void setFieldType(JavaTypeMapper fieldType) {
        this.fieldType = fieldType;
    }

    public void setFieldSetMethod(String fieldSetMethod) {
        this.fieldSetMethod = fieldSetMethod;
    }

    public void setFieldGetMethod(String fieldGetMethod) {
        this.fieldGetMethod = fieldGetMethod;
    }
}
