package com.linlinj.db.kudu.metadata;

/**
 * Created by jianglinlin on 2018/1/20.
 */
public enum JavaTypeMapper {

    BOOLEAN_TYPE("boolean", boolean.class),                             //boolean 原始类型
    BOOLEAN_CLASS("java.lang.Boolean", java.lang.Boolean.class),        //Boolean 对象类型(原始类型的包装类型)
    CHAR_TYPE("char", char.class),                                      //char 原始类型
    CHAR_CLASS("java.lang.Char", java.lang.Character.class),            //Char 对象类型
    BYTE_TYPE("byte", byte.class),                                      //byte 原始类型
    BYTE_CLASS("java.lang.Byte", java.lang.Byte.class),                 //Byte 对象类型
    SHORT_TYPE("short", short.class),                                   //short 原始类型
    SHORT_CLASS("java.lang.Short", java.lang.Short.class),              //Short 对象类型
    INT_TYPE("int", int.class),                                         //int 原始类型      int.class
    INTEGER_CLASS("java.lang.Integer", java.lang.Integer.class),        //Integer 对象类型  Integer.class
    FLOAT_TYPE("float", float.class),                                   //float 原始类型
    FLOAT_CLASS("java.lang.Float", java.lang.Float.class),              //Float 对象类型.
    LONG_TYPE("long", long.class),                                      //long 原始类型
    LONG_CLASS("java.lang.Long", java.lang.Long.class),                 //Long 对象类型
    DOUBLE_TYPE("double", double.class),                                //double 原始类型
    DOUBLE_CLASS("java.lang.Double", java.lang.Double.class),           //Double 对象类型
    STRING_CLASS("java.lang.String", java.lang.String.class),           //String 对象类型
    VOID_TYPE("void", void.class),                                      //void 原始类型
    VOID_CLASS("java.lang.Void", java.lang.Void.class);                 //Void 对象类型

    private String javaType ;
    private Class  javaClass;

    JavaTypeMapper(String javaType, Class  javaClass) {
        this.javaType = javaType;
        this.javaClass = javaClass;
    }

    public String getJavaType() {
        return javaType;
    }

    public Class getJavaClass() {
        return javaClass;
    }

    public static JavaTypeMapper fromTypeName(String javaType) {
        for (JavaTypeMapper type : JavaTypeMapper.values()) {
            if (type.getJavaType().equals(javaType)) {
                return type;
            }
        }
        return null;
    }
}
