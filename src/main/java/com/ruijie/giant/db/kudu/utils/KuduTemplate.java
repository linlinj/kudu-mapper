package com.ruijie.giant.db.kudu.utils;

import com.ruijie.giant.db.kudu.exception.KuduOperationException;
import com.ruijie.giant.db.kudu.metadata.FieldColumnMapper;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * KuduTemplate help java client operate kudu with ORM
 *
 * operation include {findOne, findAll, insert, insertBatch, update, delete}
 *
 * Created by jianglinlin on 2018/1/18.
 *
 */
public class KuduTemplate {
    //TODO address read from config
    private static final String masterAddresses  = "172.24.33.11:7051,172.24.33.12:7051,172.24.33.13:7051";

    //kuduClient can only inited once
    private static final KuduClient kuduClient = new KuduClient.KuduClientBuilder(masterAddresses).build();

    //Cache all table on Kudu
    private static final Map<String, KuduTable> tableList = new ConcurrentHashMap<String, KuduTable>();

    //Cache all table's column
    private static final Map<String, Map<String, FieldColumnMapper>> tableColumnMap
            = new ConcurrentHashMap<String, Map<String, FieldColumnMapper>>();

    //Cache JaveBean field mapped completely to kudu column
    private static final Map<String, Boolean> fieldColumnMapper = new ConcurrentHashMap<String,Boolean>();

    static {
        try {
            List<String> tables = kuduClient.getTablesList().getTablesList();
            for (String tableName:tables) {
                KuduTable table = kuduClient.openTable(tableName);
                if (table != null) {
                    tableList.put(tableName, table);
                    buildTableColumnCache(table);
                }
            }
        } catch (KuduException e) {
            throw new KuduOperationException(e);
        }
    }

    //SingleTon
    private KuduTemplate(){}
    private static class KuduTemplateSingleTon{
        private static volatile KuduTemplate instance = new KuduTemplate();
    }
    public static KuduTemplate getInstance() {
        return KuduTemplateSingleTon.instance;
    }

    /**
     * Create Kudu Table
     *
     * @param //tableName
     * @param /columns {@code KuduTemplate.ColumnMetaData}
     *
     * @return true if create success , false the table is exist
     */
    /*
    public static boolean createTable(String tableName, List<ColumnMetaData> columns) {

        try {
            if (kuduClient.tableExists(tableName)) {
                throw new IllegalArgumentException("Create table fail, \""+ tableName +"\" is already exist");
            }
        } catch (KuduException e) {
            throw new KuduOperationException(e);
        }

        List<ColumnSchema> cols = new ArrayList<ColumnSchema>();
        List<String> rangeKeys = new ArrayList<String>();
        for (ColumnMetaData col: columns) {
            ColumnSchema.ColumnSchemaBuilder columnSchema
                    = new ColumnSchema.ColumnSchemaBuilder(col.getName(), col.getType());
            if (col.getKey()) {
                columnSchema.key(true);
                rangeKeys.add(col.getName());
            }
            columnSchema.encoding(col.getEncoding());
            cols.add(columnSchema.build());
        }

        try {
            kuduClient.createTable(tableName, new Schema(cols),
                    new CreateTableOptions().setRangePartitionColumns(rangeKeys));
        } catch (KuduException e) {
            throw new KuduOperationException(e);
        }

        return true;
    }


    /**
     * find one record by primary key
     *
     * @param  query the specify entity query which is annotated by {@code KuduTableAnnotation}
     *
     * @return  entity or null
     *
     */
    public  <T> T findOne(T query) {
        KuduTable table = getTable(ReflectUtils.getTableNameFromClass(query.getClass()));

        checkFieldColumnMapper(table.getName(), query.getClass());

        Map<String, FieldColumnMapper> tableColumns = getTableColumns(table.getName());

        KuduScanner.KuduScannerBuilder kuduScannerBuilder = kuduClient.newScannerBuilder(table);
        for (String key: tableColumns.keySet()) {
            FieldColumnMapper mapper = tableColumns.get(key);
            try {
                // Generating predicate by primary key
                if (mapper.getColumnIsKey()){
                    KuduPredicate.ComparisonOp op = KuduPredicate.ComparisonOp.EQUAL;
                    kuduScannerBuilder.addPredicate(queryRowDataGenerator(table, mapper, op, query));
                }
            } catch (Exception e) {
                throw new KuduOperationException(e);
            }
        }

        KuduScanner scanner = kuduScannerBuilder.build();
        List<T> list = find(scanner, table.getName(), query.getClass());
        if (list.size()>0) {
            return list.get(0);
        }

        return null;
    }

    public  <T> T findTest(T query) {
        KuduTable table = getTable(ReflectUtils.getTableNameFromClass(query.getClass()));

        checkFieldColumnMapper(table.getName(), query.getClass());

        Map<String, FieldColumnMapper> tableColumns = getTableColumns(table.getName());
        List<KuduPredicate> kp = new ArrayList<KuduPredicate>();

        KuduScanner.KuduScannerBuilder kuduScannerBuilder = kuduClient.newScannerBuilder(table);
        for (String key: tableColumns.keySet()) {
            FieldColumnMapper mapper = tableColumns.get(key);
            try {
                // Generating predicate by primary key
                if (mapper.getColumnIsKey()){
                    KuduPredicate.ComparisonOp op = KuduPredicate.ComparisonOp.EQUAL;
                    kuduScannerBuilder.addPredicate(queryRowDataGenerator(table, mapper, op, query));
                }
            } catch (Exception e) {
                throw new KuduOperationException(e);
            }
        }

        KuduScanner scanner = kuduScannerBuilder.build();
        List<T> list = find(scanner, table.getName(), query.getClass());
        if (list.size()>0) {
            return list.get(0);
        }

        return null;
    }

    /**
     * find all data from kudu
     *
     * WARNING : don't use is on bigtable
     *
     * @param  cls the specify class which is annotated by {@code KuduTableAnnotation}
     *
     * @return  list dataset of T
     *
     */
    public <T> List<T> findAll(Class<T> cls) {
        KuduTable table = getTable(ReflectUtils.getTableNameFromClass(cls));

        checkFieldColumnMapper(table.getName(), cls);

        KuduScanner scanner = kuduClient.newScannerBuilder(table).build();

        return find(scanner, table.getName(), cls);
    }

    // find execute, called by findOne and findAll
    private <T> List<T> find(KuduScanner scanner, String table, Class cls) {
        Map<String, FieldColumnMapper> tableColumns = getTableColumns(table);
        List<T> ret= new ArrayList<T>();
        while ( scanner.hasMoreRows()) {
            try {
                RowResultIterator results = scanner.nextRows();
                while (results.hasNext()) {
                    RowResult result = results.next();
                    T instance = (T)cls.newInstance();
                    for (String key: tableColumns.keySet()) {
                        javaBeanDataGenerator(instance, tableColumns.get(key), result);
                    }
                    ret.add(instance);
                }
            } catch (KuduException e) {
                throw new KuduOperationException("Kudu exception in method findOne.", e);
            } catch (NoSuchMethodException e) {
                throw new KuduOperationException("NoSuchMethodException:", e);
            } catch (InstantiationException e) {
                throw new KuduOperationException("InstantiationException:", e);
            } catch (IllegalAccessException e) {
                throw new KuduOperationException("IllegalAccessException:", e);
            } catch (InvocationTargetException e) {
                throw new KuduOperationException("InvocationTargetException:", e);
            }
        }
        return ret;
    }

    /**
     * insert data into kudu
     *
     * @param  data, the T must is annotated by {@code KuduTableAnnotation}
     *
     */

    public <T> void insert( T data) {
        if (data == null) {
            throw new IllegalArgumentException("InsertBatch method parameter is empty");
        }
        List list = new ArrayList<T>();
        list.add(data);
        insertBatch(list);
    }

    /**
     * insert batch
     *
     * @param  data, the T must is annotated by {@code KuduTableAnnotation}
     *
     */

    public <T> void insertBatch( List<T> data) {

        if (data == null || data.size() == 0) {
            throw new IllegalArgumentException("InsertBatch method parameter is empty");
        }

        KuduTable table = getTable(ReflectUtils.getTableNameFromClass(data.get(0).getClass()));

        checkFieldColumnMapper(table.getName(), data.get(0).getClass());

        Map<String, FieldColumnMapper> tableColumns = getTableColumns(table.getName());
        KuduSession sess = kuduClient.newSession();
        sess.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        for (int i = 0; i < data.size(); i++) {
            Object obj = data.get(i);
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            boolean isDbFiled = false;
            for (String column: tableColumns.keySet()) {
                FieldColumnMapper mapper = tableColumns.get(column);
                tableRowDataGenerator(row, mapper, obj);
                isDbFiled = true;
            }
            try {
                if (isDbFiled)
                    sess.apply(insert);
            } catch (KuduException e) {
                throw new KuduOperationException("Kudu exception while insert data.", e);
            }

        }
        try {
            sess.flush();
            sess.close();
        } catch (KuduException e) {
            throw new KuduOperationException("Kudu exception while insert data.", e);
        }

    }

    /**
     * update data
     *
     * @param  entity, the T must is annotated by {@code KuduTableAnnotation}
     *
     */
    public <T> void update(T entity) {

        KuduTable table = getTable(ReflectUtils.getTableNameFromClass(entity.getClass()));

        checkFieldColumnMapper(table.getName(), entity.getClass());

        KuduSession sess = kuduClient.newSession();
        Map<String, FieldColumnMapper> tableColumns = getTableColumns(table.getName());
        Update update = table.newUpdate();
        PartialRow row = update.getRow();
        boolean isDbFiled = false;
        for (String column: tableColumns.keySet()) {
            FieldColumnMapper mapper = tableColumns.get(column);
            tableRowDataGenerator(row, mapper, entity);
            isDbFiled = true;
        }

        try {
            if (isDbFiled) {
                sess.apply(update);
            }
            sess.flush();
            sess.close();
        } catch (KuduException e) {
            throw new KuduOperationException("Kudu exception while update data.", e);
        }
    }

    /**
     * delete data
     *
     * @param  entity, the T must is annotated by {@code KuduTableAnnotation}
     *
     */
    public <T> void delete(T entity) {

        KuduTable table = getTable(ReflectUtils.getTableNameFromClass(entity.getClass()));

        checkFieldColumnMapper(table.getName(), entity.getClass());

        KuduSession sess = kuduClient.newSession();
        Map<String, FieldColumnMapper> tableColumns = getTableColumns(table.getName());

        Delete delete = table.newDelete();
        PartialRow row = delete.getRow();
        boolean isDbFiled = false;
        for (String column: tableColumns.keySet()) {
            FieldColumnMapper mapper = tableColumns.get(column);
            // delete operation only need key columns
            if (mapper.getColumnIsKey()) {
                tableRowDataGenerator(row, mapper, entity);
                isDbFiled = true;
            }
        }
        try {
            if (isDbFiled) {
                sess.apply(delete);
            }
            sess.flush();
            sess.close();
        } catch (KuduException e) {
            throw new KuduOperationException("Kudu exception while delete data.", e);
        }
    }

    /**
     * convert column data to javabean, for find operation
     * */
    private static <T> void javaBeanDataGenerator(T instance, FieldColumnMapper mapper, RowResult result)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        Method method = instance.getClass().getMethod(mapper.getFieldSetMethod(), mapper.getFieldType().getJavaClass());
        int index = mapper.getColumnIndex();

        switch (mapper.getColumnType()) {
            case INT8:
                method.invoke(instance, result.getByte(index));
                break;
            case INT16:
                method.invoke(instance, result.getShort(index));
                break;
            case INT32:
                method.invoke(instance, result.getInt(index));
                break;
            case INT64:
            case UNIXTIME_MICROS:
                method.invoke(instance, result.getLong(index));
                break;
            case STRING:
                method.invoke(instance, result.getString(index));
                break;
            case BOOL:
                method.invoke(instance, result.getBoolean(index));
                break;
            case FLOAT:
                method.invoke(instance, result.getFloat(index));
                break;
            case DOUBLE:
                method.invoke(instance, result.getDouble(index));
                break;
            //TODO 考虑支持
            //case BINARY:
            //byte bytes[] = new byte[16];
            //rng.nextBytes(bytes);
            //row.addBinary(rdg.getIndex(), (bytes [])rdg.getIndex());
            //break;
            default:
                throw new UnsupportedOperationException("Unknown kudu field type " + mapper.getColumnType());
        }
    }

    /**
     * convert javabean data to column, for insert , update, delete operation
     * */
    private static void tableRowDataGenerator(PartialRow row, FieldColumnMapper mapper, Object obj) {

        Method method = null;
        Object value = null;
        try {
            method = obj.getClass().getMethod(mapper.getFieldGetMethod());
            value = method.invoke(obj);
        } catch (NoSuchMethodException e) {
            throw new KuduOperationException("NoSuchMethodException: "+mapper.getFieldGetMethod(), e);
        } catch (IllegalAccessException e) {
            throw new KuduOperationException("IllegalAccessException: "+mapper.getFieldGetMethod(), e);
        } catch (InvocationTargetException e) {
            throw new KuduOperationException("InvocationTargetException: "+mapper.getFieldGetMethod(), e);
        }

        int index = mapper.getColumnIndex();

        switch (mapper.getColumnType()) {
            case INT8:
                row.addByte(index, (Byte) value);
                break;
            case INT16:
                row.addShort(index, (Short) value);
                break;
            case INT32:
                row.addInt(index, (Integer) value);
                break;
            case INT64:
            case UNIXTIME_MICROS:
                row.addLong(index, (Long) value);
                break;
            case STRING:
                row.addString(index, (String) value);
                break;
            case BOOL:
                row.addBoolean(index, (Boolean) value);
                break;
            case FLOAT:
                row.addFloat(index, (Float) value);
                break;
            case DOUBLE:
                row.addDouble(index, (Double) value);
                break;
            //TODO 考虑支持
            //case BINARY:
            //byte bytes[] = new byte[16];
            //rng.nextBytes(bytes);
            //row.addBinary(rdg.getIndex(), (bytes [])rdg.getIndex());
            //break;
            default:
                throw new UnsupportedOperationException("Unknown type " + mapper.getColumnType());
        }
    }

    /**
     * Generator predicate
     * */
    private static KuduPredicate queryRowDataGenerator(KuduTable table,
                                                       FieldColumnMapper mapper,
                                                       KuduPredicate.ComparisonOp op,
                                                       Object query) {

        Method method = null;
        Object value = null;
        try {
            method = query.getClass().getMethod(mapper.getFieldGetMethod());
            value = method.invoke(query);
        } catch (NoSuchMethodException e) {
            throw new KuduOperationException("NoSuchMethodException: "+mapper.getFieldGetMethod(), e);
        } catch (IllegalAccessException e) {
            throw new KuduOperationException("IllegalAccessException: "+mapper.getFieldGetMethod(), e);
        } catch (InvocationTargetException e) {
            throw new KuduOperationException("InvocationTargetException: "+mapper.getFieldGetMethod(), e);
        }

        int index = mapper.getColumnIndex();

        ColumnSchema cs = table.getSchema().getColumnByIndex(table.getSchema().getColumnIndex(index));
        KuduPredicate  kuduPredicate;
        switch (mapper.getColumnType()) {
            case INT8:
                byte[] b = {(Byte) value};
                kuduPredicate = KuduPredicate.newComparisonPredicate(cs, op, b);
                break;
            case INT16:
                kuduPredicate = KuduPredicate.newComparisonPredicate(cs, op, (Short) value);
                break;
            case INT32:
                kuduPredicate = KuduPredicate.newComparisonPredicate(cs, op, (Integer) value);
                break;
            case INT64:
            case UNIXTIME_MICROS:
                kuduPredicate = KuduPredicate.newComparisonPredicate(cs, op, (Long) value);
                break;
            case STRING:
                kuduPredicate = KuduPredicate.newComparisonPredicate(cs, op, (String) value);
                break;
            case BOOL:
                kuduPredicate = KuduPredicate.newComparisonPredicate(cs, op, (Boolean) value);
                break;
            case FLOAT:
                kuduPredicate = KuduPredicate.newComparisonPredicate(cs, op, (Float) value);
                break;
            case DOUBLE:
                kuduPredicate = KuduPredicate.newComparisonPredicate(cs, op, (Double) value);
                break;
            //TODO 考虑支持
            //case BINARY:
            //byte bytes[] = new byte[16];
            //rng.nextBytes(bytes);
            //row.addBinary(rdg.getIndex(), (bytes [])rdg.getIndex());
            //break;
            default:
                throw new UnsupportedOperationException("Unknown type " + mapper.getColumnType());
        }
        return kuduPredicate;
    }

    private static KuduTable getTable(String tableName) throws KuduOperationException {
        if (!tableList.containsKey(tableName)) {
            KuduTable table = null;
            try {
                table = kuduClient.openTable(tableName);
                tableList.put(tableName, table);
                buildTableColumnCache(table);
            } catch (KuduException e) {
                throw new KuduOperationException("Kudu client open table \""+ tableName + "\" fail. ", e);
            }
        }
        return tableList.get(tableName);
    }

    private static Map<String, FieldColumnMapper> getTableColumns(String table) {
        return tableColumnMap.get(table);
    }

    /**
     * cache table column metadata
     */
    private static void buildTableColumnCache(KuduTable table) {
        int columnCount = table.getSchema().getColumnCount();
        Map<String, FieldColumnMapper> map = new HashMap<String, FieldColumnMapper>();
        for (int i = 0; i < columnCount; i++) {
            ColumnSchema cs = table.getSchema().getColumnByIndex(i);
            FieldColumnMapper mapper = new FieldColumnMapper();
            mapper.setColumnName(cs.getName());
            mapper.setColumnType(cs.getType());
            mapper.setColumnIsKey(cs.isKey());
            mapper.setColumnIndex(i);
            mapper.setColumnEncoding(cs.getEncoding());
            map.put(cs.getName(), mapper);
            fieldColumnMapper.put(table.getName(), false);
        }
        tableColumnMap.put(table.getName(), map);
    }

    /**
     *  check javabean field and column is mapped complete.
     */
    private static boolean checkFieldColumnMapper(String tableName, Class cls) {
        Boolean mapper = fieldColumnMapper.get(tableName);
        if (mapper == null) {
            return false;
        }
        if (mapper == false) {
            completeTableColumnCache(cls);
        }
        return fieldColumnMapper.get(tableName);
    }

    /**
     * cache javabean field metadata
     */
    private static void completeTableColumnCache(Class cls) {

        String tableName =  ReflectUtils.getTableNameFromClass(cls);
        Map<String, FieldColumnMapper> map;
        if ( (map = tableColumnMap.get(tableName)) == null) {
            return;
        }
        Map<String, FieldColumnMapper> columns = ReflectUtils.getAnnotationedColumns(cls);
        if (columns.size() == 0) {
            return;
        }
        for (String column : columns.keySet()) {
            FieldColumnMapper cached = map.get(column);
            if (cached == null) {
                throw new IllegalArgumentException("Column \""+ column +"\" is not exist in table \"" + tableName+"\"");
            }
            FieldColumnMapper mapper = columns.get(column);
            cached.setFieldType(mapper.getFieldType());
            cached.setFieldName(mapper.getFieldName());
            cached.setFieldGetMethod(mapper.getFieldGetMethod());
            cached.setFieldSetMethod(mapper.getFieldSetMethod());
        }
        fieldColumnMapper.put(tableName, true);
    }
}