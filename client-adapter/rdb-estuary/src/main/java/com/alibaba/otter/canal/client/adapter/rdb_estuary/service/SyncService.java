package com.alibaba.otter.canal.client.adapter.rdb_estuary.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.client.adapter.rdb_estuary.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.rdb_estuary.config.MappingConfig.DbMapping;
import com.alibaba.otter.canal.client.adapter.rdb_estuary.config.SchemaItem;
import com.alibaba.otter.canal.client.adapter.rdb_estuary.support.BatchExecutor;
import com.alibaba.otter.canal.client.adapter.rdb_estuary.support.SingleDml;
import com.alibaba.otter.canal.client.adapter.rdb_estuary.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.Util;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * @author YangWeiDong
 * @date 2022年05月11日 15:52
 */
public class SyncService {

    private static final Logger logger  = LoggerFactory.getLogger(SyncService.class);

    // 目标库表字段类型缓存: instance.schema.table -> <columnName, jdbcType>
    private Map<String, Map<String, Integer>> columnsTypeCache;

    private int                               threads = 3;
    private boolean                           skipDupException;

    private List<SyncItem>[]                  dmlsPartition;
    private BatchExecutor[]                   batchExecutors;
    private ExecutorService[]                 executorThreads;

    public List<SyncItem>[] getDmlsPartition() {
        return dmlsPartition;
    }

    public Map<String, Map<String, Integer>> getColumnsTypeCache() {
        return columnsTypeCache;
    }

    public SyncService(DataSource dataSource, Integer threads, boolean skipDupException){
        this(dataSource, threads, new ConcurrentHashMap<>(), skipDupException);
    }

    public SyncService(DataSource dataSource, Integer threads, Map<String, Map<String, Integer>> columnsTypeCache,
                          boolean skipDupException){
        this.columnsTypeCache = columnsTypeCache;
        this.skipDupException = skipDupException;
        try {
            if (threads != null) {
                this.threads = threads;
            }
            this.dmlsPartition = new List[this.threads];
            this.batchExecutors = new BatchExecutor[this.threads];
            this.executorThreads = new ExecutorService[this.threads];
            for (int i = 0; i < this.threads; i++) {
                dmlsPartition[i] = new ArrayList<>();
                batchExecutors[i] = new BatchExecutor(dataSource);
                executorThreads[i] = Executors.newSingleThreadExecutor();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 批量同步回调
     *
     * @param dmls 批量 DML
     * @param function 回调方法
     */
    public void sync(List<Dml> dmls, Function<Dml, Boolean> function) {
        try {
            boolean toExecute = false;
            for (Dml dml : dmls) {
                if (!toExecute) {
                    toExecute = function.apply(dml);
                } else {
                    function.apply(dml);
                }
            }
            if (toExecute) {
                List<Future<Boolean>> futures = new ArrayList<>();
                for (int i = 0; i < threads; i++) {
                    int j = i;
                    if (dmlsPartition[j].isEmpty()) {
                        // bypass
                        continue;
                    }

                    futures.add(executorThreads[i].submit(() -> {
                        try {
                            dmlsPartition[j].forEach(syncItem -> sync(batchExecutors[j],
                                    syncItem.config,
                                    syncItem.singleDml));
                            dmlsPartition[j].clear();
                            batchExecutors[j].commit();
                            return true;
                        } catch (Throwable e) {
                            dmlsPartition[j].clear();
                            batchExecutors[j].rollback();
                            throw new RuntimeException(e);
                        }
                    }));
                }

                RuntimeException exception = null;
                for (Future<Boolean> future : futures) {
                    try {
                        future.get();
                    } catch (Exception e) {
                        exception = new RuntimeException(e);
                    }
                }
                if (exception != null) {
                    throw exception;
                }
            }
        } finally {
            for (BatchExecutor batchExecutor : batchExecutors) {
                if (batchExecutor != null) {
                    batchExecutor.close();
                }
            }
        }
    }

    /**
     * 批量同步
     *
     * @param mappingConfig 配置集合
     * @param dmls 批量 DML
     */
    public void sync(Map<String, Map<String, MappingConfig>> mappingConfig, List<Dml> dmls, Properties envProperties) {
        sync(dmls, dml -> {
            if (dml.getIsDdl() != null && dml.getIsDdl() && StringUtils.isNotEmpty(dml.getSql())) {
                // DDL
                //columnsTypeCache.remove(dml.getDestination() + "." + dml.getDatabase() + "." + dml.getTable());
                return false;
            } else {
                // DML
                String destination = StringUtils.trimToEmpty(dml.getDestination());
                String groupId = StringUtils.trimToEmpty(dml.getGroupId());
                String database = dml.getDatabase();
                String table = dml.getTable();
                Map<String, MappingConfig> configMap;
                if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
                    configMap = mappingConfig.get(destination + "-" + groupId + "_" + database + "-" + table);
                } else {
                    configMap = mappingConfig.get(destination + "_" + database + "-" + table);
                }

                if (configMap == null) {
                    return false;
                }

                if (configMap.values().isEmpty()) {
                    return false;
                }

                for (MappingConfig config : configMap.values()) {
                    boolean caseInsensitive = false;
                    if (config.getConcurrent()) {
                        List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml, caseInsensitive);
                        for (int i = 0; i < singleDmls.size(); i++) {
                            SingleDml singleDml = singleDmls.get(i);
                            int hash = pkHash(config.getDbMapping(), i);
                            SyncItem syncItem = new SyncItem(config, singleDml);
                            dmlsPartition[hash].add(syncItem);
                        }
                    } else {
                        int hash = 0;
                        List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml, caseInsensitive);
                        singleDmls.forEach(singleDml -> {
                            SyncItem syncItem = new SyncItem(config, singleDml);
                            dmlsPartition[hash].add(syncItem);
                        });
                    }
                }
                return true;
            }
        }   );
    }

    /**
     * 单条 dml 同步
     *
     * @param batchExecutor 批量事务执行器
     * @param config 对应配置对象
     * @param dml DML
     */
    public void sync(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) {
        if (config != null) {
            try {
                String type = dml.getType();
                if (type != null && type.equalsIgnoreCase("INSERT")) {
                    insert(batchExecutor, config, dml);
                } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                    update(batchExecutor, config, dml);
                } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                    delete(batchExecutor, config, dml);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("DML: {}", JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void insert(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) throws SQLException {
        Map<String, Object> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        SchemaItem schemaItem = config.getDbMapping().getSchemaItem();
        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getDataSource(), config);

        if (schemaItem.isGroup()){//有group by
            boolean execTruncate = false;//是否执行清空目标表

            for (SchemaItem.TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                if (tableItem.getTableName().equalsIgnoreCase(dml.getTable())
                        && tableItem.getRelationGroupFieldItems().isEmpty()) {
                    execTruncate = true;
                    break;
                }
            }

            if (execTruncate){
                truncateAndInsert(config, ds, batchExecutor, ctype);
            } else {
                for (SchemaItem.TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                    if (tableItem.getTableName().equalsIgnoreCase(dml.getTable())) {
                        syncByGroupField(config, ds, batchExecutor, ctype, tableItem, dml.getData(), null);
                    }
                }
            }
        } else {//无group by
            if (schemaItem.getAliasTableItems().size() == 1 && schemaItem.isAllFieldsSimple()){//单表&所有字段都是简单字段
                singleTableSimpleFieldInsert(config, batchExecutor, ctype, dml);
            } else {
                if (schemaItem.getMainTable().getTableName().equalsIgnoreCase(dml.getTable())) {//主表
                    mainTableInsert(config, ds, batchExecutor, ctype, dml);
                }
                for (SchemaItem.TableItem tableItem : schemaItem.getAliasTableItems().values()) {//从表
                    if (tableItem.isMain()) {
                        continue;
                    }
                    if (!tableItem.getTableName().equalsIgnoreCase(dml.getTable())) {
                        continue;
                    }
                    syncByLeftOnField(config, ds, batchExecutor, ctype, tableItem, dml.getData(), null);
                }
            }
        }
    }

    private void update(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) throws SQLException {
        Map<String, Object> data = dml.getData();
        Map<String, Object> old = dml.getOld();
        if (data == null || data.isEmpty() || old == null || old.isEmpty()) {
            return;
        }

        SchemaItem schemaItem = config.getDbMapping().getSchemaItem();
        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getDataSource(), config);

        if (schemaItem.isGroup()) {//有group by
            boolean execTruncate = false;//是否执行清空目标表
            boolean fieldNoReference  = true;//被更新的字段是否在查询字段/group字段中未被引用
            for (SchemaItem.TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                if (tableItem.getTableName().equalsIgnoreCase(dml.getTable())) {
                    if (tableItem.getRelationGroupFieldItems().isEmpty()) {
                        execTruncate = true;
                    }

                    out: for (String field : dml.getOld().keySet()) {
                        for (SchemaItem.FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                            for (SchemaItem.ColumnItem item : fieldItem.getColumnItems()) {
                                if (item.getOwner().equalsIgnoreCase(tableItem.getAlias()) && item.getColumnName().equalsIgnoreCase(field)){
                                    fieldNoReference = false;
                                    break out;
                                }
                            }
                        }
                        for (SchemaItem.FieldItem fieldItem : tableItem.getRelationTableFields().keySet()) {
                            if (fieldItem.getColumn().getColumnName().equalsIgnoreCase(field)){
                                fieldNoReference = false;
                                break out;
                            }
                        }
                        for (SchemaItem.FieldItem fieldItem : tableItem.getAnotherRelationTableFields().keySet()) {
                            if (fieldItem.getColumn().getColumnName().equalsIgnoreCase(field)){
                                fieldNoReference = false;
                                break out;
                            }
                        }
                    }
                }
            }

            if (fieldNoReference){//更新的字段在查询字段(group字段)中未被引用
                return;
            }
            if (execTruncate){
                truncateAndInsert(config, ds, batchExecutor, ctype);
            } else {
                for (SchemaItem.TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                    if (tableItem.getTableName().equalsIgnoreCase(dml.getTable())) {
                        syncByGroupField(config, ds, batchExecutor, ctype, tableItem, dml.getData(), null);
                        syncByGroupField(config, ds, batchExecutor, ctype, tableItem, dml.getData(), dml.getOld());
                    }
                }
            }

        } else {//无group by
            if (schemaItem.getAliasTableItems().size() == 1 && schemaItem.isAllFieldsSimple()){//单表&&所有字段都是简单字段
                singleTableSimpleFieldUpdate(config, batchExecutor, ctype, dml);
            } else {
                boolean mainTableFieldNoReference = true;//更新字段在主表查询字段中无引用
                out: for (SchemaItem.FieldItem fieldItem : schemaItem.getMainTable().getRelationSelectFieldItems()) {
                    for (SchemaItem.ColumnItem columnItem : fieldItem.getColumnItems()) {
                        if (columnItem.getOwner().equalsIgnoreCase(schemaItem.getMainTable().getAlias())
                                && dml.getOld().containsKey(columnItem.getColumnName())){
                            mainTableFieldNoReference = false;
                            break out;
                        }
                    }
                }
                if (schemaItem.getMainTable().getTableName().equalsIgnoreCase(dml.getTable()) && !mainTableFieldNoReference) {//主表&&更新字段在查询字段中有引用
                    mainTableUpdate(config, ds, batchExecutor, ctype, dml);
                }

                for (SchemaItem.TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                    if (tableItem.isMain()) {
                        continue;
                    }
                    if (!tableItem.getTableName().equalsIgnoreCase(dml.getTable())) {
                        continue;
                    }

                    boolean fieldNoReference = true;//更新的字段在查询字段无引用
                    out: for (SchemaItem.FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                        for (SchemaItem.ColumnItem columnItem : fieldItem.getColumnItems()) {
                            if (columnItem.getOwner().equalsIgnoreCase(tableItem.getAlias())
                                    && dml.getOld().containsKey(columnItem.getColumnName())){
                                fieldNoReference = false;
                                break out;
                            }
                        }
                    }

                    boolean onFieldNoReference = true;//更新的字段在关联字段中无引用
                    for (SchemaItem.FieldItem fieldItem : tableItem.getRelationTableFields().keySet()) {
                        if (dml.getOld().containsKey(fieldItem.getColumn().getColumnName())){
                            onFieldNoReference = false;
                            break;
                        }
                    }

                    if (fieldNoReference && onFieldNoReference){//查询字段&关联字段未更新
                        continue;
                    }
                    if (onFieldNoReference){//关联字段&被关联字段未更新
                        syncByLeftOnField(config, ds, batchExecutor, ctype, tableItem, dml.getData(), null);
                    } else {//关联字段&被关联字段被更新
                        syncByLeftOnField(config, ds, batchExecutor, ctype, tableItem, dml.getData(), null);
                        syncByLeftOnField(config, ds, batchExecutor, ctype, tableItem, dml.getData(), dml.getOld());
                    }

                }
            }
        }
    }

    private void delete(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) throws SQLException {
        Map<String, Object> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        SchemaItem schemaItem = config.getDbMapping().getSchemaItem();
        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getDataSource(), config);

        if (schemaItem.isGroup()) {//有group by
            boolean execTruncate = false;//是否执行清空目标表

            for (SchemaItem.TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                if (tableItem.getTableName().equalsIgnoreCase(dml.getTable())
                        && tableItem.getRelationGroupFieldItems().isEmpty()) {
                    execTruncate = true;
                    break;
                }
            }

            if (execTruncate){
                truncateAndInsert(config, ds, batchExecutor, ctype);
            } else {
                for (SchemaItem.TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                    if (tableItem.getTableName().equalsIgnoreCase(dml.getTable())) {
                        syncByGroupField(config, ds, batchExecutor, ctype, tableItem, dml.getData(), null);
                    }
                }
            }
        } else {//无group by
            if (schemaItem.getMainTable().getTableName().equalsIgnoreCase(dml.getTable())) {//主表
                mainTableDelete(config, ds, batchExecutor, ctype, dml);
            }
            for (SchemaItem.TableItem tableItem : schemaItem.getAliasTableItems().values()) {//从表
                if (tableItem.isMain()) {
                    continue;
                }
                if (!tableItem.getTableName().equalsIgnoreCase(dml.getTable())) {
                    continue;
                }

                syncByLeftOnField(config, ds, batchExecutor, ctype, tableItem, dml.getData(), null);
            }
        }
    }

    /**
     * 获取目标字段类型
     *
     * @param dataSource 目标数据源
     * @param config
     * @return 字段sqlType
     */
    private Map<String, Integer> getTargetColumnType(DataSource dataSource, MappingConfig config) {
        String cacheKey = config.getDestination() + "." + config.getDbMapping().getTargetDb() + "." + config.getDbMapping().getTargetTable();
        Map<String, Integer> columnType = columnsTypeCache.get(cacheKey);
        if (columnType == null) {
            synchronized (SyncService.class) {
                columnType = columnsTypeCache.get(cacheKey);
                if (columnType == null) {
                    columnType = SyncUtil.getColumnTypeMap(dataSource
                            , SyncUtil.getDbTableName(config.getDbMapping()));
                    columnsTypeCache.put(cacheKey, columnType);
                }
            }
        }
        return columnType;
    }

    /**
     * 拼接插入目标表的sql
     * @param dbMapping
     * @return
     */
    private String getInsertSql(MappingConfig.DbMapping dbMapping){
        SchemaItem schemaItem = dbMapping.getSchemaItem();

        StringBuilder insertSql = new StringBuilder();
        insertSql.append("INSERT INTO ").append(SyncUtil.getDbTableName(dbMapping)).append(" (");
        for (String column : schemaItem.getSelectFields().keySet()) {
            insertSql.append("`").append(column).append("`").append(",");
        }
        int len = insertSql.length();
        insertSql.delete(len - 1, len).append(") VALUES (");
        for (int i = 0; i < schemaItem.getSelectFields().size(); i++) {
            insertSql.append("?,");
        }
        len = insertSql.length();
        insertSql.delete(len - 1, len).append(")");
        return insertSql.toString();
    }

    private void insertTargetTable(BatchExecutor batchExecutor, String insertSql, List<Map<String, ?>> insertValues) throws SQLException{
        try {
            batchExecutor.execute(insertSql, insertValues);
        } catch (SQLException e) {
            if (skipDupException
                    && (e.getMessage().contains("Duplicate entry") || e.getMessage().startsWith("ORA-00001:"))) {
                // ignore
                // TODO 增加更多关系数据库的主键冲突的错误码
            } else {
                throw e;
            }
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Insert into target table, sql: {}", insertSql);
        }
    }

    /**
     * 执行查询语句 将结果插入目标表
     * @param config
     * @param ds
     * @param batchExecutor
     * @param ctype
     * @param selectSql
     * @param selectParams
     */
    private void searchAndInsertTargetTable(MappingConfig config, DataSource ds, BatchExecutor batchExecutor, Map<String, Integer> ctype
            , String selectSql, List<Object> selectParams){
        SchemaItem schemaItem = config.getDbMapping().getSchemaItem();
        String insertSql = getInsertSql(config.getDbMapping());

        if (logger.isTraceEnabled()) {
            logger.trace("select from source database, sql: {}", selectSql);
        }
        Util.sqlRS(ds, selectSql, selectParams, rs->{
            try {
                while (rs.next()) {
                    List<Map<String, ?>> insertValues = new ArrayList<>();
                    for (String column : schemaItem.getSelectFields().keySet()) {
                        Object value = rs.getObject(column);
                        Integer type = ctype.get(column);
                        if (type == null) {
                            throw new RuntimeException("Target column: " + column + " not matched");
                        }
                        BatchExecutor.setValue(insertValues, type, value);
                    }

                    insertTargetTable(batchExecutor, insertSql, insertValues);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return 0;
        });
    }

    /**
     * 清空目标表然后运行原始sql将结果插入目标表
     * @param config
     * @param ds
     * @param batchExecutor
     * @param ctype
     * @throws SQLException
     */
    private void truncateAndInsert(MappingConfig config, DataSource ds, BatchExecutor batchExecutor, Map<String, Integer> ctype) throws SQLException{
        //清空目标表
        String truncateSql = "DELETE FROM " + SyncUtil.getDbTableName(config.getDbMapping());
        if (config.getDbMapping().getDeleteCondition() != null && !config.getDbMapping().getDeleteCondition().isEmpty()){
            truncateSql = truncateSql + " WHERE " + config.getDbMapping().getDeleteCondition();
        }
        batchExecutor.execute(truncateSql, new ArrayList<>());
        if (logger.isTraceEnabled()) {
            logger.trace("truncate target table, sql: {}", truncateSql);
        }

        //执行原始sql并插入目标表
        searchAndInsertTargetTable(config, ds, batchExecutor, ctype, config.getDbMapping().getSql(), new ArrayList<>());
    }

    private void singleTableSimpleFieldInsert(MappingConfig config, BatchExecutor batchExecutor, Map<String, Integer> ctype, SingleDml dml) throws SQLException{
        String insertSql = getInsertSql(config.getDbMapping());
        List<Map<String, ?>> insertValues = new ArrayList<>();
        for (SchemaItem.FieldItem fieldItem : config.getDbMapping().getSchemaItem().getSelectFields().values()) {
            Object value = dml.getData().get(fieldItem.getColumn().getColumnName());
            Integer type = ctype.get(fieldItem.getFieldName());
            if (type == null) {
                throw new RuntimeException("Target column: " + fieldItem.getFieldName() + " not matched");
            }
            BatchExecutor.setValue(insertValues, type, value);
        }

        insertTargetTable(batchExecutor, insertSql, insertValues);
    }

    private void singleTableSimpleFieldUpdate(MappingConfig config, BatchExecutor batchExecutor, Map<String, Integer> ctype, SingleDml dml) throws SQLException {
        SchemaItem schemaItem = config.getDbMapping().getSchemaItem();

        List<Map<String, ?>> updateParams = new ArrayList<>();
        StringBuilder updateSql = new StringBuilder();
        updateSql.append("UPDATE ").append(SyncUtil.getDbTableName(config.getDbMapping())).append(" SET ");
        for (SchemaItem.FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            updateSql.append(fieldItem.getFieldName());
            Object value = dml.getData().get(fieldItem.getColumn().getColumnName());
            if (value == null){
                updateSql.append(" = null,");
            } else {
                updateSql.append(" = ?,");
                Integer type = ctype.get(fieldItem.getFieldName());
                if (type == null) {
                    throw new RuntimeException("Target column: " + fieldItem.getFieldName() + " not matched");
                }
                BatchExecutor.setValue(updateParams, type, value);
            }
        }
        int len = updateSql.length();
        updateSql.delete(len - 1, len);

        updateSql.append(" WHERE ").append(config.getDbMapping().get_id());
        SchemaItem.FieldItem idField = schemaItem.getSelectFields().get(config.getDbMapping().get_id());
        Object value = dml.getData().get(idField.getColumn().getColumnName());
        if (value == null){
            updateSql.append(" IS NULL ");
        } else {
            updateSql.append(" = ? ");
            Integer type = ctype.get(idField.getFieldName());
            if (type == null) {
                throw new RuntimeException("Target column: " + idField.getFieldName() + " not matched");
            }
            BatchExecutor.setValue(updateParams, type, value);
        }
        if (config.getDbMapping().getDeleteCondition() != null && !config.getDbMapping().getDeleteCondition().isEmpty()){
            updateSql.append(" AND ").append(config.getDbMapping().getDeleteCondition());
        }
        batchExecutor.execute(updateSql.toString(), updateParams);
        if (logger.isTraceEnabled()) {
            logger.trace("update target table, sql: {}", updateSql.toString());
        }
    }

    private void mainTableInsert(MappingConfig config, DataSource ds, BatchExecutor batchExecutor, Map<String, Integer> ctype, SingleDml dml) throws SQLException{
        DbMapping dbMapping = config.getDbMapping();

        List<Object> selectParams = new ArrayList<>();
        StringBuilder selectWhere = new StringBuilder();
        SchemaItem.FieldItem idField = dbMapping.getSchemaItem().getSelectFields().get(dbMapping.get_id());
        selectWhere.append(idField.getColumn().getOwner()).append(".").append(idField.getColumn().getColumnName());
        Object value = dml.getData().get(idField.getColumn().getColumnName());
        if (value == null){
            selectWhere.append(" IS NULL ");
        } else {
            selectWhere.append(" = ? ");
            selectParams.add(value);
        }
        String selectSql = SyncUtil.injectionWhere(dbMapping.getSql(), selectWhere.toString());
        searchAndInsertTargetTable(config, ds, batchExecutor, ctype, selectSql, selectParams);
    }

    private void mainTableDelete(MappingConfig config, DataSource ds, BatchExecutor batchExecutor, Map<String, Integer> ctype, SingleDml dml) throws SQLException {
        DbMapping dbMapping = config.getDbMapping();

        List<Map<String, ?>> deleteParams = new ArrayList<>();
        StringBuilder deleteSql = new StringBuilder();
        deleteSql.append("DELETE FROM ").append(SyncUtil.getDbTableName(config.getDbMapping())).append(" WHERE ").append(dbMapping.get_id());
        SchemaItem.FieldItem idField = dbMapping.getSchemaItem().getSelectFields().get(dbMapping.get_id());
        Object value = dml.getData().get(idField.getColumn().getColumnName());
        if (value == null){
            deleteSql.append(" IS NULL ");
        } else {
            deleteSql.append(" = ? ");
            Integer type = ctype.get(idField.getFieldName());
            if (type == null) {
                throw new RuntimeException("Target column: " + idField.getFieldName() + " not matched");
            }
            BatchExecutor.setValue(deleteParams, type, value);
        }
        if (config.getDbMapping().getDeleteCondition() != null && !config.getDbMapping().getDeleteCondition().isEmpty()){
            deleteSql.append(" AND ").append(config.getDbMapping().getDeleteCondition());
        }
        batchExecutor.execute(deleteSql.toString(), deleteParams);
        if (logger.isTraceEnabled()) {
            logger.trace("delete target table, sql: {}", deleteSql.toString());
        }
    }

    private void mainTableUpdate(MappingConfig config, DataSource ds, BatchExecutor batchExecutor, Map<String, Integer> ctype, SingleDml dml) throws SQLException {
        mainTableDelete(config, ds, batchExecutor, ctype, dml);
        mainTableInsert(config, ds, batchExecutor, ctype, dml);
    }

    private void syncByGroupField(MappingConfig config, DataSource ds, BatchExecutor batchExecutor, Map<String, Integer> ctype
            , SchemaItem.TableItem tableItem, Map<String, Object> data, Map<String, Object> old) throws SQLException{
        SchemaItem schemaItem = config.getDbMapping().getSchemaItem();
        //删除目标表
        List<Map<String, ?>> deleteParams = new ArrayList<>();
        StringBuilder deleteSql = new StringBuilder();
        deleteSql.append("DELETE FROM ").append(SyncUtil.getDbTableName(config.getDbMapping())).append(" WHERE ");
        if (config.getDbMapping().getDeleteCondition() != null && !config.getDbMapping().getDeleteCondition().isEmpty()){
            deleteSql.append(config.getDbMapping().getDeleteCondition()).append(" AND ");
        }
        for (SchemaItem.FieldItem fieldItem : tableItem.getRelationGroupFieldItems()) {
            SchemaItem.ColumnItem columnItem = fieldItem.getColumn();
            List<SchemaItem.FieldItem> fieldItems = schemaItem.getColumnFields().get(columnItem.getOwner() + "." + columnItem.getColumnName());//group字段相关的查询字段
            for (SchemaItem.FieldItem item : fieldItems) {
                String targetFieldName = item.getFieldName();
                deleteSql.append(targetFieldName);
                Object value = getValue(columnItem.getColumnName(), data, old);
                if (value == null){
                    deleteSql.append(" IS NULL AND ");
                } else {
                    deleteSql.append(" = ? AND ");
                    Integer type = ctype.get(targetFieldName);
                    if (type == null) {
                        throw new RuntimeException("Target column: " + targetFieldName + " not matched");
                    }
                    BatchExecutor.setValue(deleteParams, type, value);
                }
            }
        }
        int len = deleteSql.length();
        deleteSql.delete(len - 4, len);
        batchExecutor.execute(deleteSql.toString(), deleteParams);
        if (logger.isTraceEnabled()) {
            logger.trace("delete target table, sql: {}", deleteSql.toString());
        }

        //拼接查询sql
        List<Object> selectParams = new ArrayList<>();
        StringBuilder selectWhere = new StringBuilder();
        for (SchemaItem.FieldItem fieldItem : tableItem.getRelationGroupFieldItems()) {
            SchemaItem.ColumnItem columnItem = fieldItem.getColumn();
            selectWhere.append(columnItem.getOwner()).append(".").append(columnItem.getColumnName());
            Object value = getValue(columnItem.getColumnName(), data, old);
            if (value == null){
                selectWhere.append(" IS NULL AND ");
            } else {
                selectWhere.append(" = ? AND ");
                String targetFieldName = null;
                for (SchemaItem.FieldItem item : schemaItem.getColumnFields().get(columnItem.getOwner() + "." + columnItem.getColumnName())) {
                    if (item.isMethod() || item.isBinaryOp()){
                        continue;
                    }
                    targetFieldName = item.getFieldName();
                    break;
                }
                Integer type = ctype.get(targetFieldName);
                if (type == null) {
                    throw new RuntimeException("Target column: " + targetFieldName + " not matched");
                }
                selectParams.add(value);
            }
        }
        len = selectWhere.length();
        selectWhere.delete(len - 4, len);
        String selectSql = SyncUtil.injectionWhere(config.getDbMapping().getSql(), selectWhere.toString());

        searchAndInsertTargetTable(config, ds, batchExecutor, ctype, selectSql, selectParams);
    }

    private void syncByLeftOnField(MappingConfig config, DataSource ds, BatchExecutor batchExecutor, Map<String, Integer> ctype
            , SchemaItem.TableItem tableItem, Map<String, Object> data, Map<String, Object> old) throws SQLException{
        SchemaItem schemaItem = config.getDbMapping().getSchemaItem();
        //删除目标表
        List<Map<String, ?>> deleteParams = new ArrayList<>();
        StringBuilder deleteSql = new StringBuilder();
        deleteSql.append("DELETE FROM ").append(SyncUtil.getDbTableName(config.getDbMapping())).append(" WHERE ");
        if (config.getDbMapping().getDeleteCondition() != null && !config.getDbMapping().getDeleteCondition().isEmpty()){
            deleteSql.append(config.getDbMapping().getDeleteCondition()).append(" AND ");
        }
        for (SchemaItem.FieldItem fieldItem : tableItem.getRelationTableFields().keySet()) {
            SchemaItem.ColumnItem columnItem = fieldItem.getColumn();
            List<SchemaItem.FieldItem> fieldItems = tableItem.getRelationTableFields().get(fieldItem);//关联前表的on字段相关的查询字段
            for (SchemaItem.FieldItem item : fieldItems) {
                if (item.isBinaryOp() || item.isMethod()){
                    continue;
                }
                String targetFieldName = item.getFieldName();
                deleteSql.append(targetFieldName);
                Object value = getValue(columnItem.getColumnName(), data, old);
                if (value == null){
                    deleteSql.append(" IS NULL AND ");
                } else {
                    deleteSql.append(" = ? AND ");
                    Integer type = ctype.get(targetFieldName);
                    if (type == null) {
                        throw new RuntimeException("Target column: " + targetFieldName + " not matched");
                    }
                    BatchExecutor.setValue(deleteParams, type, value);
                }
            }
        }
        int len = deleteSql.length();
        deleteSql.delete(len - 4, len);
        batchExecutor.execute(deleteSql.toString(), deleteParams);
        if (logger.isTraceEnabled()) {
            logger.trace("delete target table, sql: {}", deleteSql.toString());
        }

        //拼接查询sql
        List<Object> selectParams = new ArrayList<>();
        StringBuilder selectWhere = new StringBuilder();
        for (SchemaItem.FieldItem fieldItem : tableItem.getRelationTableFields().keySet()) {
            SchemaItem.ColumnItem columnItem = fieldItem.getColumn();
            SchemaItem.ColumnItem leftColumnItem = null;//对应的关联字段
            for (SchemaItem.RelationFieldsPair relationField : tableItem.getRelationFields()) {
                if (relationField.getLeftFieldItem() == fieldItem){
                    leftColumnItem = relationField.getRightFieldItem().getColumn();
                    break;
                }
                if (relationField.getRightFieldItem() == fieldItem){
                    leftColumnItem = relationField.getLeftFieldItem().getColumn();
                    break;
                }
            }
            selectWhere.append(leftColumnItem.getOwner()).append(".").append(leftColumnItem.getColumnName());
            Object value = getValue(columnItem.getColumnName(), data, old);
            if (value == null){
                selectWhere.append(" IS NULL AND ");
            } else {
                selectWhere.append(" = ? AND ");
                String targetFieldName = null;
                for (SchemaItem.FieldItem item : tableItem.getRelationTableFields().get(fieldItem)) {
                    if (item.isBinaryOp() || item.isMethod()){
                        continue;
                    }
                    targetFieldName = item.getFieldName();
                    break;
                }
                if (targetFieldName == null){
                    throw new RuntimeException("Source column: " + fieldItem.getExpr() + " do not have  select field");
                }
                Integer type = ctype.get(targetFieldName);
                if (type == null) {
                    throw new RuntimeException("Target column: " + targetFieldName + " not matched");
                }
                selectParams.add(value);
            }
        }
        len = selectWhere.length();
        selectWhere.delete(len - 4, len);
        String selectSql = SyncUtil.injectionWhere(config.getDbMapping().getSql(), selectWhere.toString());

        searchAndInsertTargetTable(config, ds, batchExecutor, ctype, selectSql, selectParams);
    }

    private Object getValue(String key, Map<String, Object> data, Map<String, Object> old){
        if (old != null && old.containsKey(key)){
            return old.get(key);
        } else {
            return data.get(key);
        }
    }

    /**
     * 取hash
     */

    public int pkHash(DbMapping dbMapping, int index) {
        int hash = dbMapping.hashCode()+index;
        hash = Math.abs(hash) % threads;
        return Math.abs(hash);
    }

    public void close() {
        for (int i = 0; i < threads; i++) {
            executorThreads[i].shutdown();
        }
    }

    public static class SyncItem {

        private MappingConfig config;
        private SingleDml singleDml;

        public SyncItem(MappingConfig config, SingleDml singleDml){
            this.config = config;
            this.singleDml = singleDml;
        }
    }
}
