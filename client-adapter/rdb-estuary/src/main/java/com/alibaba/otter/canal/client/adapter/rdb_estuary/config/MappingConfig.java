package com.alibaba.otter.canal.client.adapter.rdb_estuary.config;

import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;

/**
 * RDB estuary 配置
 *
 * @author rewerma 2018-11-07 下午02:41:34
 * @version 1.0.0
 */
public class MappingConfig implements AdapterConfig {

    private String    dataSourceKey;      // 数据源key

    private String    destination;        // canal实例或MQ的topic

    private String    groupId;            // groupId

    private String    outerAdapterKey;    // 对应适配器的key

    private boolean   concurrent = false; // 是否并行同步

    private DbMapping dbMapping;          // db映射配置

    public String getDataSourceKey() {
        return dataSourceKey;
    }

    public void setDataSourceKey(String dataSourceKey) {
        this.dataSourceKey = dataSourceKey;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getOuterAdapterKey() {
        return outerAdapterKey;
    }

    public void setOuterAdapterKey(String outerAdapterKey) {
        this.outerAdapterKey = outerAdapterKey;
    }

    public boolean getConcurrent() {
        return concurrent;
    }

    public void setConcurrent(boolean concurrent) {
        this.concurrent = concurrent;
    }

    public DbMapping getDbMapping() {
        return dbMapping;
    }

    public void setDbMapping(DbMapping dbMapping) {
        this.dbMapping = dbMapping;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public AdapterMapping getMapping() {
        return dbMapping;
    }

    public void validate() {
        if (dbMapping.getSql() == null || dbMapping.getSql().isEmpty()) {
            throw new NullPointerException("dbMapping.sql");
        }
        if (dbMapping.getTargetTable() == null || dbMapping.getTargetTable().isEmpty()) {
            throw new NullPointerException("dbMapping.targetTable");
        }
        String groupSql = SqlParser.parse4GroupBy(SqlParser.parseSQLSelectQueryBlock(dbMapping.getSql()));
        if (groupSql != null && !(dbMapping.get_id() == null || dbMapping.get_id().isEmpty())) {
            throw new NullPointerException("dbMapping._id");
        }
    }

    public static class DbMapping implements AdapterMapping {

        private String              sql;
        private String              targetDb;                                // 目标库名
        private String              targetTable;                             // 目标表名

        private String              _id;                                     // 主表的主键(或者其他能标识主表唯一行的字段) 对应的目标表中的字段名(带groupBy的情况不需要配置)
        private String              deleteCondition;                         // 用于拼接在delete语句的where条件中
        private int                 commitBatch     = 5000;                  // etl等批量提交大小

        private SchemaItem          schemaItem;                              // sql解析结果模型

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public String get_id() {
            return _id;
        }

        public void set_id(String _id) {
            this._id = _id;
        }

        public String getTargetDb() {
            return targetDb;
        }

        public void setTargetDb(String targetDb) {
            this.targetDb = targetDb;
        }

        public String getTargetTable() {
            return targetTable;
        }

        public void setTargetTable(String targetTable) {
            this.targetTable = targetTable;
        }

        public String getEtlCondition() {
            throw new UnsupportedOperationException();
        }

        public void setDeleteCondition(String deleteCondition) {
            this.deleteCondition = deleteCondition;
        }

        public String getDeleteCondition() {
            return deleteCondition;
        }

        public int getCommitBatch() {
            return commitBatch;
        }

        public void setCommitBatch(int commitBatch) {
            this.commitBatch = commitBatch;
        }

        public SchemaItem getSchemaItem() {
            return schemaItem;
        }

        public void setSchemaItem(SchemaItem schemaItem) {
            this.schemaItem = schemaItem;
        }
    }
}
