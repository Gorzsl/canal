package com.alibaba.otter.canal.client.adapter.rdb_estuary.service;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.rdb_estuary.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.rdb_estuary.config.MappingConfig.DbMapping;
import com.alibaba.otter.canal.client.adapter.rdb_estuary.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.*;
import com.google.common.base.Joiner;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author YangWeiDong
 * @date 2022年05月09日 13:23
 */
public class EtlService extends AbstractEtlService {

    private final static String type = "rdb-estuary";
    private final long   CNT_PER_TASK = 10000L;

    private DataSource    targetDS;
    private MappingConfig config;

    public EtlService(DataSource targetDS, MappingConfig config){
        super(type, config);
        this.targetDS = targetDS;
        this.config = config;
    }

    public EtlResult importData(List<String> params) {
        DbMapping mapping = config.getDbMapping();
        logger.info("start etl to import data");
        String sql = mapping.getSql();
        return importData(sql, params);
    }

    @Override
    protected boolean executeSqlImport(DataSource srcDS, String sql, List<Object> values, AdapterConfig.AdapterMapping mapping, AtomicLong impCount, List<String> errMsg) {

        try {
            DbMapping dbMapping = (DbMapping) mapping;
            Map<String, Integer> columnType = SyncUtil.getColumnTypeMap(targetDS, SyncUtil.getDbTableName(dbMapping));//目标表的列名 和 字段类型的映射

            Util.sqlRS(srcDS, sql, values, rs -> {
                int idx = 1;

                try {
                    boolean completed = false;

                    StringBuilder insertSql = new StringBuilder();
                    insertSql.append("INSERT INTO ").append(SyncUtil.getDbTableName(dbMapping)).append(" (");
                    dbMapping.getSchemaItem().getSelectFields().values()
                            .forEach(fieldItem -> insertSql.append(fieldItem.getFieldName()).append(","));

                    int len = insertSql.length();
                    insertSql.delete(len - 1, len).append(") VALUES (");
                    int mapLen = dbMapping.getSchemaItem().getSelectFields().size();
                    for (int i = 0; i < mapLen; i++) {
                        insertSql.append("?,");
                    }
                    len = insertSql.length();
                    insertSql.delete(len - 1, len).append(")");

                    try (Connection connTarget = targetDS.getConnection();
                         PreparedStatement pstmt = connTarget.prepareStatement(insertSql.toString())) {
                        connTarget.setAutoCommit(false);

                        while (rs.next()) {
                            completed = false;
                            pstmt.clearParameters();

                            int i = 1;
                            for (String fieldName : dbMapping.getSchemaItem().getSelectFields().keySet()) {
                                Integer type = columnType.get(fieldName);

                                Object value = rs.getObject(fieldName);
                                if (value != null) {
                                    SyncUtil.setPStmt(type, pstmt, value, i);
                                } else {
                                    pstmt.setNull(i, type);
                                }

                                i++;
                            }

                            pstmt.execute();
                            if (logger.isTraceEnabled()) {
                                logger.trace("Insert into target table, sql: {}", insertSql);
                            }

                            if (idx % dbMapping.getCommitBatch() == 0) {
                                connTarget.commit();
                                completed = true;
                            }
                            idx++;
                            impCount.incrementAndGet();
                            if (logger.isDebugEnabled()) {
                                logger.debug("successful import count:" + impCount.get());
                            }
                        }
                        if (!completed) {
                            connTarget.commit();
                        }
                    }

                } catch (Exception e) {
                    logger.error(dbMapping.getTargetTable() + " etl failed! ==>" + e.getMessage(), e);
                    errMsg.add(dbMapping.getTargetTable() + " etl failed! ==>" + e.getMessage());
                }
                return idx;
            });

            return true;
        } catch (Exception e){
            logger.error(e.getMessage(), e);
            return false;
        }
    }

    @Override
    protected EtlResult importData(String sql, List<String> params) {
        EtlResult etlResult = new EtlResult();
        AtomicLong impCount = new AtomicLong();
        List<String> errMsg = new ArrayList<>();
        if (config == null) {
            logger.warn("{} mapping config is null, etl go end ", type);
            etlResult.setErrorMessage(type + "mapping config is null, etl go end ");
            return etlResult;
        }

        long start = System.currentTimeMillis();
        try {
            DruidDataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());

            //清空目标表
            String truncateSql = "TRUNCATE " + SyncUtil.getDbTableName(config.getDbMapping());
            try (Connection connTarget = targetDS.getConnection();
                 PreparedStatement pstmt = connTarget.prepareStatement(truncateSql)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("truncate sql : {}", truncateSql);
                }
                pstmt.execute();
            }

            List<Object> values = new ArrayList<>();
            if (logger.isDebugEnabled()) {
                logger.debug("etl sql : {}", sql);
            }

            // 获取总数
            String countSql = "SELECT COUNT(1) FROM ( " + sql + ") _CNT ";
            long cnt = (Long) Util.sqlRS(dataSource, countSql, values, rs -> {
                Long count = null;
                try {
                    if (rs.next()) {
                        count = ((Number) rs.getObject(1)).longValue();
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
                return count == null ? 0L : count;
            });

            // 当大于1万条记录时开启多线程
            if (cnt >= 10000) {
                int threadCount = Runtime.getRuntime().availableProcessors();

                long offset;
                long size = CNT_PER_TASK;
                long workerCnt = cnt / size + (cnt % size == 0 ? 0 : 1);

                if (logger.isDebugEnabled()) {
                    logger.debug("workerCnt {} for cnt {} threadCount {}", workerCnt, cnt, threadCount);
                }

                ExecutorService executor = Util.newFixedThreadPool(threadCount, 5000L);
                List<Future<Boolean>> futures = new ArrayList<>();
                for (long i = 0; i < workerCnt; i++) {
                    offset = size * i;
                    String sqlFinal = sql + " LIMIT " + offset + "," + size;
                    Future<Boolean> future = executor.submit(() -> executeSqlImport(dataSource,
                            sqlFinal,
                            values,
                            config.getMapping(),
                            impCount,
                            errMsg));
                    futures.add(future);
                }

                for (Future<Boolean> future : futures) {
                    future.get();
                }
                executor.shutdown();
            } else {
                executeSqlImport(dataSource, sql, values, config.getMapping(), impCount, errMsg);
            }

            logger.info("数据全量导入完成, 一共导入 {} 条数据, 耗时: {}", impCount.get(), System.currentTimeMillis() - start);
            etlResult.setResultMessage("导入" + type + " 数据：" + impCount.get() + " 条");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            errMsg.add(type + " 数据导入异常 =>" + e.getMessage());
        }
        if (errMsg.isEmpty()) {
            etlResult.setSucceeded(true);
        } else {
            etlResult.setErrorMessage(Joiner.on("\n").join(errMsg));
        }
        return etlResult;
    }
}
