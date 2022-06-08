package com.alibaba.otter.canal.client.adapter.rdb_estuary;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.rdb_estuary.config.ConfigLoader;
import com.alibaba.otter.canal.client.adapter.rdb_estuary.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.rdb_estuary.config.SchemaItem;
import com.alibaba.otter.canal.client.adapter.rdb_estuary.config.SqlParser;
import com.alibaba.otter.canal.client.adapter.rdb_estuary.monitor.RdbEstuaryConfigMonitor;
import com.alibaba.otter.canal.client.adapter.rdb_estuary.service.EtlService;
import com.alibaba.otter.canal.client.adapter.rdb_estuary.service.SyncService;
import com.alibaba.otter.canal.client.adapter.rdb_estuary.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.*;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 适配器实现类
 * @author YangWeiDong
 * @date 2022年05月07日 15:02
 */
@SPI("rdb-estuary")
public class RdbEstuaryAdapter implements OuterAdapter {
    private static Logger logger              = LoggerFactory.getLogger(RdbEstuaryAdapter.class);

    private Map<String, MappingConfig> mapping          = new ConcurrentHashMap<>();                // 文件名对应配置
    private Map<String, Map<String, MappingConfig>>  mappingConfigCache  = new ConcurrentHashMap<>();                // schema-table|配置文件名|配置

    private DruidDataSource dataSource;             //目标库数据源

    private SyncService syncService;

    private RdbEstuaryConfigMonitor configMonitor;

    private Properties envProperties;

    public Map<String, MappingConfig> getMapping() {
        return mapping;
    }

    public Map<String, Map<String, MappingConfig>> getMappingConfigCache() {
        return mappingConfigCache;
    }

    public SyncService getSyncService() {
        return syncService;
    }

    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        this.envProperties = envProperties;

        Map<String, MappingConfig> rdbMappingTmp = ConfigLoader.load(envProperties);
        // 过滤不匹配的key的配置
        rdbMappingTmp.forEach((key, mappingConfig) -> {
            if ((mappingConfig.getOuterAdapterKey() == null && configuration.getKey() == null)
                    || (mappingConfig.getOuterAdapterKey() != null && mappingConfig.getOuterAdapterKey()
                    .equalsIgnoreCase(configuration.getKey()))) {
                mapping.put(key, mappingConfig);
            }
        });
        if (mapping.isEmpty()) {
            throw new RuntimeException("No rdb estuary adapter found for config key: " + configuration.getKey());
        }
        for (Map.Entry<String, MappingConfig> entry : mapping.entrySet()) {
            String configName = entry.getKey();
            MappingConfig mappingConfig = entry.getValue();
            addSyncConfigToCache(configName, mappingConfig);
        }

        // 初始化连接池
        Map<String, String> properties = configuration.getProperties();
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName(properties.get("jdbc.driverClassName"));
        dataSource.setUrl(properties.get("jdbc.url"));
        dataSource.setUsername(properties.get("jdbc.username"));
        dataSource.setPassword(properties.get("jdbc.password"));
        dataSource.setInitialSize(1);
        dataSource.setMinIdle(1);
        dataSource.setMaxActive(30);
        dataSource.setMaxWait(60000);
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        dataSource.setMinEvictableIdleTimeMillis(300000);
        dataSource.setUseUnfairLock(true);

        try {
            dataSource.init();
        } catch (SQLException e) {
            logger.error("ERROR ## failed to initial datasource: " + properties.get("jdbc.url"), e);
        }

        String threads = properties.get("threads");
        boolean skipDupException = BooleanUtils.toBoolean(configuration.getProperties()
                .getOrDefault("skipDupException", "true"));
        syncService = new SyncService(dataSource,
                threads != null ? Integer.valueOf(threads) : null,
                skipDupException);

        configMonitor = new RdbEstuaryConfigMonitor();
        configMonitor.init(configuration.getKey(), this, envProperties);
    }

    @Override
    public void sync(List<Dml> dmls) {
        if (dmls == null || dmls.isEmpty()) {
            return;
        }
        try {
            syncService.sync(mappingConfigCache, dmls, envProperties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void destroy() {
        if (configMonitor != null) {
            configMonitor.destroy();
        }

        if (syncService != null) {
            syncService.close();
        }

        if (dataSource != null) {
            dataSource.close();
        }
    }

    @Override
    public EtlResult etl(String task, List<String> params) {
        EtlResult etlResult = new EtlResult();
        MappingConfig config = mapping.get(task);
        if (config != null) {
            DataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
            EtlService esEtlService = new EtlService(dataSource, config);
            if (dataSource != null) {
                return esEtlService.importData(params);
            } else {
                etlResult.setSucceeded(false);
                etlResult.setErrorMessage("DataSource not found");
                return etlResult;
            }
        } else {
            StringBuilder resultMsg = new StringBuilder();
            boolean resSuccess = true;
            for (MappingConfig configTmp : mapping.values()) {
                // 取所有的destination为task的配置
                if (configTmp.getDestination().equals(task)) {
                    EtlService esEtlService = new EtlService(dataSource, configTmp);
                    EtlResult etlRes = esEtlService.importData(params);
                    if (!etlRes.getSucceeded()) {
                        resSuccess = false;
                        resultMsg.append(etlRes.getErrorMessage()).append("\n");
                    } else {
                        resultMsg.append(etlRes.getResultMessage()).append("\n");
                    }
                }
            }
            if (resultMsg.length() > 0) {
                etlResult.setSucceeded(resSuccess);
                if (resSuccess) {
                    etlResult.setResultMessage(resultMsg.toString());
                } else {
                    etlResult.setErrorMessage(resultMsg.toString());
                }
                return etlResult;
            }
        }
        etlResult.setSucceeded(false);
        etlResult.setErrorMessage("Task not found");
        return etlResult;
    }

    @Override
    public Map<String, Object> count(String task) {
        MappingConfig config = mapping.get(task);
        MappingConfig.DbMapping dbMapping = config.getDbMapping();
        String sql = "SELECT COUNT(1) AS cnt FROM " + SyncUtil.getDbTableName(dbMapping);
        Connection conn = null;
        Map<String, Object> res = new LinkedHashMap<>();
        try {
            conn = dataSource.getConnection();
            Util.sqlRS(conn, sql, rs -> {
                try {
                    if (rs.next()) {
                        Long rowCount = rs.getLong("cnt");
                        res.put("count", rowCount);
                    }
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            });
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        res.put("targetTable", SyncUtil.getDbTableName(dbMapping));

        return res;
    }

    @Override
    public String getDestination(String task) {
        MappingConfig config = mapping.get(task);
        if (config != null) {
            return config.getDestination();
        }
        return null;
    }

    public void addSyncConfigToCache(String configName, MappingConfig config) {
        Properties envProperties = this.envProperties;
        SchemaItem schemaItem = SqlParser.parse(config.getDbMapping().getSql());
        config.getDbMapping().setSchemaItem(schemaItem);

        DruidDataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        if (dataSource == null || dataSource.getUrl() == null) {
            throw new RuntimeException("No data source found: " + config.getDataSourceKey());
        }
        Pattern pattern = Pattern.compile(".*:(.*)://.*/(.*)\\?.*$");
        Matcher matcher = pattern.matcher(dataSource.getUrl());
        if (!matcher.find()) {
            throw new RuntimeException("Not found the schema of jdbc-url: " + config.getDataSourceKey());
        }
        String schema = matcher.group(2);

        schemaItem.getAliasTableItems()
            .values()
            .forEach(tableItem -> {
                String tableSchema = schema;
                if (tableItem.getSchema() != null && !tableItem.getSchema().isEmpty()){
                    tableSchema = tableItem.getSchema();
                }

                Map<String, MappingConfig> mappingConfigMap;
                if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
                    mappingConfigMap = mappingConfigCache.computeIfAbsent(StringUtils.trimToEmpty(config.getDestination())
                                    + "-"
                                    + StringUtils.trimToEmpty(config.getGroupId())
                                    + "_"
                                    + tableSchema
                                    + "-"
                                    + tableItem.getTableName(),
                            k -> new ConcurrentHashMap<>());
                } else {
                    mappingConfigMap = mappingConfigCache.computeIfAbsent(StringUtils.trimToEmpty(config.getDestination())
                                    + "_"
                                    + tableSchema
                                    + "-"
                                    + tableItem.getTableName(),
                            k -> new ConcurrentHashMap<>());
                }

                mappingConfigMap.put(configName, config);
            });
    }
}
