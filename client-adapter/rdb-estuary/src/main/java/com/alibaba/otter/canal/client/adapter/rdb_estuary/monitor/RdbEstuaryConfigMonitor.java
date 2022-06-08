package com.alibaba.otter.canal.client.adapter.rdb_estuary.monitor;

import com.alibaba.otter.canal.client.adapter.config.YmlConfigBinder;
import com.alibaba.otter.canal.client.adapter.rdb_estuary.RdbEstuaryAdapter;
import com.alibaba.otter.canal.client.adapter.rdb_estuary.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.support.MappingConfigsLoader;
import com.alibaba.otter.canal.client.adapter.support.Util;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.Properties;

/**
 * @author YangWeiDong
 * @date 2022年05月09日 9:27
 */
public class RdbEstuaryConfigMonitor {
    private static final Logger   logger      = LoggerFactory.getLogger(RdbEstuaryConfigMonitor.class);

    private static final String   adapterName = "rdb-estuary";

    private String                key;

    private RdbEstuaryAdapter     adapter;

    private Properties            envProperties;

    private FileAlterationMonitor fileMonitor;

    public void init(String key, RdbEstuaryAdapter adapter, Properties envProperties) {
        this.key = key;
        this.adapter = adapter;
        this.envProperties = envProperties;
        File confDir = Util.getConfDirPath(adapterName);
        try {
            FileAlterationObserver observer = new FileAlterationObserver(confDir,
                    FileFilterUtils.and(FileFilterUtils.fileFileFilter(), FileFilterUtils.suffixFileFilter("yml")));
            FileListener listener = new FileListener();
            observer.addListener(listener);
            fileMonitor = new FileAlterationMonitor(3000, observer);
            fileMonitor.start();

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void destroy() {
        try {
            fileMonitor.stop();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }


    private class FileListener extends FileAlterationListenerAdaptor {

        @Override
        public void onFileCreate(File file) {
            super.onFileCreate(file);
            try {
                // 加载新增的配置文件
                String configContent = MappingConfigsLoader.loadConfig(adapterName + File.separator + file.getName());
                MappingConfig config = YmlConfigBinder
                        .bindYmlToObj(null, configContent, MappingConfig.class, null, envProperties);
                if (config == null) {
                    return;
                }
                config.validate();
                if ((key == null && config.getOuterAdapterKey() == null)
                        || (key != null && key.equals(config.getOuterAdapterKey()))) {
                    addConfigToCache(file, config);

                    logger.info("Add a new rdb-estuary mapping config: {} to canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        @Override
        public void onFileChange(File file) {
            super.onFileChange(file);

            try {
                if (adapter.getMapping().containsKey(file.getName())) {
                    // 加载配置文件
                    String configContent = MappingConfigsLoader
                            .loadConfig(adapterName + File.separator + file.getName());
                    if (configContent == null) {
                        onFileDelete(file);
                        return;
                    }
                    MappingConfig config = YmlConfigBinder
                            .bindYmlToObj(null, configContent, MappingConfig.class, null, envProperties);
                    if (config == null) {
                        return;
                    }
                    config.validate();
                    if ((key == null && config.getOuterAdapterKey() == null)
                            || (key != null && key.equals(config.getOuterAdapterKey()))) {
                        if (adapter.getMapping().containsKey(file.getName())) {
                            deleteConfigFromCache(file);
                        }
                        addConfigToCache(file, config);
                    } else {
                        // 不能修改outerAdapterKey
                        throw new RuntimeException("Outer adapter key not allowed modify");
                    }
                    logger.info("Change a rdb-estuary mapping config: {} of canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        @Override
        public void onFileDelete(File file) {
            super.onFileDelete(file);

            try {
                if (adapter.getMapping().containsKey(file.getName())) {
                    deleteConfigFromCache(file);

                    logger.info("Delete a rdb-estuary mapping config: {} of canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        private void addConfigToCache(File file, MappingConfig mappingConfig) {
            if (mappingConfig == null || mappingConfig.getDbMapping() == null) {
                return;
            }
            adapter.getMapping().put(file.getName(), mappingConfig);
            adapter.addSyncConfigToCache(file.getName(), mappingConfig);
        }

        private void deleteConfigFromCache(File file) {
            adapter.getMapping().remove(file.getName());
            for (Map<String, MappingConfig> value : adapter.getMappingConfigCache().values()) {
                if (value != null){
                    value.remove(file.getName());
                }
            }
        }
    }
}
