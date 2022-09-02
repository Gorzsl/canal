package com.alibaba.otter.canal.client.adapter.rdb_estuary.config;

import java.util.*;


/**
 * ES 映射配置视图
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class SchemaItem {

    private Map<String, TableItem>                aliasTableItems = new LinkedHashMap<>(); // 别名对应表名
    private Map<String, FieldItem>                selectFields    = new LinkedHashMap<>(); // 查询字段
    private List<FieldItem>                       groupByFields   = new ArrayList<>();     // groupBy字段
    private String                                sql;

    private volatile Map<String, List<TableItem>> tableItemAliases; //表名 - 表对象(list) 的映射 (不同所属schema的表也会放在同一个value中)
    private volatile Map<String, List<FieldItem>> columnFields; //原始列(文本：schema.tableName) - 原始列相关的查询字段(数组)的映射
    private volatile Boolean                      allFieldsSimple;

    public void init() {
        this.getTableItemAliases();
        this.getColumnFields();
        this.isAllFieldsSimple();
        aliasTableItems.values().forEach(tableItem -> {
            tableItem.getRelationTableFields();
            tableItem.getRelationSelectFieldItems();
            tableItem.getRelationGroupFieldItems();

            for (Map.Entry<FieldItem, List<FieldItem>> entry : tableItem.getRelationTableFields().entrySet()) {
                FieldItem fieldItem = entry.getKey();
                List<FieldItem> selectFieldItem = entry.getValue();
                FieldItem anotherFieldItem = null;
                for (RelationFieldsPair relationField : tableItem.getRelationFields()) {
                    if (relationField.getLeftFieldItem() == fieldItem){
                        anotherFieldItem = relationField.getRightFieldItem();
                        break;
                    }
                    if (relationField.getRightFieldItem() == fieldItem){
                        anotherFieldItem = relationField.getLeftFieldItem();
                        break;
                    }
                }

                if (anotherFieldItem != null) {
                    TableItem anotherTableItem = aliasTableItems.get(anotherFieldItem.getOwner());
                    anotherTableItem.getAnotherRelationTableFields().put(anotherFieldItem, selectFieldItem);
                }
            }
        });
    }

    public Map<String, TableItem> getAliasTableItems() {
        return aliasTableItems;
    }

    public void setAliasTableItems(Map<String, TableItem> aliasTableItems) {
        this.aliasTableItems = aliasTableItems;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Map<String, FieldItem> getSelectFields() {
        return selectFields;
    }

    public void setSelectFields(Map<String, FieldItem> selectFields) {
        this.selectFields = selectFields;
    }

    public List<FieldItem> getGroupByFields() {
        return groupByFields;
    }

    public void setGroupByFields(List<FieldItem> groupByFields) {
        this.groupByFields = groupByFields;
    }

    public String toSql() {
        // todo
        return null;
    }

    public Map<String, List<TableItem>> getTableItemAliases() {
        if (tableItemAliases == null) {
            synchronized (SchemaItem.class) {
                if (tableItemAliases == null) {
                    tableItemAliases = new LinkedHashMap<>();
                    aliasTableItems.forEach((alias, tableItem) -> {
                        List<TableItem> aliases = tableItemAliases
                            .computeIfAbsent(tableItem.getTableName().toLowerCase(), k -> new ArrayList<>());
                        aliases.add(tableItem);
                    });
                }
            }
        }
        return tableItemAliases;
    }

    public Map<String, List<FieldItem>> getColumnFields() {
        if (columnFields == null) {
            synchronized (SchemaItem.class) {
                if (columnFields == null) {
                    columnFields = new LinkedHashMap<>();
                    getSelectFields()
                        .forEach((fieldName, fieldItem) -> fieldItem.getColumnItems().forEach(columnItem -> {
                            // TableItem tableItem = getAliasTableItems().get(columnItem.getOwner());
                            // if (!tableItem.isSubQuery()) {
                            //当数据列并非原始列时，columnName是空的，例如concat('px',id)
                            if(columnItem.getColumnName() != null) {
                                List<FieldItem> fieldItems = columnFields.computeIfAbsent(
                                        columnItem.getOwner() + "." + columnItem.getColumnName(),
                                        k -> new ArrayList<>());
                                fieldItems.add(fieldItem);
                            }
                            // } else {
                            // tableItem.getSubQueryFields().forEach(subQueryField -> {
                            // List<FieldItem> fieldItems = columnFields.computeIfAbsent(
                            // columnItem.getOwner() + "." + subQueryField.getColumn().getColumnName(),
                            // k -> new ArrayList<>());
                            // fieldItems.add(fieldItem);
                            // });
                            // }
                        }));
                }
            }
        }
        return columnFields;
    }

    public boolean isAllFieldsSimple() {
        if (allFieldsSimple == null) {
            synchronized (SchemaItem.class) {
                if (allFieldsSimple == null) {
                    allFieldsSimple = true;

                    for (FieldItem fieldItem : getSelectFields().values()) {
                        if (fieldItem.isMethod() || fieldItem.isBinaryOp() || fieldItem.getColumnItems().isEmpty()) {
                            allFieldsSimple = false;
                            break;
                        }
                    }
                }
            }
        }

        return allFieldsSimple;
    }

    //是否使用了group by
    public Boolean isGroup() {
        return !getGroupByFields().isEmpty();
    }

    public TableItem getMainTable() {
        if (!aliasTableItems.isEmpty()) {
            return aliasTableItems.values().iterator().next();
        } else {
            return null;
        }
    }

    public FieldItem getIdFieldItem(MappingConfig.DbMapping mapping) {
        return getSelectFields().get(mapping.get_id());
    }

    public static class TableItem {

        private SchemaItem                               schemaItem;

        private String                                   schema;
        private String                                   tableName;
        private String                                   alias;
        private String                                   subQuerySql;
        private List<FieldItem>                          subQueryFields = new ArrayList<>();
        private List<RelationFieldsPair>                 relationFields = new ArrayList<>(); //或许可以理解为该表对象的下一行中on涉及的关联对

        private boolean                                  main;
        private boolean                                  subQuery;

        private volatile Map<FieldItem, List<FieldItem>> relationTableFields;               // 当前表关联条件字段对应主表查询字段
        private Map<FieldItem, List<FieldItem>>          anotherRelationTableFields = new HashMap<>();// 当前表被其他表关联的字段对应的查询字段
        private volatile List<FieldItem>                 relationSelectFieldItems;          // 子表所在主表的查询字段
        private volatile List<FieldItem>                 relationGroupFieldItems;           // 子表涉及的groupBy字段

        public TableItem(SchemaItem schemaItem){
            this.schemaItem = schemaItem;
        }

        public SchemaItem getSchemaItem() {
            return schemaItem;
        }

        public void setSchemaItem(SchemaItem schemaItem) {
            this.schemaItem = schemaItem;
        }

        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getAlias() {
            return alias;
        }

        public void setAlias(String alias) {
            this.alias = alias;
        }

        public String getSubQuerySql() {
            return subQuerySql;
        }

        public void setSubQuerySql(String subQuerySql) {
            this.subQuerySql = subQuerySql;
        }

        public boolean isMain() {
            return main;
        }

        public void setMain(boolean main) {
            this.main = main;
        }

        public boolean isSubQuery() {
            return subQuery;
        }

        public void setSubQuery(boolean subQuery) {
            this.subQuery = subQuery;
        }

        public List<FieldItem> getSubQueryFields() {
            return subQueryFields;
        }

        public void setSubQueryFields(List<FieldItem> subQueryFields) {
            this.subQueryFields = subQueryFields;
        }

        public List<RelationFieldsPair> getRelationFields() {
            return relationFields;
        }

        public void setRelationFields(List<RelationFieldsPair> relationFields) {
            this.relationFields = relationFields;
        }

        public Map<FieldItem, List<FieldItem>> getRelationTableFields() {
            if (relationTableFields == null) {
                synchronized (SchemaItem.class) {
                    if (relationTableFields == null) {
                        relationTableFields = new LinkedHashMap<>();

                        getRelationFields().forEach(relationFieldsPair -> {
                            FieldItem leftFieldItem = relationFieldsPair.getLeftFieldItem();
                            FieldItem rightFieldItem = relationFieldsPair.getRightFieldItem();
                            FieldItem currentTableRelField = null;
                            if (getAlias().equals(leftFieldItem.getOwner())) {
                                currentTableRelField = leftFieldItem;
                            } else if (getAlias().equals(rightFieldItem.getOwner())) {
                                currentTableRelField = rightFieldItem;
                            }

                            if (currentTableRelField != null) {
                                List<FieldItem> fieldItems = new ArrayList<>();

                                List<FieldItem> leftSelectFieldItem = getSchemaItem().getColumnFields()
                                    .get(leftFieldItem.getOwner() + "." + leftFieldItem.getColumn().getColumnName());
                                if (leftSelectFieldItem != null && !leftSelectFieldItem.isEmpty()) {
                                    fieldItems.addAll(leftSelectFieldItem);
                                }
                                List<FieldItem> rightSelectFieldItem = getSchemaItem().getColumnFields()
                                        .get(rightFieldItem.getOwner() + "." + rightFieldItem.getColumn().getColumnName());
                                if (rightSelectFieldItem != null && !rightSelectFieldItem.isEmpty()) {
                                    fieldItems.addAll(rightSelectFieldItem);
                                }

                                relationTableFields.put(currentTableRelField, fieldItems);
                            }
                        });
                    }
                }
            }
            return relationTableFields;
        }

        public Map<FieldItem, List<FieldItem>> getAnotherRelationTableFields() {
            return anotherRelationTableFields;
        }

        public List<FieldItem> getRelationSelectFieldItems() {
            if (relationSelectFieldItems == null) {
                synchronized (SchemaItem.class) {
                    if (relationSelectFieldItems == null) {
                        List<FieldItem> relationSelectFieldItemsTmp = new ArrayList<>();
                        for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
                            if (fieldItem.getOwners().contains(getAlias())) {
                                relationSelectFieldItemsTmp.add(fieldItem);
                            }
                        }
                        relationSelectFieldItems = relationSelectFieldItemsTmp;
                    }
                }
            }
            return relationSelectFieldItems;
        }

        public List<FieldItem> getRelationGroupFieldItems() {
            if (relationGroupFieldItems == null){
                synchronized (SchemaItem.class){
                    if (relationGroupFieldItems == null){
                        List<FieldItem> relationGroupFieldItemsTmp = new ArrayList<>();
                        for (FieldItem field : schemaItem.getGroupByFields()) {
                            if (field.getOwners().contains(getAlias())) {
                                relationGroupFieldItemsTmp.add(field);
                            }
                        }
                        relationGroupFieldItems = relationGroupFieldItemsTmp;
                    }
                }
            }
            return relationGroupFieldItems;
        }
    }

    public static class RelationFieldsPair {

        private FieldItem leftFieldItem;
        private FieldItem rightFieldItem;

        public RelationFieldsPair(FieldItem leftFieldItem, FieldItem rightFieldItem){
            this.leftFieldItem = leftFieldItem;
            this.rightFieldItem = rightFieldItem;
        }

        public FieldItem getLeftFieldItem() {
            return leftFieldItem;
        }

        public void setLeftFieldItem(FieldItem leftFieldItem) {
            this.leftFieldItem = leftFieldItem;
        }

        public FieldItem getRightFieldItem() {
            return rightFieldItem;
        }

        public void setRightFieldItem(FieldItem rightFieldItem) {
            this.rightFieldItem = rightFieldItem;
        }
    }

    public static class FieldItem {

        private String           fieldName;
        private String           expr;
        private List<ColumnItem> columnItems = new ArrayList<>();
        private List<String>     owners      = new ArrayList<>();

        private boolean          method;
        private boolean          binaryOp;

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getExpr() {
            return expr;
        }

        public void setExpr(String expr) {
            this.expr = expr;
        }

        public List<ColumnItem> getColumnItems() {
            return columnItems;
        }

        public void setColumnItems(List<ColumnItem> columnItems) {
            this.columnItems = columnItems;
        }

        public boolean isMethod() {
            return method;
        }

        public void setMethod(boolean method) {
            this.method = method;
        }

        public boolean isBinaryOp() {
            return binaryOp;
        }

        public void setBinaryOp(boolean binaryOp) {
            this.binaryOp = binaryOp;
        }

        public List<String> getOwners() {
            return owners;
        }

        public void setOwners(List<String> owners) {
            this.owners = owners;
        }

        public void addColumn(ColumnItem columnItem) {
            columnItems.add(columnItem);
        }

        public ColumnItem getColumn() {
            if (!columnItems.isEmpty()) {
                return columnItems.get(0);
            } else {
                return null;
            }
        }

        public String getOwner() {
            if (!owners.isEmpty()) {
                return owners.get(0);
            } else {
                return null;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FieldItem fieldItem = (FieldItem) o;

            return fieldName != null ? fieldName.equals(fieldItem.fieldName) : fieldItem.fieldName == null;
        }

        @Override
        public int hashCode() {
            return fieldName != null ? fieldName.hashCode() : 0;
        }
    }

    public static class ColumnItem {

        private String owner;
        private String columnName;

        public String getOwner() {
            return owner;
        }

        public void setOwner(String owner) {
            this.owner = owner;
        }

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }
    }
}
