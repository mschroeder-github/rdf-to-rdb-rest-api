CREATE TABLE IF NOT EXISTS "${table.getName()}" (
    <#list table.getColumns() as column>
        "${column.getName()}" ${column.getType()} <#if column.isNotNull()>NOT NULL</#if> <#if table.hasSinglePrimaryKey() && column.isPrimary()>PRIMARY KEY <#if column.isAutoincrement()>AUTOINCREMENT</#if></#if> <#if !(column?is_last)>,</#if>
    </#list>

    <#if !table.hasSinglePrimaryKey()>
    ,
    PRIMARY KEY (
        <#list table.getPrimaryKeyColumns() as column>
            ${column.getName()}<#if !(column?is_last)>,</#if>
        </#list>
    )
    </#if>
);
