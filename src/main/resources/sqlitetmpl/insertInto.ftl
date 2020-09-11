<#setting number_format="computer">

INSERT INTO ${table.getName()} VALUES
<#list table.getInserts() as insert>
    (<#list insert.data as item><#if !(item??)>NULL<#elseif item?is_string>'${item}'<#elseif item?is_number>${item}</#if><#if !(item?is_last)>,</#if> </#list>)<#if !(insert?is_last)>,</#if>
</#list>;
