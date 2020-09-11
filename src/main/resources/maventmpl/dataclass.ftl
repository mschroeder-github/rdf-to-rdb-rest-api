package ${package};

import java.util.*;
import static java.util.stream.Collectors.toSet;

public class ${javaClass.getName()} {

    <#list javaClass.getAttributes() as attr>
    private ${attr.getType()} ${attr.getName()};
    </#list>

    public ${javaClass.getName()}() {
        
    }

    <#list javaClass.getAttributes() as attr>
    public ${javaClass.getName()} set${attr.getMethodName()}(${attr.getType()} ${attr.getName()}) {
        this.${attr.getName()} = ${attr.getName()};
        return this;
    }

    public ${attr.getType()} get${attr.getMethodName()}() {
        return this.${attr.getName()};
    }

    public boolean has${attr.getMethodName()}() {
        return this.${attr.getName()} != null;
    }

    </#list>

    public Set<String> getStringSet(String attributeName) {
        switch(attributeName) {
            <#list javaClass.getSingleAttributes() as attr>
            case "${attr.getName()}": return new HashSet<>(Arrays.asList(String.valueOf(get${attr.getMethodName()}())));
            </#list>
            <#list javaClass.getListAttributes() as attr>
            case "${attr.getName()}": return get${attr.getMethodName()}().stream().map(e -> String.valueOf(e)).collect(toSet());
            </#list>
        }
        return null;
    } 

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("${javaClass.getName()}{");
        <#list javaClass.getAttributes() as attr>
        sb.append("${attr.getName()}=").append(${attr.getName()})<#if !(attr?is_last)>.append(", ")</#if>;
        </#list>
        sb.append("}");
        return sb.toString();
    }


}