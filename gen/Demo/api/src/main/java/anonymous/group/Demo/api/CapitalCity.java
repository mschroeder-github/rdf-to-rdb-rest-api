package anonymous.group.Demo.api;

import java.util.*;
import static java.util.stream.Collectors.toSet;

public class CapitalCity {

    private Long id;

    public CapitalCity() {
        
    }

    public CapitalCity setId(Long id) {
        this.id = id;
        return this;
    }

    public Long getId() {
        return this.id;
    }

    public boolean hasId() {
        return this.id != null;
    }


    public Set<String> getStringSet(String attributeName) {
        switch(attributeName) {
            case "id": return new HashSet<>(Arrays.asList(String.valueOf(getId())));
        }
        return null;
    } 

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("CapitalCity{");
        sb.append("id=").append(id);
        sb.append("}");
        return sb.toString();
    }


}