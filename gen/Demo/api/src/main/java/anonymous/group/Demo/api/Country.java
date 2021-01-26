package anonymous.group.Demo.api;

import java.util.*;
import static java.util.stream.Collectors.toSet;

public class Country {

    private Long id;
    private Long hasCapitalCity;

    public Country() {
        
    }

    public Country setId(Long id) {
        this.id = id;
        return this;
    }

    public Long getId() {
        return this.id;
    }

    public boolean hasId() {
        return this.id != null;
    }

    public Country setHasCapitalCity(Long hasCapitalCity) {
        this.hasCapitalCity = hasCapitalCity;
        return this;
    }

    public Long getHasCapitalCity() {
        return this.hasCapitalCity;
    }

    public boolean hasHasCapitalCity() {
        return this.hasCapitalCity != null;
    }


    public Set<String> getStringSet(String attributeName) {
        switch(attributeName) {
            case "id": return new HashSet<>(Arrays.asList(String.valueOf(getId())));
            case "hasCapitalCity": return new HashSet<>(Arrays.asList(String.valueOf(getHasCapitalCity())));
        }
        return null;
    } 

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Country{");
        sb.append("id=").append(id).append(", ");
        sb.append("hasCapitalCity=").append(hasCapitalCity);
        sb.append("}");
        return sb.toString();
    }


}