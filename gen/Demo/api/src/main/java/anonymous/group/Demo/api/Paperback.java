package anonymous.group.Demo.api;

import java.util.*;
import static java.util.stream.Collectors.toSet;

public class Paperback {

    private Long id;
    private String hasCover;
    private Long takesPlaceIn;

    public Paperback() {
        
    }

    public Paperback setId(Long id) {
        this.id = id;
        return this;
    }

    public Long getId() {
        return this.id;
    }

    public boolean hasId() {
        return this.id != null;
    }

    public Paperback setHasCover(String hasCover) {
        this.hasCover = hasCover;
        return this;
    }

    public String getHasCover() {
        return this.hasCover;
    }

    public boolean hasHasCover() {
        return this.hasCover != null;
    }

    public Paperback setTakesPlaceIn(Long takesPlaceIn) {
        this.takesPlaceIn = takesPlaceIn;
        return this;
    }

    public Long getTakesPlaceIn() {
        return this.takesPlaceIn;
    }

    public boolean hasTakesPlaceIn() {
        return this.takesPlaceIn != null;
    }


    public Set<String> getStringSet(String attributeName) {
        switch(attributeName) {
            case "id": return new HashSet<>(Arrays.asList(String.valueOf(getId())));
            case "hasCover": return new HashSet<>(Arrays.asList(String.valueOf(getHasCover())));
            case "takesPlaceIn": return new HashSet<>(Arrays.asList(String.valueOf(getTakesPlaceIn())));
        }
        return null;
    } 

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Paperback{");
        sb.append("id=").append(id).append(", ");
        sb.append("hasCover=").append(hasCover).append(", ");
        sb.append("takesPlaceIn=").append(takesPlaceIn);
        sb.append("}");
        return sb.toString();
    }


}