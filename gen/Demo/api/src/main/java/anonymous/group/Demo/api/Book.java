package anonymous.group.Demo.api;

import java.util.*;
import static java.util.stream.Collectors.toSet;

public class Book {

    private Long id;
    private String hasCover;
    private Long publishedDate;
    private Long takesPlaceIn;
    private List<LangString> name;

    public Book() {
        
    }

    public Book setId(Long id) {
        this.id = id;
        return this;
    }

    public Long getId() {
        return this.id;
    }

    public boolean hasId() {
        return this.id != null;
    }

    public Book setHasCover(String hasCover) {
        this.hasCover = hasCover;
        return this;
    }

    public String getHasCover() {
        return this.hasCover;
    }

    public boolean hasHasCover() {
        return this.hasCover != null;
    }

    public Book setPublishedDate(Long publishedDate) {
        this.publishedDate = publishedDate;
        return this;
    }

    public Long getPublishedDate() {
        return this.publishedDate;
    }

    public boolean hasPublishedDate() {
        return this.publishedDate != null;
    }

    public Book setTakesPlaceIn(Long takesPlaceIn) {
        this.takesPlaceIn = takesPlaceIn;
        return this;
    }

    public Long getTakesPlaceIn() {
        return this.takesPlaceIn;
    }

    public boolean hasTakesPlaceIn() {
        return this.takesPlaceIn != null;
    }

    public Book setName(List<LangString> name) {
        this.name = name;
        return this;
    }

    public List<LangString> getName() {
        return this.name;
    }

    public boolean hasName() {
        return this.name != null;
    }


    public Set<String> getStringSet(String attributeName) {
        switch(attributeName) {
            case "id": return new HashSet<>(Arrays.asList(String.valueOf(getId())));
            case "hasCover": return new HashSet<>(Arrays.asList(String.valueOf(getHasCover())));
            case "publishedDate": return new HashSet<>(Arrays.asList(String.valueOf(getPublishedDate())));
            case "takesPlaceIn": return new HashSet<>(Arrays.asList(String.valueOf(getTakesPlaceIn())));
            case "name": return getName().stream().map(e -> String.valueOf(e)).collect(toSet());
        }
        return null;
    } 

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Book{");
        sb.append("id=").append(id).append(", ");
        sb.append("hasCover=").append(hasCover).append(", ");
        sb.append("publishedDate=").append(publishedDate).append(", ");
        sb.append("takesPlaceIn=").append(takesPlaceIn).append(", ");
        sb.append("name=").append(name);
        sb.append("}");
        return sb.toString();
    }


}