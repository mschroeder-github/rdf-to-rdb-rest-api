package anonymous.group.api;

import java.util.*;
import static java.util.stream.Collectors.toSet;

public class ProductType {

    private Long id;
    private String label;
    private String comment;
    private String publisher;
    private Long date;
    private Long subClassOf;
    private List<Long> type;

    public ProductType() {
        
    }

    public ProductType setId(Long id) {
        this.id = id;
        return this;
    }

    public Long getId() {
        return this.id;
    }

    public boolean hasId() {
        return this.id != null;
    }

    public ProductType setLabel(String label) {
        this.label = label;
        return this;
    }

    public String getLabel() {
        return this.label;
    }

    public boolean hasLabel() {
        return this.label != null;
    }

    public ProductType setComment(String comment) {
        this.comment = comment;
        return this;
    }

    public String getComment() {
        return this.comment;
    }

    public boolean hasComment() {
        return this.comment != null;
    }

    public ProductType setPublisher(String publisher) {
        this.publisher = publisher;
        return this;
    }

    public String getPublisher() {
        return this.publisher;
    }

    public boolean hasPublisher() {
        return this.publisher != null;
    }

    public ProductType setDate(Long date) {
        this.date = date;
        return this;
    }

    public Long getDate() {
        return this.date;
    }

    public boolean hasDate() {
        return this.date != null;
    }

    public ProductType setSubClassOf(Long subClassOf) {
        this.subClassOf = subClassOf;
        return this;
    }

    public Long getSubClassOf() {
        return this.subClassOf;
    }

    public boolean hasSubClassOf() {
        return this.subClassOf != null;
    }

    public ProductType setType(List<Long> type) {
        this.type = type;
        return this;
    }

    public List<Long> getType() {
        return this.type;
    }

    public boolean hasType() {
        return this.type != null;
    }


    public Set<String> getStringSet(String attributeName) {
        switch(attributeName) {
            case "id": return new HashSet<>(Arrays.asList(String.valueOf(getId())));
            case "label": return new HashSet<>(Arrays.asList(String.valueOf(getLabel())));
            case "comment": return new HashSet<>(Arrays.asList(String.valueOf(getComment())));
            case "publisher": return new HashSet<>(Arrays.asList(String.valueOf(getPublisher())));
            case "date": return new HashSet<>(Arrays.asList(String.valueOf(getDate())));
            case "subClassOf": return new HashSet<>(Arrays.asList(String.valueOf(getSubClassOf())));
            case "type": return getType().stream().map(e -> String.valueOf(e)).collect(toSet());
        }
        return null;
    } 

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ProductType{");
        sb.append("id=").append(id).append(", ");
        sb.append("label=").append(label).append(", ");
        sb.append("comment=").append(comment).append(", ");
        sb.append("publisher=").append(publisher).append(", ");
        sb.append("date=").append(date).append(", ");
        sb.append("subClassOf=").append(subClassOf).append(", ");
        sb.append("type=").append(type);
        sb.append("}");
        return sb.toString();
    }


}