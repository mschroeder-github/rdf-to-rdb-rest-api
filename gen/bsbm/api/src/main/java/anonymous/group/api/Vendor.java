package anonymous.group.api;

import java.util.*;
import static java.util.stream.Collectors.toSet;

public class Vendor {

    private Long id;
    private String label;
    private String comment;
    private String country;
    private String homepage;
    private String publisher;
    private Long date;
    private List<Long> type;

    public Vendor() {
        
    }

    public Vendor setId(Long id) {
        this.id = id;
        return this;
    }

    public Long getId() {
        return this.id;
    }

    public boolean hasId() {
        return this.id != null;
    }

    public Vendor setLabel(String label) {
        this.label = label;
        return this;
    }

    public String getLabel() {
        return this.label;
    }

    public boolean hasLabel() {
        return this.label != null;
    }

    public Vendor setComment(String comment) {
        this.comment = comment;
        return this;
    }

    public String getComment() {
        return this.comment;
    }

    public boolean hasComment() {
        return this.comment != null;
    }

    public Vendor setCountry(String country) {
        this.country = country;
        return this;
    }

    public String getCountry() {
        return this.country;
    }

    public boolean hasCountry() {
        return this.country != null;
    }

    public Vendor setHomepage(String homepage) {
        this.homepage = homepage;
        return this;
    }

    public String getHomepage() {
        return this.homepage;
    }

    public boolean hasHomepage() {
        return this.homepage != null;
    }

    public Vendor setPublisher(String publisher) {
        this.publisher = publisher;
        return this;
    }

    public String getPublisher() {
        return this.publisher;
    }

    public boolean hasPublisher() {
        return this.publisher != null;
    }

    public Vendor setDate(Long date) {
        this.date = date;
        return this;
    }

    public Long getDate() {
        return this.date;
    }

    public boolean hasDate() {
        return this.date != null;
    }

    public Vendor setType(List<Long> type) {
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
            case "country": return new HashSet<>(Arrays.asList(String.valueOf(getCountry())));
            case "homepage": return new HashSet<>(Arrays.asList(String.valueOf(getHomepage())));
            case "publisher": return new HashSet<>(Arrays.asList(String.valueOf(getPublisher())));
            case "date": return new HashSet<>(Arrays.asList(String.valueOf(getDate())));
            case "type": return getType().stream().map(e -> String.valueOf(e)).collect(toSet());
        }
        return null;
    } 

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Vendor{");
        sb.append("id=").append(id).append(", ");
        sb.append("label=").append(label).append(", ");
        sb.append("comment=").append(comment).append(", ");
        sb.append("country=").append(country).append(", ");
        sb.append("homepage=").append(homepage).append(", ");
        sb.append("publisher=").append(publisher).append(", ");
        sb.append("date=").append(date).append(", ");
        sb.append("type=").append(type);
        sb.append("}");
        return sb.toString();
    }


}