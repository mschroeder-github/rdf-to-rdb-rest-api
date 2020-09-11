package anonymous.group.api;

import java.util.*;
import static java.util.stream.Collectors.toSet;

public class Person {

    private Long id;
    private String name;
    private String country;
    private String mboxsha1sum;
    private String publisher;
    private Long date;
    private List<Long> type;

    public Person() {
        
    }

    public Person setId(Long id) {
        this.id = id;
        return this;
    }

    public Long getId() {
        return this.id;
    }

    public boolean hasId() {
        return this.id != null;
    }

    public Person setName(String name) {
        this.name = name;
        return this;
    }

    public String getName() {
        return this.name;
    }

    public boolean hasName() {
        return this.name != null;
    }

    public Person setCountry(String country) {
        this.country = country;
        return this;
    }

    public String getCountry() {
        return this.country;
    }

    public boolean hasCountry() {
        return this.country != null;
    }

    public Person setMboxsha1sum(String mboxsha1sum) {
        this.mboxsha1sum = mboxsha1sum;
        return this;
    }

    public String getMboxsha1sum() {
        return this.mboxsha1sum;
    }

    public boolean hasMboxsha1sum() {
        return this.mboxsha1sum != null;
    }

    public Person setPublisher(String publisher) {
        this.publisher = publisher;
        return this;
    }

    public String getPublisher() {
        return this.publisher;
    }

    public boolean hasPublisher() {
        return this.publisher != null;
    }

    public Person setDate(Long date) {
        this.date = date;
        return this;
    }

    public Long getDate() {
        return this.date;
    }

    public boolean hasDate() {
        return this.date != null;
    }

    public Person setType(List<Long> type) {
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
            case "name": return new HashSet<>(Arrays.asList(String.valueOf(getName())));
            case "country": return new HashSet<>(Arrays.asList(String.valueOf(getCountry())));
            case "mboxsha1sum": return new HashSet<>(Arrays.asList(String.valueOf(getMboxsha1sum())));
            case "publisher": return new HashSet<>(Arrays.asList(String.valueOf(getPublisher())));
            case "date": return new HashSet<>(Arrays.asList(String.valueOf(getDate())));
            case "type": return getType().stream().map(e -> String.valueOf(e)).collect(toSet());
        }
        return null;
    } 

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Person{");
        sb.append("id=").append(id).append(", ");
        sb.append("name=").append(name).append(", ");
        sb.append("country=").append(country).append(", ");
        sb.append("mboxsha1sum=").append(mboxsha1sum).append(", ");
        sb.append("publisher=").append(publisher).append(", ");
        sb.append("date=").append(date).append(", ");
        sb.append("type=").append(type);
        sb.append("}");
        return sb.toString();
    }


}