package anonymous.group.Demo.api;

import java.util.*;
import static java.util.stream.Collectors.toSet;

public class Page {

    private Long id;
    private Long bookHasPage;
    private Long paperbackHasPage;
    private List<Long> mentionsCapitalCity;

    public Page() {
        
    }

    public Page setId(Long id) {
        this.id = id;
        return this;
    }

    public Long getId() {
        return this.id;
    }

    public boolean hasId() {
        return this.id != null;
    }

    public Page setBookHasPage(Long bookHasPage) {
        this.bookHasPage = bookHasPage;
        return this;
    }

    public Long getBookHasPage() {
        return this.bookHasPage;
    }

    public boolean hasBookHasPage() {
        return this.bookHasPage != null;
    }

    public Page setPaperbackHasPage(Long paperbackHasPage) {
        this.paperbackHasPage = paperbackHasPage;
        return this;
    }

    public Long getPaperbackHasPage() {
        return this.paperbackHasPage;
    }

    public boolean hasPaperbackHasPage() {
        return this.paperbackHasPage != null;
    }

    public Page setMentionsCapitalCity(List<Long> mentionsCapitalCity) {
        this.mentionsCapitalCity = mentionsCapitalCity;
        return this;
    }

    public List<Long> getMentionsCapitalCity() {
        return this.mentionsCapitalCity;
    }

    public boolean hasMentionsCapitalCity() {
        return this.mentionsCapitalCity != null;
    }


    public Set<String> getStringSet(String attributeName) {
        switch(attributeName) {
            case "id": return new HashSet<>(Arrays.asList(String.valueOf(getId())));
            case "bookHasPage": return new HashSet<>(Arrays.asList(String.valueOf(getBookHasPage())));
            case "paperbackHasPage": return new HashSet<>(Arrays.asList(String.valueOf(getPaperbackHasPage())));
            case "mentionsCapitalCity": return getMentionsCapitalCity().stream().map(e -> String.valueOf(e)).collect(toSet());
        }
        return null;
    } 

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Page{");
        sb.append("id=").append(id).append(", ");
        sb.append("bookHasPage=").append(bookHasPage).append(", ");
        sb.append("paperbackHasPage=").append(paperbackHasPage).append(", ");
        sb.append("mentionsCapitalCity=").append(mentionsCapitalCity);
        sb.append("}");
        return sb.toString();
    }


}