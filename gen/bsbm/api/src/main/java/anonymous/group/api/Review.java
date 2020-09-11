package anonymous.group.api;

import java.util.*;
import static java.util.stream.Collectors.toSet;

public class Review {

    private Long id;
    private String title;
    private String publisher;
    private Long date;
    private Long rating1;
    private Long rating2;
    private Long rating3;
    private Long rating4;
    private Long reviewDate;
    private Long reviewFor;
    private Long reviewer;
    private List<LangString> text;
    private List<Long> type;

    public Review() {
        
    }

    public Review setId(Long id) {
        this.id = id;
        return this;
    }

    public Long getId() {
        return this.id;
    }

    public boolean hasId() {
        return this.id != null;
    }

    public Review setTitle(String title) {
        this.title = title;
        return this;
    }

    public String getTitle() {
        return this.title;
    }

    public boolean hasTitle() {
        return this.title != null;
    }

    public Review setPublisher(String publisher) {
        this.publisher = publisher;
        return this;
    }

    public String getPublisher() {
        return this.publisher;
    }

    public boolean hasPublisher() {
        return this.publisher != null;
    }

    public Review setDate(Long date) {
        this.date = date;
        return this;
    }

    public Long getDate() {
        return this.date;
    }

    public boolean hasDate() {
        return this.date != null;
    }

    public Review setRating1(Long rating1) {
        this.rating1 = rating1;
        return this;
    }

    public Long getRating1() {
        return this.rating1;
    }

    public boolean hasRating1() {
        return this.rating1 != null;
    }

    public Review setRating2(Long rating2) {
        this.rating2 = rating2;
        return this;
    }

    public Long getRating2() {
        return this.rating2;
    }

    public boolean hasRating2() {
        return this.rating2 != null;
    }

    public Review setRating3(Long rating3) {
        this.rating3 = rating3;
        return this;
    }

    public Long getRating3() {
        return this.rating3;
    }

    public boolean hasRating3() {
        return this.rating3 != null;
    }

    public Review setRating4(Long rating4) {
        this.rating4 = rating4;
        return this;
    }

    public Long getRating4() {
        return this.rating4;
    }

    public boolean hasRating4() {
        return this.rating4 != null;
    }

    public Review setReviewDate(Long reviewDate) {
        this.reviewDate = reviewDate;
        return this;
    }

    public Long getReviewDate() {
        return this.reviewDate;
    }

    public boolean hasReviewDate() {
        return this.reviewDate != null;
    }

    public Review setReviewFor(Long reviewFor) {
        this.reviewFor = reviewFor;
        return this;
    }

    public Long getReviewFor() {
        return this.reviewFor;
    }

    public boolean hasReviewFor() {
        return this.reviewFor != null;
    }

    public Review setReviewer(Long reviewer) {
        this.reviewer = reviewer;
        return this;
    }

    public Long getReviewer() {
        return this.reviewer;
    }

    public boolean hasReviewer() {
        return this.reviewer != null;
    }

    public Review setText(List<LangString> text) {
        this.text = text;
        return this;
    }

    public List<LangString> getText() {
        return this.text;
    }

    public boolean hasText() {
        return this.text != null;
    }

    public Review setType(List<Long> type) {
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
            case "title": return new HashSet<>(Arrays.asList(String.valueOf(getTitle())));
            case "publisher": return new HashSet<>(Arrays.asList(String.valueOf(getPublisher())));
            case "date": return new HashSet<>(Arrays.asList(String.valueOf(getDate())));
            case "rating1": return new HashSet<>(Arrays.asList(String.valueOf(getRating1())));
            case "rating2": return new HashSet<>(Arrays.asList(String.valueOf(getRating2())));
            case "rating3": return new HashSet<>(Arrays.asList(String.valueOf(getRating3())));
            case "rating4": return new HashSet<>(Arrays.asList(String.valueOf(getRating4())));
            case "reviewDate": return new HashSet<>(Arrays.asList(String.valueOf(getReviewDate())));
            case "reviewFor": return new HashSet<>(Arrays.asList(String.valueOf(getReviewFor())));
            case "reviewer": return new HashSet<>(Arrays.asList(String.valueOf(getReviewer())));
            case "text": return getText().stream().map(e -> String.valueOf(e)).collect(toSet());
            case "type": return getType().stream().map(e -> String.valueOf(e)).collect(toSet());
        }
        return null;
    } 

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Review{");
        sb.append("id=").append(id).append(", ");
        sb.append("title=").append(title).append(", ");
        sb.append("publisher=").append(publisher).append(", ");
        sb.append("date=").append(date).append(", ");
        sb.append("rating1=").append(rating1).append(", ");
        sb.append("rating2=").append(rating2).append(", ");
        sb.append("rating3=").append(rating3).append(", ");
        sb.append("rating4=").append(rating4).append(", ");
        sb.append("reviewDate=").append(reviewDate).append(", ");
        sb.append("reviewFor=").append(reviewFor).append(", ");
        sb.append("reviewer=").append(reviewer).append(", ");
        sb.append("text=").append(text).append(", ");
        sb.append("type=").append(type);
        sb.append("}");
        return sb.toString();
    }


}