package anonymous.group.api;

import java.util.*;
import static java.util.stream.Collectors.toSet;

public class Offer {

    private Long id;
    private String offerWebpage;
    private String price;
    private String publisher;
    private Long date;
    private Long deliveryDays;
    private Long product;
    private Long validFrom;
    private Long validTo;
    private Long vendor;
    private List<Long> type;

    public Offer() {
        
    }

    public Offer setId(Long id) {
        this.id = id;
        return this;
    }

    public Long getId() {
        return this.id;
    }

    public boolean hasId() {
        return this.id != null;
    }

    public Offer setOfferWebpage(String offerWebpage) {
        this.offerWebpage = offerWebpage;
        return this;
    }

    public String getOfferWebpage() {
        return this.offerWebpage;
    }

    public boolean hasOfferWebpage() {
        return this.offerWebpage != null;
    }

    public Offer setPrice(String price) {
        this.price = price;
        return this;
    }

    public String getPrice() {
        return this.price;
    }

    public boolean hasPrice() {
        return this.price != null;
    }

    public Offer setPublisher(String publisher) {
        this.publisher = publisher;
        return this;
    }

    public String getPublisher() {
        return this.publisher;
    }

    public boolean hasPublisher() {
        return this.publisher != null;
    }

    public Offer setDate(Long date) {
        this.date = date;
        return this;
    }

    public Long getDate() {
        return this.date;
    }

    public boolean hasDate() {
        return this.date != null;
    }

    public Offer setDeliveryDays(Long deliveryDays) {
        this.deliveryDays = deliveryDays;
        return this;
    }

    public Long getDeliveryDays() {
        return this.deliveryDays;
    }

    public boolean hasDeliveryDays() {
        return this.deliveryDays != null;
    }

    public Offer setProduct(Long product) {
        this.product = product;
        return this;
    }

    public Long getProduct() {
        return this.product;
    }

    public boolean hasProduct() {
        return this.product != null;
    }

    public Offer setValidFrom(Long validFrom) {
        this.validFrom = validFrom;
        return this;
    }

    public Long getValidFrom() {
        return this.validFrom;
    }

    public boolean hasValidFrom() {
        return this.validFrom != null;
    }

    public Offer setValidTo(Long validTo) {
        this.validTo = validTo;
        return this;
    }

    public Long getValidTo() {
        return this.validTo;
    }

    public boolean hasValidTo() {
        return this.validTo != null;
    }

    public Offer setVendor(Long vendor) {
        this.vendor = vendor;
        return this;
    }

    public Long getVendor() {
        return this.vendor;
    }

    public boolean hasVendor() {
        return this.vendor != null;
    }

    public Offer setType(List<Long> type) {
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
            case "offerWebpage": return new HashSet<>(Arrays.asList(String.valueOf(getOfferWebpage())));
            case "price": return new HashSet<>(Arrays.asList(String.valueOf(getPrice())));
            case "publisher": return new HashSet<>(Arrays.asList(String.valueOf(getPublisher())));
            case "date": return new HashSet<>(Arrays.asList(String.valueOf(getDate())));
            case "deliveryDays": return new HashSet<>(Arrays.asList(String.valueOf(getDeliveryDays())));
            case "product": return new HashSet<>(Arrays.asList(String.valueOf(getProduct())));
            case "validFrom": return new HashSet<>(Arrays.asList(String.valueOf(getValidFrom())));
            case "validTo": return new HashSet<>(Arrays.asList(String.valueOf(getValidTo())));
            case "vendor": return new HashSet<>(Arrays.asList(String.valueOf(getVendor())));
            case "type": return getType().stream().map(e -> String.valueOf(e)).collect(toSet());
        }
        return null;
    } 

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Offer{");
        sb.append("id=").append(id).append(", ");
        sb.append("offerWebpage=").append(offerWebpage).append(", ");
        sb.append("price=").append(price).append(", ");
        sb.append("publisher=").append(publisher).append(", ");
        sb.append("date=").append(date).append(", ");
        sb.append("deliveryDays=").append(deliveryDays).append(", ");
        sb.append("product=").append(product).append(", ");
        sb.append("validFrom=").append(validFrom).append(", ");
        sb.append("validTo=").append(validTo).append(", ");
        sb.append("vendor=").append(vendor).append(", ");
        sb.append("type=").append(type);
        sb.append("}");
        return sb.toString();
    }


}