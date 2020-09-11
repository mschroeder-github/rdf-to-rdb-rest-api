package anonymous.group.api;

public class LangString {
    
    private String string;
    private String lang;

    public LangString(String string, String lang) {
        this.string = string;
        this.lang = lang;
    }

    public String getString() {
        return this.string;
    }

    public String getLang() {
        return this.lang;
    }

    public boolean hasLang() {
        return this.lang != null;
    }

    @Override
    public String toString() {
        return getString() + "@" + getLang();
    }

}
