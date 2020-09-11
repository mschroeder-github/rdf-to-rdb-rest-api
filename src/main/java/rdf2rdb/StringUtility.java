package rdf2rdb;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 *

 */
public class StringUtility {
    
    public static String splitCamelCaseString(String s) {
        return splitCamelCaseString(s, " ");
    }
    
    public static String splitCamelCaseString(String s, String separator) {
        LinkedList<String> result = new LinkedList<String>();
        for (String w : s.split("(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])")) {
            result.add(w);
        }
        return result.stream().collect(Collectors.joining(" ")).replaceAll("\\s+", separator);
    }

    //https://stackoverflow.com/a/14424783
    public static String encodeURIComponent(String s) {
        String result;

        try {
            result = URLEncoder.encode(s, "UTF-8")
                    .replaceAll("\\+", "%20")
                    .replaceAll("\\%21", "!")
                    .replaceAll("\\%27", "'")
                    .replaceAll("\\%28", "(")
                    .replaceAll("\\%29", ")")
                    .replaceAll("\\%7E", "~");
        } catch (UnsupportedEncodingException e) {
            result = s;
        }

        return result;
    }

    public static String decodeURIComponent(String s) {
        String result;

        try {
            result = URLDecoder.decode(s, "UTF-8");
            //TODO
            //.replaceAll("\\+", "%20")
            //.replaceAll("\\%21", "!")
            //.replaceAll("\\%27", "'")
            //.replaceAll("\\%28", "(")
            //.replaceAll("\\%29", ")")
            //.replaceAll("\\%7E", "~");
        } catch (UnsupportedEncodingException e) {
            result = s;
        }

        return result;
    }
    
    public static String makeWhitespaceVisible(String s) {
        if(s == null)
            return null;
        
        if(s.isEmpty())
            return "";
        
        return s.replace(" ", "␣").replace("\n", "\\n").replace("\t", "\\t").replace("\r", "\\r");
    }
    
    /**
     * Returns http://.../[localname] or http://...#[localname] of URI.
     * @param uri
     * @return 
     */
    public static String getLocalName(String uri) {
        int a = uri.lastIndexOf("/");
        int b = uri.lastIndexOf("#");
        int m = Math.max(a, b);
        if(m == -1) {
            //schema:name
            if(uri.contains(":")) {
                String[] split = uri.split("\\:");
                if(split.length == 2)
                    return split[1];
            }
            
            return null;
        }
        
        return uri.substring(m+1, uri.length());
    }
    
    public static List<String> getSymbols() {
        List<String> l = new ArrayList<>();
        for(int i = 33; i <= 126; i++) {
            char c = (char)i;
            if(!(Character.isLetter(i) || Character.isDigit(c))) {
                l.add(String.valueOf(c));
            }
        }
        return l;
    }
    
    public static String replaceSymbols(String str, String replaceWith) {
        for(String symbol : getSymbols()) {
            str = str.replace(symbol, replaceWith);
        }
        return str;
    }
    
    /**
     * Converts umlaut to ascii, e.g. ä to ae.
     * @param text
     * @return 
     */
    public static String umlautConversion(String text) {
        return text
                .replace("Ü", "Ue")
                .replace("Ö", "Oe")
                .replace("Ä", "Ae")
                .replace("ü", "ue")
                .replace("ö", "oe")
                .replace("ä", "ae")
                .replace("ß", "ss")
                .replace("ì", "i")
                .replace("à", "a")
                .replace("è", "e")
                .replace("ò", "o")
                .replace("ù", "u")
                ;
    }
    
    public static String toProperCase(String s) {
        if (s == null) {
            return null;
        }

        if (s.length() == 0) {
            return s;
        }

        if (s.length() == 1) {
            return s.toUpperCase();
        }

        return s.substring(0, 1).toUpperCase()
                + s.substring(1).toLowerCase();
    }
}
