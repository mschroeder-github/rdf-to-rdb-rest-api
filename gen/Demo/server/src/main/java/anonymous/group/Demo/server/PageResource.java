package anonymous.group.Demo.server;

import java.util.*;
import org.json.*;
import spark.Request;
import spark.Response;
import spark.Spark;
import anonymous.group.Demo.api.*;
import cz.jirutka.rsql.parser.ast.Node;

public class PageResource {

    private DatabaseController dbc;

    public PageResource(DatabaseController dbc) {
        this.dbc = dbc;

        Spark.get("/page", this::getPage);
        Spark.get("/page/:id", this::getPageId);

        Spark.delete("/page", this::deletePage);
        Spark.delete("/page/:id", this::deletePageId);

        Spark.post("/page", this::postPage);

        Spark.put("/page/:id", (req, resp) -> putOrPatchPageId(req, resp, true));
        Spark.patch("/page/:id", (req, resp) -> putOrPatchPageId(req, resp, false));

        Spark.options("/page", this::optionsPage);
    }

    public Object getPage(Request req, Response resp) {
        return getPage(req, resp, null);
    } 

    public Object getPageId(Request req, Response resp) {
        return getPage(req, resp, getIds(req));
    }

    private Object getPage(Request req, Response resp, List<Long> ids) {
        
        String limitStr = req.queryParamOrDefault("limit", null);
        Integer limit = limitStr == null ? null : Integer.parseInt(limitStr);

        String offsetStr = req.queryParamOrDefault("offset", null);
        Integer offset = offsetStr == null ? null : Integer.parseInt(offsetStr);

        String rqlStr = req.queryParamOrDefault("rql", null);
        Node rqlNode = null;
        if(rqlStr != null) {
            rqlNode = SqlUtils.toRQL(rqlStr);
        }

        JSONObject result = new JSONObject();
        JSONUtils.forceLinkedHashMap(result);

        result.put("type", "Page");
        
        List<Page> records = dbc.selectPage(ids, offset, limit, rqlNode);
        result.put("list", toJSONArray(records));

        resp.type("application/json");
        return result.toString(2);
    }

    public Object postPage(Request req, Response resp) {
        JSONObject json = toJSONObject(req);
        Page record = fromJSONObject(json);
        dbc.insertPage(Arrays.asList(record));
        resp.header("Location", "/page/" + record.getId());
        resp.status(201);
        return "";
    }

    public Object putOrPatchPageId(Request req, Response resp, boolean put) {
        List<Long> ids = getIds(req);
        JSONObject json = toJSONObject(req);
        Page record = fromJSONObject(json);
        record.setId(ids.get(0));
        List<Page> records = dbc.selectPage(ids, null, null, null);
        if(records.isEmpty()) {
            resp.status(404);
            return "no Page with id " + ids.get(0);
        }
        dbc.updatePage(Arrays.asList(record), put);
        resp.status(204);
        return "";
    }

    public Object deletePage(Request req, Response resp) {
        dbc.deletePage();
        resp.status(204);
        return "";
    }

    public Object deletePageId(Request req, Response resp) {
        dbc.deletePage(getIds(req));
        resp.status(204);
        return "";
    }

    public Object optionsPage(Request req, Response resp) {
        JSONObject preview = toJSONObjectPreview();
        resp.header("Allow", "GET,POST,PUT,PATCH,DELETE,OPTIONS");
        resp.type("application/json");
        return preview.toString(2);
    }

    private List<Long> getIds(Request req) {
        List<Long> ids = new ArrayList<>();
        for(String idstr : req.params("id").split(",")) {
            try {
                ids.add(Long.parseLong(idstr));
            } catch(Exception e) {
                throw new RuntimeException("id must be a number", e);
            } 
        }
        if(ids.isEmpty()) {
            throw new RuntimeException("no id is given");
        }
        return ids;
    }

    private JSONObject toJSONObject(Request req) {
        try {
            return new JSONObject(req.body());
        } catch(Exception e) {
            throw new RuntimeException("request could not be parsed to JSON", e);
        } 
    }

    public JSONArray toJSONArray(List<Page> records) {
        JSONArray array = new JSONArray();
        for(Page record : records) {
            array.put(toJSONObject(record));
        }
        return array;
    }

    public JSONObject toJSONObject(Page record) {
        JSONObject result = new JSONObject();
        JSONUtils.forceLinkedHashMap(result);
    
        if(record.hasId()) {
            result.put("id", record.getId());
        }
        if(record.hasBookHasPage()) {
            result.put("bookHasPage", record.getBookHasPage());
        }
        if(record.hasPaperbackHasPage()) {
            result.put("paperbackHasPage", record.getPaperbackHasPage());
        }

        if(record.hasMentionsCapitalCity() && !record.getMentionsCapitalCity().isEmpty()) {
            result.put("mentionsCapitalCity", new JSONArray(record.getMentionsCapitalCity())); 
        }

        return result;
    }

    public JSONObject toJSONObjectPreview() {
        JSONObject result = new JSONObject();
        JSONUtils.forceLinkedHashMap(result);

        result.put("id", "Long");
        result.put("bookHasPage", "Long");
        result.put("paperbackHasPage", "Long");

        result.put("mentionsCapitalCity", "JSONArray<Long>");

        return result;
    }

    public Page fromJSONObject(JSONObject jsonObject) {
        Page record = new Page();

        record.setBookHasPage(to(jsonObject.opt("bookHasPage"), Long.class));
        record.setPaperbackHasPage(to(jsonObject.opt("paperbackHasPage"), Long.class));

        JSONArray array;
        array = jsonObject.optJSONArray("mentionsCapitalCity");
        if(array != null && !array.isEmpty()) {
            List<Long> list = new ArrayList<>();
            for(int i = 0; i < array.length(); i++) {
                list.add(to(array.get(i), Long.class));
            }
            record.setMentionsCapitalCity(list);
        }

        return record;
    }

    private <T> T to(Object obj, Class<T> type) {
        if(obj == null)
            return (T) obj;

        if(obj instanceof Integer && type.equals(Long.class)) {
            Integer i = (Integer) obj;
            return (T) Long.valueOf(i.longValue());
}
        return (T) obj;
    }

}