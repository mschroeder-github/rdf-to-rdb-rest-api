package anonymous.group.Demo.server;

import java.util.*;
import org.json.*;
import spark.Request;
import spark.Response;
import spark.Spark;
import anonymous.group.Demo.api.*;
import cz.jirutka.rsql.parser.ast.Node;

public class CountryResource {

    private DatabaseController dbc;

    public CountryResource(DatabaseController dbc) {
        this.dbc = dbc;

        Spark.get("/country", this::getCountry);
        Spark.get("/country/:id", this::getCountryId);

        Spark.delete("/country", this::deleteCountry);
        Spark.delete("/country/:id", this::deleteCountryId);

        Spark.post("/country", this::postCountry);

        Spark.put("/country/:id", (req, resp) -> putOrPatchCountryId(req, resp, true));
        Spark.patch("/country/:id", (req, resp) -> putOrPatchCountryId(req, resp, false));

        Spark.options("/country", this::optionsCountry);
    }

    public Object getCountry(Request req, Response resp) {
        return getCountry(req, resp, null);
    } 

    public Object getCountryId(Request req, Response resp) {
        return getCountry(req, resp, getIds(req));
    }

    private Object getCountry(Request req, Response resp, List<Long> ids) {
        
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

        result.put("type", "Country");
        
        List<Country> records = dbc.selectCountry(ids, offset, limit, rqlNode);
        result.put("list", toJSONArray(records));

        resp.type("application/json");
        return result.toString(2);
    }

    public Object postCountry(Request req, Response resp) {
        JSONObject json = toJSONObject(req);
        Country record = fromJSONObject(json);
        dbc.insertCountry(Arrays.asList(record));
        resp.header("Location", "/country/" + record.getId());
        resp.status(201);
        return "";
    }

    public Object putOrPatchCountryId(Request req, Response resp, boolean put) {
        List<Long> ids = getIds(req);
        JSONObject json = toJSONObject(req);
        Country record = fromJSONObject(json);
        record.setId(ids.get(0));
        List<Country> records = dbc.selectCountry(ids, null, null, null);
        if(records.isEmpty()) {
            resp.status(404);
            return "no Country with id " + ids.get(0);
        }
        dbc.updateCountry(Arrays.asList(record), put);
        resp.status(204);
        return "";
    }

    public Object deleteCountry(Request req, Response resp) {
        dbc.deleteCountry();
        resp.status(204);
        return "";
    }

    public Object deleteCountryId(Request req, Response resp) {
        dbc.deleteCountry(getIds(req));
        resp.status(204);
        return "";
    }

    public Object optionsCountry(Request req, Response resp) {
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

    public JSONArray toJSONArray(List<Country> records) {
        JSONArray array = new JSONArray();
        for(Country record : records) {
            array.put(toJSONObject(record));
        }
        return array;
    }

    public JSONObject toJSONObject(Country record) {
        JSONObject result = new JSONObject();
        JSONUtils.forceLinkedHashMap(result);
    
        if(record.hasId()) {
            result.put("id", record.getId());
        }
        if(record.hasHasCapitalCity()) {
            result.put("hasCapitalCity", record.getHasCapitalCity());
        }


        return result;
    }

    public JSONObject toJSONObjectPreview() {
        JSONObject result = new JSONObject();
        JSONUtils.forceLinkedHashMap(result);

        result.put("id", "Long");
        result.put("hasCapitalCity", "Long");


        return result;
    }

    public Country fromJSONObject(JSONObject jsonObject) {
        Country record = new Country();

        record.setHasCapitalCity(to(jsonObject.opt("hasCapitalCity"), Long.class));

        JSONArray array;

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