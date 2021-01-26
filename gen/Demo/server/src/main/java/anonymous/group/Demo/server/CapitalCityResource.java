package anonymous.group.Demo.server;

import java.util.*;
import org.json.*;
import spark.Request;
import spark.Response;
import spark.Spark;
import anonymous.group.Demo.api.*;
import cz.jirutka.rsql.parser.ast.Node;

public class CapitalCityResource {

    private DatabaseController dbc;

    public CapitalCityResource(DatabaseController dbc) {
        this.dbc = dbc;

        Spark.get("/capitalCity", this::getCapitalCity);
        Spark.get("/capitalCity/:id", this::getCapitalCityId);

        Spark.delete("/capitalCity", this::deleteCapitalCity);
        Spark.delete("/capitalCity/:id", this::deleteCapitalCityId);

        Spark.post("/capitalCity", this::postCapitalCity);

        Spark.put("/capitalCity/:id", (req, resp) -> putOrPatchCapitalCityId(req, resp, true));
        Spark.patch("/capitalCity/:id", (req, resp) -> putOrPatchCapitalCityId(req, resp, false));

        Spark.options("/capitalCity", this::optionsCapitalCity);
    }

    public Object getCapitalCity(Request req, Response resp) {
        return getCapitalCity(req, resp, null);
    } 

    public Object getCapitalCityId(Request req, Response resp) {
        return getCapitalCity(req, resp, getIds(req));
    }

    private Object getCapitalCity(Request req, Response resp, List<Long> ids) {
        
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

        result.put("type", "CapitalCity");
        
        List<CapitalCity> records = dbc.selectCapitalCity(ids, offset, limit, rqlNode);
        result.put("list", toJSONArray(records));

        resp.type("application/json");
        return result.toString(2);
    }

    public Object postCapitalCity(Request req, Response resp) {
        JSONObject json = toJSONObject(req);
        CapitalCity record = fromJSONObject(json);
        dbc.insertCapitalCity(Arrays.asList(record));
        resp.header("Location", "/capitalCity/" + record.getId());
        resp.status(201);
        return "";
    }

    public Object putOrPatchCapitalCityId(Request req, Response resp, boolean put) {
        List<Long> ids = getIds(req);
        JSONObject json = toJSONObject(req);
        CapitalCity record = fromJSONObject(json);
        record.setId(ids.get(0));
        List<CapitalCity> records = dbc.selectCapitalCity(ids, null, null, null);
        if(records.isEmpty()) {
            resp.status(404);
            return "no CapitalCity with id " + ids.get(0);
        }
        dbc.updateCapitalCity(Arrays.asList(record), put);
        resp.status(204);
        return "";
    }

    public Object deleteCapitalCity(Request req, Response resp) {
        dbc.deleteCapitalCity();
        resp.status(204);
        return "";
    }

    public Object deleteCapitalCityId(Request req, Response resp) {
        dbc.deleteCapitalCity(getIds(req));
        resp.status(204);
        return "";
    }

    public Object optionsCapitalCity(Request req, Response resp) {
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

    public JSONArray toJSONArray(List<CapitalCity> records) {
        JSONArray array = new JSONArray();
        for(CapitalCity record : records) {
            array.put(toJSONObject(record));
        }
        return array;
    }

    public JSONObject toJSONObject(CapitalCity record) {
        JSONObject result = new JSONObject();
        JSONUtils.forceLinkedHashMap(result);
    
        if(record.hasId()) {
            result.put("id", record.getId());
        }


        return result;
    }

    public JSONObject toJSONObjectPreview() {
        JSONObject result = new JSONObject();
        JSONUtils.forceLinkedHashMap(result);

        result.put("id", "Long");


        return result;
    }

    public CapitalCity fromJSONObject(JSONObject jsonObject) {
        CapitalCity record = new CapitalCity();


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