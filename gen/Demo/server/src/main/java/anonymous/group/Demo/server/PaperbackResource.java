package anonymous.group.Demo.server;

import java.util.*;
import org.json.*;
import spark.Request;
import spark.Response;
import spark.Spark;
import anonymous.group.Demo.api.*;
import cz.jirutka.rsql.parser.ast.Node;

public class PaperbackResource {

    private DatabaseController dbc;

    public PaperbackResource(DatabaseController dbc) {
        this.dbc = dbc;

        Spark.get("/paperback", this::getPaperback);
        Spark.get("/paperback/:id", this::getPaperbackId);

        Spark.delete("/paperback", this::deletePaperback);
        Spark.delete("/paperback/:id", this::deletePaperbackId);

        Spark.post("/paperback", this::postPaperback);

        Spark.put("/paperback/:id", (req, resp) -> putOrPatchPaperbackId(req, resp, true));
        Spark.patch("/paperback/:id", (req, resp) -> putOrPatchPaperbackId(req, resp, false));

        Spark.options("/paperback", this::optionsPaperback);
    }

    public Object getPaperback(Request req, Response resp) {
        return getPaperback(req, resp, null);
    } 

    public Object getPaperbackId(Request req, Response resp) {
        return getPaperback(req, resp, getIds(req));
    }

    private Object getPaperback(Request req, Response resp, List<Long> ids) {
        
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

        result.put("type", "Paperback");
        
        List<Paperback> records = dbc.selectPaperback(ids, offset, limit, rqlNode);
        result.put("list", toJSONArray(records));

        resp.type("application/json");
        return result.toString(2);
    }

    public Object postPaperback(Request req, Response resp) {
        JSONObject json = toJSONObject(req);
        Paperback record = fromJSONObject(json);
        dbc.insertPaperback(Arrays.asList(record));
        resp.header("Location", "/paperback/" + record.getId());
        resp.status(201);
        return "";
    }

    public Object putOrPatchPaperbackId(Request req, Response resp, boolean put) {
        List<Long> ids = getIds(req);
        JSONObject json = toJSONObject(req);
        Paperback record = fromJSONObject(json);
        record.setId(ids.get(0));
        List<Paperback> records = dbc.selectPaperback(ids, null, null, null);
        if(records.isEmpty()) {
            resp.status(404);
            return "no Paperback with id " + ids.get(0);
        }
        dbc.updatePaperback(Arrays.asList(record), put);
        resp.status(204);
        return "";
    }

    public Object deletePaperback(Request req, Response resp) {
        dbc.deletePaperback();
        resp.status(204);
        return "";
    }

    public Object deletePaperbackId(Request req, Response resp) {
        dbc.deletePaperback(getIds(req));
        resp.status(204);
        return "";
    }

    public Object optionsPaperback(Request req, Response resp) {
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

    public JSONArray toJSONArray(List<Paperback> records) {
        JSONArray array = new JSONArray();
        for(Paperback record : records) {
            array.put(toJSONObject(record));
        }
        return array;
    }

    public JSONObject toJSONObject(Paperback record) {
        JSONObject result = new JSONObject();
        JSONUtils.forceLinkedHashMap(result);
    
        if(record.hasId()) {
            result.put("id", record.getId());
        }
        if(record.hasHasCover()) {
            result.put("hasCover", record.getHasCover());
        }
        if(record.hasTakesPlaceIn()) {
            result.put("takesPlaceIn", record.getTakesPlaceIn());
        }


        return result;
    }

    public JSONObject toJSONObjectPreview() {
        JSONObject result = new JSONObject();
        JSONUtils.forceLinkedHashMap(result);

        result.put("id", "Long");
        result.put("hasCover", "String");
        result.put("takesPlaceIn", "Long");


        return result;
    }

    public Paperback fromJSONObject(JSONObject jsonObject) {
        Paperback record = new Paperback();

        record.setHasCover(to(jsonObject.opt("hasCover"), String.class));
        record.setTakesPlaceIn(to(jsonObject.opt("takesPlaceIn"), Long.class));

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