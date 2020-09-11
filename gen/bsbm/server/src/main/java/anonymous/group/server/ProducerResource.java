package anonymous.group.server;

import anonymous.group.api.DatabaseController;
import anonymous.group.api.Producer;
import anonymous.group.api.SqlUtils;
import cz.jirutka.rsql.parser.ast.Node;
import java.util.*;
import org.json.*;
import spark.Request;
import spark.Response;
import spark.Spark;

public class ProducerResource {

    private DatabaseController dbc;

    public ProducerResource(DatabaseController dbc) {
        this.dbc = dbc;

        Spark.get("/producer", this::getProducer);
        Spark.get("/producer/:id", this::getProducerId);

        Spark.delete("/producer", this::deleteProducer);
        Spark.delete("/producer/:id", this::deleteProducerId);

        Spark.post("/producer", this::postProducer);

        Spark.put("/producer/:id", (req, resp) -> putOrPatchProducerId(req, resp, true));
        Spark.patch("/producer/:id", (req, resp) -> putOrPatchProducerId(req, resp, false));

        Spark.options("/producer", this::optionsProducer);
    }

    public Object getProducer(Request req, Response resp) {
        return getProducer(req, resp, null);
    } 

    public Object getProducerId(Request req, Response resp) {
        return getProducer(req, resp, getIds(req));
    }

    private Object getProducer(Request req, Response resp, List<Long> ids) {
        
        String limitStr = req.queryParamOrDefault("limit", null);
        Integer limit = null;
        Integer offset = null;
        if(limitStr != null) {
            limit = Integer.parseInt(limitStr);
            String offsetStr = req.queryParamOrDefault("offset", null);
            offset = offsetStr == null ? null : Integer.parseInt(offsetStr);
        }

        String rqlStr = req.queryParamOrDefault("rql", null);
        Node rqlNode = null;
        if(rqlStr != null) {
            rqlNode = SqlUtils.toRQL(rqlStr);
        }

        JSONObject result = new JSONObject();
        JSONUtils.forceLinkedHashMap(result);

        result.put("type", "Producer");
        
        List<Producer> records = dbc.selectProducer(ids, offset, limit, rqlNode);
        result.put("list", toJSONArray(records));

        resp.type("application/json");
        return result.toString(2);
    }

    public Object postProducer(Request req, Response resp) {
        JSONObject json = toJSONObject(req);
        Producer record = fromJSONObject(json);
        dbc.insertProducer(Arrays.asList(record));
        resp.header("Location", "/producer/" + record.getId());
        resp.status(201);
        return "";
    }

    public Object putOrPatchProducerId(Request req, Response resp, boolean put) {
        List<Long> ids = getIds(req);
        JSONObject json = toJSONObject(req);
        Producer record = fromJSONObject(json);
        record.setId(ids.get(0));
        List<Producer> records = dbc.selectProducer(ids, null, null, null);
        if(records.isEmpty()) {
            resp.status(404);
            return "no Producer with id " + ids.get(0);
        }
        dbc.updateProducer(Arrays.asList(record), put);
        resp.status(204);
        return "";
    }

    public Object deleteProducer(Request req, Response resp) {
        dbc.deleteProducer();
        resp.status(204);
        return "";
    }

    public Object deleteProducerId(Request req, Response resp) {
        dbc.deleteProducer(getIds(req));
        resp.status(204);
        return "";
    }

    public Object optionsProducer(Request req, Response resp) {
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

    public JSONArray toJSONArray(List<Producer> records) {
        JSONArray array = new JSONArray();
        for(Producer record : records) {
            array.put(toJSONObject(record));
        }
        return array;
    }

    public JSONObject toJSONObject(Producer record) {
        JSONObject result = new JSONObject();
        JSONUtils.forceLinkedHashMap(result);
    
        if(record.hasId()) {
            result.put("id", record.getId());
        }
        if(record.hasLabel()) {
            result.put("label", record.getLabel());
        }
        if(record.hasComment()) {
            result.put("comment", record.getComment());
        }
        if(record.hasCountry()) {
            result.put("country", record.getCountry());
        }
        if(record.hasHomepage()) {
            result.put("homepage", record.getHomepage());
        }
        if(record.hasPublisher()) {
            result.put("publisher", record.getPublisher());
        }
        if(record.hasDate()) {
            result.put("date", record.getDate());
        }

        if(record.hasType() && !record.getType().isEmpty()) {
            result.put("type", new JSONArray(record.getType())); 
        }

        return result;
    }

    public JSONObject toJSONObjectPreview() {
        JSONObject result = new JSONObject();
        JSONUtils.forceLinkedHashMap(result);

        result.put("id", "Long");
        result.put("label", "String");
        result.put("comment", "String");
        result.put("country", "String");
        result.put("homepage", "String");
        result.put("publisher", "String");
        result.put("date", "Long");

        result.put("type", "JSONArray<Long>");

        return result;
    }

    public Producer fromJSONObject(JSONObject jsonObject) {
        Producer record = new Producer();

        record.setLabel((String) jsonObject.opt("label"));
        record.setComment((String) jsonObject.opt("comment"));
        record.setCountry((String) jsonObject.opt("country"));
        record.setHomepage((String) jsonObject.opt("homepage"));
        record.setPublisher((String) jsonObject.opt("publisher"));
        record.setDate((Long) jsonObject.opt("date"));

        JSONArray array;
        array = jsonObject.optJSONArray("type");
        if(array != null && !array.isEmpty()) {
            List<Long> list = new ArrayList<>();
            for(int i = 0; i < array.length(); i++) {
                list.add((Long) array.get(i));
            }
            record.setType(list);
        }

        return record;
    }

}