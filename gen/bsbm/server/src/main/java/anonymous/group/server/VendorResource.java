package anonymous.group.server;

import anonymous.group.api.DatabaseController;
import anonymous.group.api.SqlUtils;
import anonymous.group.api.Vendor;
import java.util.*;
import org.json.*;
import spark.Request;
import spark.Response;
import spark.Spark;

import cz.jirutka.rsql.parser.ast.Node;

public class VendorResource {

    private DatabaseController dbc;

    public VendorResource(DatabaseController dbc) {
        this.dbc = dbc;

        Spark.get("/vendor", this::getVendor);
        Spark.get("/vendor/:id", this::getVendorId);

        Spark.delete("/vendor", this::deleteVendor);
        Spark.delete("/vendor/:id", this::deleteVendorId);

        Spark.post("/vendor", this::postVendor);

        Spark.put("/vendor/:id", (req, resp) -> putOrPatchVendorId(req, resp, true));
        Spark.patch("/vendor/:id", (req, resp) -> putOrPatchVendorId(req, resp, false));

        Spark.options("/vendor", this::optionsVendor);
    }

    public Object getVendor(Request req, Response resp) {
        return getVendor(req, resp, null);
    } 

    public Object getVendorId(Request req, Response resp) {
        return getVendor(req, resp, getIds(req));
    }

    private Object getVendor(Request req, Response resp, List<Long> ids) {
        
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

        result.put("type", "Vendor");
        
        List<Vendor> records = dbc.selectVendor(ids, offset, limit, rqlNode);
        result.put("list", toJSONArray(records));

        resp.type("application/json");
        return result.toString(2);
    }

    public Object postVendor(Request req, Response resp) {
        JSONObject json = toJSONObject(req);
        Vendor record = fromJSONObject(json);
        dbc.insertVendor(Arrays.asList(record));
        resp.header("Location", "/vendor/" + record.getId());
        resp.status(201);
        return "";
    }

    public Object putOrPatchVendorId(Request req, Response resp, boolean put) {
        List<Long> ids = getIds(req);
        JSONObject json = toJSONObject(req);
        Vendor record = fromJSONObject(json);
        record.setId(ids.get(0));
        List<Vendor> records = dbc.selectVendor(ids, null, null, null);
        if(records.isEmpty()) {
            resp.status(404);
            return "no Vendor with id " + ids.get(0);
        }
        dbc.updateVendor(Arrays.asList(record), put);
        resp.status(204);
        return "";
    }

    public Object deleteVendor(Request req, Response resp) {
        dbc.deleteVendor();
        resp.status(204);
        return "";
    }

    public Object deleteVendorId(Request req, Response resp) {
        dbc.deleteVendor(getIds(req));
        resp.status(204);
        return "";
    }

    public Object optionsVendor(Request req, Response resp) {
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

    public JSONArray toJSONArray(List<Vendor> records) {
        JSONArray array = new JSONArray();
        for(Vendor record : records) {
            array.put(toJSONObject(record));
        }
        return array;
    }

    public JSONObject toJSONObject(Vendor record) {
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

    public Vendor fromJSONObject(JSONObject jsonObject) {
        Vendor record = new Vendor();

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