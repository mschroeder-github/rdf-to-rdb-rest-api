package anonymous.group.server;

import anonymous.group.api.DatabaseController;
import anonymous.group.api.ProductFeature;
import anonymous.group.api.SqlUtils;
import java.util.*;
import org.json.*;
import spark.Request;
import spark.Response;
import spark.Spark;

import cz.jirutka.rsql.parser.ast.Node;

public class ProductFeatureResource {

    private DatabaseController dbc;

    public ProductFeatureResource(DatabaseController dbc) {
        this.dbc = dbc;

        Spark.get("/productFeature", this::getProductFeature);
        Spark.get("/productFeature/:id", this::getProductFeatureId);

        Spark.delete("/productFeature", this::deleteProductFeature);
        Spark.delete("/productFeature/:id", this::deleteProductFeatureId);

        Spark.post("/productFeature", this::postProductFeature);

        Spark.put("/productFeature/:id", (req, resp) -> putOrPatchProductFeatureId(req, resp, true));
        Spark.patch("/productFeature/:id", (req, resp) -> putOrPatchProductFeatureId(req, resp, false));

        Spark.options("/productFeature", this::optionsProductFeature);
    }

    public Object getProductFeature(Request req, Response resp) {
        return getProductFeature(req, resp, null);
    } 

    public Object getProductFeatureId(Request req, Response resp) {
        return getProductFeature(req, resp, getIds(req));
    }

    private Object getProductFeature(Request req, Response resp, List<Long> ids) {
        
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

        result.put("type", "ProductFeature");
        
        List<ProductFeature> records = dbc.selectProductFeature(ids, offset, limit, rqlNode);
        result.put("list", toJSONArray(records));

        resp.type("application/json");
        return result.toString(2);
    }

    public Object postProductFeature(Request req, Response resp) {
        JSONObject json = toJSONObject(req);
        ProductFeature record = fromJSONObject(json);
        dbc.insertProductFeature(Arrays.asList(record));
        resp.header("Location", "/productFeature/" + record.getId());
        resp.status(201);
        return "";
    }

    public Object putOrPatchProductFeatureId(Request req, Response resp, boolean put) {
        List<Long> ids = getIds(req);
        JSONObject json = toJSONObject(req);
        ProductFeature record = fromJSONObject(json);
        record.setId(ids.get(0));
        List<ProductFeature> records = dbc.selectProductFeature(ids, null, null, null);
        if(records.isEmpty()) {
            resp.status(404);
            return "no ProductFeature with id " + ids.get(0);
        }
        dbc.updateProductFeature(Arrays.asList(record), put);
        resp.status(204);
        return "";
    }

    public Object deleteProductFeature(Request req, Response resp) {
        dbc.deleteProductFeature();
        resp.status(204);
        return "";
    }

    public Object deleteProductFeatureId(Request req, Response resp) {
        dbc.deleteProductFeature(getIds(req));
        resp.status(204);
        return "";
    }

    public Object optionsProductFeature(Request req, Response resp) {
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

    public JSONArray toJSONArray(List<ProductFeature> records) {
        JSONArray array = new JSONArray();
        for(ProductFeature record : records) {
            array.put(toJSONObject(record));
        }
        return array;
    }

    public JSONObject toJSONObject(ProductFeature record) {
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
        result.put("publisher", "String");
        result.put("date", "Long");

        result.put("type", "JSONArray<Long>");

        return result;
    }

    public ProductFeature fromJSONObject(JSONObject jsonObject) {
        ProductFeature record = new ProductFeature();

        record.setLabel((String) jsonObject.opt("label"));
        record.setComment((String) jsonObject.opt("comment"));
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