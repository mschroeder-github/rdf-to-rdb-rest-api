package anonymous.group.server;

import anonymous.group.api.DatabaseController;
import anonymous.group.api.Product;
import anonymous.group.api.SqlUtils;
import java.util.*;
import org.json.*;
import spark.Request;
import spark.Response;
import spark.Spark;

import cz.jirutka.rsql.parser.ast.Node;

public class ProductResource {

    private DatabaseController dbc;

    public ProductResource(DatabaseController dbc) {
        this.dbc = dbc;

        Spark.get("/product", this::getProduct);
        Spark.get("/product/:id", this::getProductId);

        Spark.delete("/product", this::deleteProduct);
        Spark.delete("/product/:id", this::deleteProductId);

        Spark.post("/product", this::postProduct);

        Spark.put("/product/:id", (req, resp) -> putOrPatchProductId(req, resp, true));
        Spark.patch("/product/:id", (req, resp) -> putOrPatchProductId(req, resp, false));

        Spark.options("/product", this::optionsProduct);
    }

    public Object getProduct(Request req, Response resp) {
        return getProduct(req, resp, null);
    } 

    public Object getProductId(Request req, Response resp) {
        return getProduct(req, resp, getIds(req));
    }

    private Object getProduct(Request req, Response resp, List<Long> ids) {
        
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

        result.put("type", "Product");
        
        List<Product> records = dbc.selectProduct(ids, offset, limit, rqlNode);
        result.put("list", toJSONArray(records));

        resp.type("application/json");
        return result.toString(2);
    }

    public Object postProduct(Request req, Response resp) {
        JSONObject json = toJSONObject(req);
        Product record = fromJSONObject(json);
        dbc.insertProduct(Arrays.asList(record));
        resp.header("Location", "/product/" + record.getId());
        resp.status(201);
        return "";
    }

    public Object putOrPatchProductId(Request req, Response resp, boolean put) {
        List<Long> ids = getIds(req);
        JSONObject json = toJSONObject(req);
        Product record = fromJSONObject(json);
        record.setId(ids.get(0));
        List<Product> records = dbc.selectProduct(ids, null, null, null);
        if(records.isEmpty()) {
            resp.status(404);
            return "no Product with id " + ids.get(0);
        }
        dbc.updateProduct(Arrays.asList(record), put);
        resp.status(204);
        return "";
    }

    public Object deleteProduct(Request req, Response resp) {
        dbc.deleteProduct();
        resp.status(204);
        return "";
    }

    public Object deleteProductId(Request req, Response resp) {
        dbc.deleteProduct(getIds(req));
        resp.status(204);
        return "";
    }

    public Object optionsProduct(Request req, Response resp) {
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

    public JSONArray toJSONArray(List<Product> records) {
        JSONArray array = new JSONArray();
        for(Product record : records) {
            array.put(toJSONObject(record));
        }
        return array;
    }

    public JSONObject toJSONObject(Product record) {
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
        if(record.hasProductPropertyTextual1()) {
            result.put("productPropertyTextual1", record.getProductPropertyTextual1());
        }
        if(record.hasProductPropertyTextual2()) {
            result.put("productPropertyTextual2", record.getProductPropertyTextual2());
        }
        if(record.hasProductPropertyTextual3()) {
            result.put("productPropertyTextual3", record.getProductPropertyTextual3());
        }
        if(record.hasProductPropertyTextual4()) {
            result.put("productPropertyTextual4", record.getProductPropertyTextual4());
        }
        if(record.hasProductPropertyTextual5()) {
            result.put("productPropertyTextual5", record.getProductPropertyTextual5());
        }
        if(record.hasProductPropertyTextual6()) {
            result.put("productPropertyTextual6", record.getProductPropertyTextual6());
        }
        if(record.hasPublisher()) {
            result.put("publisher", record.getPublisher());
        }
        if(record.hasDate()) {
            result.put("date", record.getDate());
        }
        if(record.hasProducer()) {
            result.put("producer", record.getProducer());
        }
        if(record.hasProductPropertyNumeric1()) {
            result.put("productPropertyNumeric1", record.getProductPropertyNumeric1());
        }
        if(record.hasProductPropertyNumeric2()) {
            result.put("productPropertyNumeric2", record.getProductPropertyNumeric2());
        }
        if(record.hasProductPropertyNumeric3()) {
            result.put("productPropertyNumeric3", record.getProductPropertyNumeric3());
        }
        if(record.hasProductPropertyNumeric4()) {
            result.put("productPropertyNumeric4", record.getProductPropertyNumeric4());
        }
        if(record.hasProductPropertyNumeric5()) {
            result.put("productPropertyNumeric5", record.getProductPropertyNumeric5());
        }
        if(record.hasProductPropertyNumeric6()) {
            result.put("productPropertyNumeric6", record.getProductPropertyNumeric6());
        }

        if(record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty()) {
            result.put("productFeatureProductFeature", new JSONArray(record.getProductFeatureProductFeature())); 
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
        result.put("productPropertyTextual1", "String");
        result.put("productPropertyTextual2", "String");
        result.put("productPropertyTextual3", "String");
        result.put("productPropertyTextual4", "String");
        result.put("productPropertyTextual5", "String");
        result.put("productPropertyTextual6", "String");
        result.put("publisher", "String");
        result.put("date", "Long");
        result.put("producer", "Long");
        result.put("productPropertyNumeric1", "Long");
        result.put("productPropertyNumeric2", "Long");
        result.put("productPropertyNumeric3", "Long");
        result.put("productPropertyNumeric4", "Long");
        result.put("productPropertyNumeric5", "Long");
        result.put("productPropertyNumeric6", "Long");

        result.put("productFeatureProductFeature", "JSONArray<Long>");
        result.put("type", "JSONArray<Long>");

        return result;
    }

    public Product fromJSONObject(JSONObject jsonObject) {
        Product record = new Product();

        record.setLabel((String) jsonObject.opt("label"));
        record.setComment((String) jsonObject.opt("comment"));
        record.setProductPropertyTextual1((String) jsonObject.opt("productPropertyTextual1"));
        record.setProductPropertyTextual2((String) jsonObject.opt("productPropertyTextual2"));
        record.setProductPropertyTextual3((String) jsonObject.opt("productPropertyTextual3"));
        record.setProductPropertyTextual4((String) jsonObject.opt("productPropertyTextual4"));
        record.setProductPropertyTextual5((String) jsonObject.opt("productPropertyTextual5"));
        record.setProductPropertyTextual6((String) jsonObject.opt("productPropertyTextual6"));
        record.setPublisher((String) jsonObject.opt("publisher"));
        record.setDate((Long) jsonObject.opt("date"));
        record.setProducer((Long) jsonObject.opt("producer"));
        record.setProductPropertyNumeric1((Long) jsonObject.opt("productPropertyNumeric1"));
        record.setProductPropertyNumeric2((Long) jsonObject.opt("productPropertyNumeric2"));
        record.setProductPropertyNumeric3((Long) jsonObject.opt("productPropertyNumeric3"));
        record.setProductPropertyNumeric4((Long) jsonObject.opt("productPropertyNumeric4"));
        record.setProductPropertyNumeric5((Long) jsonObject.opt("productPropertyNumeric5"));
        record.setProductPropertyNumeric6((Long) jsonObject.opt("productPropertyNumeric6"));

        JSONArray array;
        array = jsonObject.optJSONArray("productFeatureProductFeature");
        if(array != null && !array.isEmpty()) {
            List<Long> list = new ArrayList<>();
            for(int i = 0; i < array.length(); i++) {
                list.add((Long) array.get(i));
            }
            record.setProductFeatureProductFeature(list);
        }
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