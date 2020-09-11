package anonymous.group.server;

import anonymous.group.api.DatabaseController;
import anonymous.group.api.ProductType;
import anonymous.group.api.SqlUtils;
import java.util.*;
import org.json.*;
import spark.Request;
import spark.Response;
import spark.Spark;

import cz.jirutka.rsql.parser.ast.Node;

public class ProductTypeResource {

    private DatabaseController dbc;

    public ProductTypeResource(DatabaseController dbc) {
        this.dbc = dbc;

        Spark.get("/productType", this::getProductType);
        Spark.get("/productType/:id", this::getProductTypeId);

        Spark.delete("/productType", this::deleteProductType);
        Spark.delete("/productType/:id", this::deleteProductTypeId);

        Spark.post("/productType", this::postProductType);

        Spark.put("/productType/:id", (req, resp) -> putOrPatchProductTypeId(req, resp, true));
        Spark.patch("/productType/:id", (req, resp) -> putOrPatchProductTypeId(req, resp, false));

        Spark.options("/productType", this::optionsProductType);
    }

    public Object getProductType(Request req, Response resp) {
        return getProductType(req, resp, null);
    } 

    public Object getProductTypeId(Request req, Response resp) {
        return getProductType(req, resp, getIds(req));
    }

    private Object getProductType(Request req, Response resp, List<Long> ids) {
        
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

        result.put("type", "ProductType");
        
        List<ProductType> records = dbc.selectProductType(ids, offset, limit, rqlNode);
        result.put("list", toJSONArray(records));

        resp.type("application/json");
        return result.toString(2);
    }

    public Object postProductType(Request req, Response resp) {
        JSONObject json = toJSONObject(req);
        ProductType record = fromJSONObject(json);
        dbc.insertProductType(Arrays.asList(record));
        resp.header("Location", "/productType/" + record.getId());
        resp.status(201);
        return "";
    }

    public Object putOrPatchProductTypeId(Request req, Response resp, boolean put) {
        List<Long> ids = getIds(req);
        JSONObject json = toJSONObject(req);
        ProductType record = fromJSONObject(json);
        record.setId(ids.get(0));
        List<ProductType> records = dbc.selectProductType(ids, null, null, null);
        if(records.isEmpty()) {
            resp.status(404);
            return "no ProductType with id " + ids.get(0);
        }
        dbc.updateProductType(Arrays.asList(record), put);
        resp.status(204);
        return "";
    }

    public Object deleteProductType(Request req, Response resp) {
        dbc.deleteProductType();
        resp.status(204);
        return "";
    }

    public Object deleteProductTypeId(Request req, Response resp) {
        dbc.deleteProductType(getIds(req));
        resp.status(204);
        return "";
    }

    public Object optionsProductType(Request req, Response resp) {
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

    public JSONArray toJSONArray(List<ProductType> records) {
        JSONArray array = new JSONArray();
        for(ProductType record : records) {
            array.put(toJSONObject(record));
        }
        return array;
    }

    public JSONObject toJSONObject(ProductType record) {
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
        if(record.hasSubClassOf()) {
            result.put("subClassOf", record.getSubClassOf());
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
        result.put("subClassOf", "Long");

        result.put("type", "JSONArray<Long>");

        return result;
    }

    public ProductType fromJSONObject(JSONObject jsonObject) {
        ProductType record = new ProductType();

        record.setLabel((String) jsonObject.opt("label"));
        record.setComment((String) jsonObject.opt("comment"));
        record.setPublisher((String) jsonObject.opt("publisher"));
        record.setDate((Long) jsonObject.opt("date"));
        record.setSubClassOf((Long) jsonObject.opt("subClassOf"));

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