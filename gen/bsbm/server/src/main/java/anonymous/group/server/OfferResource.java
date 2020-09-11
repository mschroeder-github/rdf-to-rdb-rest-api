package anonymous.group.server;

import anonymous.group.api.DatabaseController;
import anonymous.group.api.Offer;
import anonymous.group.api.SqlUtils;
import java.util.*;
import org.json.*;
import spark.Request;
import spark.Response;
import spark.Spark;

import cz.jirutka.rsql.parser.ast.Node;

public class OfferResource {

    private DatabaseController dbc;

    public OfferResource(DatabaseController dbc) {
        this.dbc = dbc;

        Spark.get("/offer", this::getOffer);
        Spark.get("/offer/:id", this::getOfferId);

        Spark.delete("/offer", this::deleteOffer);
        Spark.delete("/offer/:id", this::deleteOfferId);

        Spark.post("/offer", this::postOffer);

        Spark.put("/offer/:id", (req, resp) -> putOrPatchOfferId(req, resp, true));
        Spark.patch("/offer/:id", (req, resp) -> putOrPatchOfferId(req, resp, false));

        Spark.options("/offer", this::optionsOffer);
    }

    public Object getOffer(Request req, Response resp) {
        return getOffer(req, resp, null);
    } 

    public Object getOfferId(Request req, Response resp) {
        return getOffer(req, resp, getIds(req));
    }

    private Object getOffer(Request req, Response resp, List<Long> ids) {
        
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

        result.put("type", "Offer");
        
        List<Offer> records = dbc.selectOffer(ids, offset, limit, rqlNode);
        result.put("list", toJSONArray(records));

        resp.type("application/json");
        return result.toString(2);
    }

    public Object postOffer(Request req, Response resp) {
        JSONObject json = toJSONObject(req);
        Offer record = fromJSONObject(json);
        dbc.insertOffer(Arrays.asList(record));
        resp.header("Location", "/offer/" + record.getId());
        resp.status(201);
        return "";
    }

    public Object putOrPatchOfferId(Request req, Response resp, boolean put) {
        List<Long> ids = getIds(req);
        JSONObject json = toJSONObject(req);
        Offer record = fromJSONObject(json);
        record.setId(ids.get(0));
        List<Offer> records = dbc.selectOffer(ids, null, null, null);
        if(records.isEmpty()) {
            resp.status(404);
            return "no Offer with id " + ids.get(0);
        }
        dbc.updateOffer(Arrays.asList(record), put);
        resp.status(204);
        return "";
    }

    public Object deleteOffer(Request req, Response resp) {
        dbc.deleteOffer();
        resp.status(204);
        return "";
    }

    public Object deleteOfferId(Request req, Response resp) {
        dbc.deleteOffer(getIds(req));
        resp.status(204);
        return "";
    }

    public Object optionsOffer(Request req, Response resp) {
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

    public JSONArray toJSONArray(List<Offer> records) {
        JSONArray array = new JSONArray();
        for(Offer record : records) {
            array.put(toJSONObject(record));
        }
        return array;
    }

    public JSONObject toJSONObject(Offer record) {
        JSONObject result = new JSONObject();
        JSONUtils.forceLinkedHashMap(result);
    
        if(record.hasId()) {
            result.put("id", record.getId());
        }
        if(record.hasOfferWebpage()) {
            result.put("offerWebpage", record.getOfferWebpage());
        }
        if(record.hasPrice()) {
            result.put("price", record.getPrice());
        }
        if(record.hasPublisher()) {
            result.put("publisher", record.getPublisher());
        }
        if(record.hasDate()) {
            result.put("date", record.getDate());
        }
        if(record.hasDeliveryDays()) {
            result.put("deliveryDays", record.getDeliveryDays());
        }
        if(record.hasProduct()) {
            result.put("product", record.getProduct());
        }
        if(record.hasValidFrom()) {
            result.put("validFrom", record.getValidFrom());
        }
        if(record.hasValidTo()) {
            result.put("validTo", record.getValidTo());
        }
        if(record.hasVendor()) {
            result.put("vendor", record.getVendor());
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
        result.put("offerWebpage", "String");
        result.put("price", "String");
        result.put("publisher", "String");
        result.put("date", "Long");
        result.put("deliveryDays", "Long");
        result.put("product", "Long");
        result.put("validFrom", "Long");
        result.put("validTo", "Long");
        result.put("vendor", "Long");

        result.put("type", "JSONArray<Long>");

        return result;
    }

    public Offer fromJSONObject(JSONObject jsonObject) {
        Offer record = new Offer();

        record.setOfferWebpage((String) jsonObject.opt("offerWebpage"));
        record.setPrice((String) jsonObject.opt("price"));
        record.setPublisher((String) jsonObject.opt("publisher"));
        record.setDate((Long) jsonObject.opt("date"));
        record.setDeliveryDays((Long) jsonObject.opt("deliveryDays"));
        record.setProduct((Long) jsonObject.opt("product"));
        record.setValidFrom((Long) jsonObject.opt("validFrom"));
        record.setValidTo((Long) jsonObject.opt("validTo"));
        record.setVendor((Long) jsonObject.opt("vendor"));

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