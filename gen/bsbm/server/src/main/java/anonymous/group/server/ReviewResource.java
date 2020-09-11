package anonymous.group.server;

import anonymous.group.api.DatabaseController;
import anonymous.group.api.LangString;
import anonymous.group.api.Review;
import anonymous.group.api.SqlUtils;
import java.util.*;
import org.json.*;
import spark.Request;
import spark.Response;
import spark.Spark;

import cz.jirutka.rsql.parser.ast.Node;

public class ReviewResource {

    private DatabaseController dbc;

    public ReviewResource(DatabaseController dbc) {
        this.dbc = dbc;

        Spark.get("/review", this::getReview);
        Spark.get("/review/:id", this::getReviewId);

        Spark.delete("/review", this::deleteReview);
        Spark.delete("/review/:id", this::deleteReviewId);

        Spark.post("/review", this::postReview);

        Spark.put("/review/:id", (req, resp) -> putOrPatchReviewId(req, resp, true));
        Spark.patch("/review/:id", (req, resp) -> putOrPatchReviewId(req, resp, false));

        Spark.options("/review", this::optionsReview);
    }

    public Object getReview(Request req, Response resp) {
        return getReview(req, resp, null);
    } 

    public Object getReviewId(Request req, Response resp) {
        return getReview(req, resp, getIds(req));
    }

    private Object getReview(Request req, Response resp, List<Long> ids) {
        
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

        result.put("type", "Review");
        
        List<Review> records = dbc.selectReview(ids, offset, limit, rqlNode);
        result.put("list", toJSONArray(records));

        resp.type("application/json");
        return result.toString(2);
    }

    public Object postReview(Request req, Response resp) {
        JSONObject json = toJSONObject(req);
        Review record = fromJSONObject(json);
        dbc.insertReview(Arrays.asList(record));
        resp.header("Location", "/review/" + record.getId());
        resp.status(201);
        return "";
    }

    public Object putOrPatchReviewId(Request req, Response resp, boolean put) {
        List<Long> ids = getIds(req);
        JSONObject json = toJSONObject(req);
        Review record = fromJSONObject(json);
        record.setId(ids.get(0));
        List<Review> records = dbc.selectReview(ids, null, null, null);
        if(records.isEmpty()) {
            resp.status(404);
            return "no Review with id " + ids.get(0);
        }
        dbc.updateReview(Arrays.asList(record), put);
        resp.status(204);
        return "";
    }

    public Object deleteReview(Request req, Response resp) {
        dbc.deleteReview();
        resp.status(204);
        return "";
    }

    public Object deleteReviewId(Request req, Response resp) {
        dbc.deleteReview(getIds(req));
        resp.status(204);
        return "";
    }

    public Object optionsReview(Request req, Response resp) {
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

    public JSONArray toJSONArray(List<Review> records) {
        JSONArray array = new JSONArray();
        for(Review record : records) {
            array.put(toJSONObject(record));
        }
        return array;
    }

    public JSONObject toJSONObject(Review record) {
        JSONObject result = new JSONObject();
        JSONUtils.forceLinkedHashMap(result);
    
        if(record.hasId()) {
            result.put("id", record.getId());
        }
        if(record.hasTitle()) {
            result.put("title", record.getTitle());
        }
        if(record.hasPublisher()) {
            result.put("publisher", record.getPublisher());
        }
        if(record.hasDate()) {
            result.put("date", record.getDate());
        }
        if(record.hasRating1()) {
            result.put("rating1", record.getRating1());
        }
        if(record.hasRating2()) {
            result.put("rating2", record.getRating2());
        }
        if(record.hasRating3()) {
            result.put("rating3", record.getRating3());
        }
        if(record.hasRating4()) {
            result.put("rating4", record.getRating4());
        }
        if(record.hasReviewDate()) {
            result.put("reviewDate", record.getReviewDate());
        }
        if(record.hasReviewFor()) {
            result.put("reviewFor", record.getReviewFor());
        }
        if(record.hasReviewer()) {
            result.put("reviewer", record.getReviewer());
        }

        if(record.hasText() && !record.getText().isEmpty()) {
            result.put("text", new JSONArray(record.getText())); 
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
        result.put("title", "String");
        result.put("publisher", "String");
        result.put("date", "Long");
        result.put("rating1", "Long");
        result.put("rating2", "Long");
        result.put("rating3", "Long");
        result.put("rating4", "Long");
        result.put("reviewDate", "Long");
        result.put("reviewFor", "Long");
        result.put("reviewer", "Long");

        result.put("text", "JSONArray<LangString>");
        result.put("type", "JSONArray<Long>");

        return result;
    }

    public Review fromJSONObject(JSONObject jsonObject) {
        Review record = new Review();

        record.setTitle((String) jsonObject.opt("title"));
        record.setPublisher((String) jsonObject.opt("publisher"));
        record.setDate((Long) jsonObject.opt("date"));
        record.setRating1((Long) jsonObject.opt("rating1"));
        record.setRating2((Long) jsonObject.opt("rating2"));
        record.setRating3((Long) jsonObject.opt("rating3"));
        record.setRating4((Long) jsonObject.opt("rating4"));
        record.setReviewDate((Long) jsonObject.opt("reviewDate"));
        record.setReviewFor((Long) jsonObject.opt("reviewFor"));
        record.setReviewer((Long) jsonObject.opt("reviewer"));

        JSONArray array;
        array = jsonObject.optJSONArray("text");
        if(array != null && !array.isEmpty()) {
            List<LangString> list = new ArrayList<>();
            for(int i = 0; i < array.length(); i++) {
                list.add((LangString) array.get(i));
            }
            record.setText(list);
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