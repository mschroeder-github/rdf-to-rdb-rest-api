package anonymous.group.server;

import anonymous.group.api.DatabaseController;
import anonymous.group.api.Person;
import anonymous.group.api.SqlUtils;
import java.util.*;
import org.json.*;
import spark.Request;
import spark.Response;
import spark.Spark;

import cz.jirutka.rsql.parser.ast.Node;

public class PersonResource {

    private DatabaseController dbc;

    public PersonResource(DatabaseController dbc) {
        this.dbc = dbc;

        Spark.get("/person", this::getPerson);
        Spark.get("/person/:id", this::getPersonId);

        Spark.delete("/person", this::deletePerson);
        Spark.delete("/person/:id", this::deletePersonId);

        Spark.post("/person", this::postPerson);

        Spark.put("/person/:id", (req, resp) -> putOrPatchPersonId(req, resp, true));
        Spark.patch("/person/:id", (req, resp) -> putOrPatchPersonId(req, resp, false));

        Spark.options("/person", this::optionsPerson);
    }

    public Object getPerson(Request req, Response resp) {
        return getPerson(req, resp, null);
    } 

    public Object getPersonId(Request req, Response resp) {
        return getPerson(req, resp, getIds(req));
    }

    private Object getPerson(Request req, Response resp, List<Long> ids) {
        
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

        result.put("type", "Person");
        
        List<Person> records = dbc.selectPerson(ids, offset, limit, rqlNode);
        result.put("list", toJSONArray(records));

        resp.type("application/json");
        return result.toString(2);
    }

    public Object postPerson(Request req, Response resp) {
        JSONObject json = toJSONObject(req);
        Person record = fromJSONObject(json);
        dbc.insertPerson(Arrays.asList(record));
        resp.header("Location", "/person/" + record.getId());
        resp.status(201);
        return "";
    }

    public Object putOrPatchPersonId(Request req, Response resp, boolean put) {
        List<Long> ids = getIds(req);
        JSONObject json = toJSONObject(req);
        Person record = fromJSONObject(json);
        record.setId(ids.get(0));
        List<Person> records = dbc.selectPerson(ids, null, null, null);
        if(records.isEmpty()) {
            resp.status(404);
            return "no Person with id " + ids.get(0);
        }
        dbc.updatePerson(Arrays.asList(record), put);
        resp.status(204);
        return "";
    }

    public Object deletePerson(Request req, Response resp) {
        dbc.deletePerson();
        resp.status(204);
        return "";
    }

    public Object deletePersonId(Request req, Response resp) {
        dbc.deletePerson(getIds(req));
        resp.status(204);
        return "";
    }

    public Object optionsPerson(Request req, Response resp) {
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

    public JSONArray toJSONArray(List<Person> records) {
        JSONArray array = new JSONArray();
        for(Person record : records) {
            array.put(toJSONObject(record));
        }
        return array;
    }

    public JSONObject toJSONObject(Person record) {
        JSONObject result = new JSONObject();
        JSONUtils.forceLinkedHashMap(result);
    
        if(record.hasId()) {
            result.put("id", record.getId());
        }
        if(record.hasName()) {
            result.put("name", record.getName());
        }
        if(record.hasCountry()) {
            result.put("country", record.getCountry());
        }
        if(record.hasMboxsha1sum()) {
            result.put("mboxsha1sum", record.getMboxsha1sum());
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
        result.put("name", "String");
        result.put("country", "String");
        result.put("mboxsha1sum", "String");
        result.put("publisher", "String");
        result.put("date", "Long");

        result.put("type", "JSONArray<Long>");

        return result;
    }

    public Person fromJSONObject(JSONObject jsonObject) {
        Person record = new Person();

        record.setName((String) jsonObject.opt("name"));
        record.setCountry((String) jsonObject.opt("country"));
        record.setMboxsha1sum((String) jsonObject.opt("mboxsha1sum"));
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