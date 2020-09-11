package ${package};

import java.util.*;
import org.json.*;
import spark.Request;
import spark.Response;
import spark.Spark;
import ${apiClasspath}.*;
import cz.jirutka.rsql.parser.ast.Node;

public class ${name} {

    private DatabaseController dbc;

    public ${name}(DatabaseController dbc) {
        this.dbc = dbc;

        Spark.get("/${javaClass.getEndpointName()}", this::get${javaClass.getName()});
        Spark.get("/${javaClass.getEndpointName()}/:id", this::get${javaClass.getName()}Id);

        Spark.delete("/${javaClass.getEndpointName()}", this::delete${javaClass.getName()});
        Spark.delete("/${javaClass.getEndpointName()}/:id", this::delete${javaClass.getName()}Id);

        Spark.post("/${javaClass.getEndpointName()}", this::post${javaClass.getName()});

        Spark.put("/${javaClass.getEndpointName()}/:id", (req, resp) -> putOrPatch${javaClass.getName()}Id(req, resp, true));
        Spark.patch("/${javaClass.getEndpointName()}/:id", (req, resp) -> putOrPatch${javaClass.getName()}Id(req, resp, false));

        Spark.options("/${javaClass.getEndpointName()}", this::options${javaClass.getName()});
    }

    public Object get${javaClass.getName()}(Request req, Response resp) {
        return get${javaClass.getName()}(req, resp, null);
    } 

    public Object get${javaClass.getName()}Id(Request req, Response resp) {
        return get${javaClass.getName()}(req, resp, getIds(req));
    }

    private Object get${javaClass.getName()}(Request req, Response resp, List<Long> ids) {
        
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

        result.put("type", "${javaClass.getName()}");
        
        List<${javaClass.getName()}> records = dbc.select${javaClass.getName()}(ids, offset, limit, rqlNode);
        result.put("list", toJSONArray(records));

        resp.type("application/json");
        return result.toString(2);
    }

    public Object post${javaClass.getName()}(Request req, Response resp) {
        JSONObject json = toJSONObject(req);
        ${javaClass.getName()} record = fromJSONObject(json);
        dbc.insert${javaClass.getName()}(Arrays.asList(record));
        resp.header("Location", "/${javaClass.getEndpointName()}/" + record.getId());
        resp.status(201);
        return "";
    }

    public Object putOrPatch${javaClass.getName()}Id(Request req, Response resp, boolean put) {
        List<Long> ids = getIds(req);
        JSONObject json = toJSONObject(req);
        ${javaClass.getName()} record = fromJSONObject(json);
        record.setId(ids.get(0));
        List<${javaClass.getName()}> records = dbc.select${javaClass.getName()}(ids, null, null, null);
        if(records.isEmpty()) {
            resp.status(404);
            return "no ${javaClass.getName()} with id " + ids.get(0);
        }
        dbc.update${javaClass.getName()}(Arrays.asList(record), put);
        resp.status(204);
        return "";
    }

    public Object delete${javaClass.getName()}(Request req, Response resp) {
        dbc.delete${javaClass.getName()}();
        resp.status(204);
        return "";
    }

    public Object delete${javaClass.getName()}Id(Request req, Response resp) {
        dbc.delete${javaClass.getName()}(getIds(req));
        resp.status(204);
        return "";
    }

    public Object options${javaClass.getName()}(Request req, Response resp) {
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

    public JSONArray toJSONArray(List<${javaClass.getName()}> records) {
        JSONArray array = new JSONArray();
        for(${javaClass.getName()} record : records) {
            array.put(toJSONObject(record));
        }
        return array;
    }

    public JSONObject toJSONObject(${javaClass.getName()} record) {
        JSONObject result = new JSONObject();
        JSONUtils.forceLinkedHashMap(result);
    
        <#list javaClass.getSingleAttributes() as attr>
        if(record.has${attr.getMethodName()}()) {
            result.put("${attr.getName()}", record.get${attr.getMethodName()}());
        }
        </#list>

        <#list javaClass.getListAttributes() as attr>
        if(record.has${attr.getMethodName()}() && !record.get${attr.getMethodName()}().isEmpty()) {
            result.put("${attr.getName()}", new JSONArray(record.get${attr.getMethodName()}())); <#-- TODO LangString -->
        }
        </#list>

        return result;
    }

    public JSONObject toJSONObjectPreview() {
        JSONObject result = new JSONObject();
        JSONUtils.forceLinkedHashMap(result);

        <#list javaClass.getSingleAttributes() as attr>
        result.put("${attr.getName()}", "${attr.getType()}");
        </#list>

        <#list javaClass.getListAttributes() as attr>
        result.put("${attr.getName()}", "JSONArray<${attr.getTypeSingle()}>");
        </#list>

        return result;
    }

    public ${javaClass.getName()} fromJSONObject(JSONObject jsonObject) {
        ${javaClass.getName()} record = new ${javaClass.getName()}();

        <#list javaClass.getSingleAttributesWithoutId() as attr>
        record.set${attr.getMethodName()}((${attr.getType()}) jsonObject.opt("${attr.getName()}"));
        </#list>

        JSONArray array;
        <#list javaClass.getListAttributes() as attr>
        array = jsonObject.optJSONArray("${attr.getName()}");
        if(array != null && !array.isEmpty()) {
            ${attr.getType()} list = new ArrayList<>();
            for(int i = 0; i < array.length(); i++) {
                list.add((${attr.getTypeSingle()}) array.get(i));
            }
            record.set${attr.getMethodName()}(list);
        }
        </#list>

        return record;
    }

}