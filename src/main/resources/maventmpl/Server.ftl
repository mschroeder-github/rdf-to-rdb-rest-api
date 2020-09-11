package ${package};

import ${apiClasspath}.*;
import spark.*;
import org.json.*;
import java.io.File;

public class Server {

    public static int PORT = ${defaultPort};
    private File dbFile; 

    public Server(String[] args) {
        dbFile = new File("${databaseFilePath}");
        if(args.length > 0) {
            dbFile = new File(args[0]);
        }
        if(args.length > 1) {
            PORT = Integer.parseInt(args[1]);
        }
    }

    public void start() {
        System.out.println("Startup Server");
        
        Spark.port(PORT);

        Spark.exception(Exception.class, (exception, request, response) -> {
            exception.printStackTrace();
            response.body(exception.getMessage());
        });

        //if ends with '/' redirect to path without '/'
        Spark.before((req, res) -> {
            String path = req.pathInfo();
            if (!path.equals("/") && path.endsWith("/")) {
                res.redirect(path.substring(0, path.length() - 1));
            }
        });

        Spark.awaitInitialization();
    }

    public void endpoints() {
        System.out.println("Open database");
        DatabaseController dbc = new DatabaseController(dbFile);
        dbc.open();

        <#list endpointClasses as endpointClass>
        new ${endpointClass}(dbc);
        </#list>

        Spark.options("/", this::optionsRoot);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() { 
                System.out.println("Close database");
                dbc.close();
            } 
        });
    }

    private Object optionsRoot(Request req, Response resp) {
        JSONObject json = new JSONObject();

        JSONArray endpoints = new JSONArray();
        <#list javaClasses as javaClass>
        endpoints.put("/${javaClass.getEndpointName()}");
        </#list>
        json.put("endpoints", endpoints);

        resp.header("Allow", "OPTIONS");
        resp.type("application/json");
        return json.toString(2);
    }

    public static void main(String[] args) {
        Server server = new Server(args);
        server.start();
        server.endpoints();

        System.out.println("Server is running at port " + PORT);
    }
}