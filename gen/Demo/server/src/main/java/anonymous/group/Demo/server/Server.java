package anonymous.group.Demo.server;

import anonymous.group.Demo.api.*;
import spark.*;
import org.json.*;
import java.io.File;

public class Server {

    public static int PORT = 8081;
    private File dbFile; 

    public Server(String[] args) {
        dbFile = new File("../../Demo.db");
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

        new BookResource(dbc);
        new CapitalCityResource(dbc);
        new CountryResource(dbc);
        new PageResource(dbc);
        new PaperbackResource(dbc);

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
        endpoints.put("/book");
        endpoints.put("/capitalCity");
        endpoints.put("/country");
        endpoints.put("/page");
        endpoints.put("/paperback");
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