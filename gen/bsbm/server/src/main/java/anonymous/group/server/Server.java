package anonymous.group.server;

import anonymous.group.api.DatabaseController;

import spark.*;
import org.json.*;
import java.io.File;

public class Server {

    public static int PORT = 8081;
    private File dbFile; 

    public Server(String[] args) {
        dbFile = new File("../../bsbm.db");
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

        new OfferResource(dbc);
        new PersonResource(dbc);
        new ProducerResource(dbc);
        new ProductFeatureResource(dbc);
        new ProductResource(dbc);
        new ProductType10Resource(dbc);
        new ProductType11Resource(dbc);
        new ProductType12Resource(dbc);
        new ProductType13Resource(dbc);
        new ProductType14Resource(dbc);
        new ProductType15Resource(dbc);
        new ProductType16Resource(dbc);
        new ProductType17Resource(dbc);
        new ProductType18Resource(dbc);
        new ProductType21Resource(dbc);
        new ProductType6Resource(dbc);
        new ProductType7Resource(dbc);
        new ProductType8Resource(dbc);
        new ProductType9Resource(dbc);
        new ProductTypeResource(dbc);
        new ReviewResource(dbc);
        new VendorResource(dbc);

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
        endpoints.put("/offer");
        endpoints.put("/person");
        endpoints.put("/producer");
        endpoints.put("/product");
        endpoints.put("/productFeature");
        endpoints.put("/productType");
        endpoints.put("/productType10");
        endpoints.put("/productType11");
        endpoints.put("/productType12");
        endpoints.put("/productType13");
        endpoints.put("/productType14");
        endpoints.put("/productType15");
        endpoints.put("/productType16");
        endpoints.put("/productType17");
        endpoints.put("/productType18");
        endpoints.put("/productType21");
        endpoints.put("/productType6");
        endpoints.put("/productType7");
        endpoints.put("/productType8");
        endpoints.put("/productType9");
        endpoints.put("/review");
        endpoints.put("/vendor");
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