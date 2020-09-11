package rdf2rdb;

import anonymous.group.api.DatabaseController;
import anonymous.group.server.Server;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.io.IOUtils;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.json.JSONObject;

/**
 * Idea is to check all 12 explore query if it is possible to query them also with CRUD API.

 */
public class BsbmEvaluator {
    
    private Model model;
    private Map<String, Long> res2id;
    
    public BsbmEvaluator(File modelFile) throws IOException {
        FileInputStream fis = new FileInputStream(modelFile);
        model = ModelFactory.createDefaultModel().read(fis, null, "TTL");
        fis.close();
        System.out.println(model.size() + " BSBM triples");
        
        Server bsbmServer = new Server(new String[] { "gen/bsbm.db", "8081" });
        bsbmServer.start();
        bsbmServer.endpoints();
        System.out.println("BSBM Server at port " + Server.PORT);
        
        DatabaseController dbc = new DatabaseController(new File("gen/bsbm.db"));
        dbc.open();
        res2id = dbc.getResourceIdMap();
        System.out.println(res2id.size() + " resources in res2id map");
        dbc.close();
    }
    
    public void queries() throws IOException {
        LatexTable latexTable = new LatexTable();
        latexTable.addColumn("Nr.", "Number", "c", true);
        latexTable.addColumn("Request", "", "l", true);
        
        StringBuilder console = new StringBuilder();
        console.append("\n");
        
        for(BsbmQuery query : queries) {
            console.append("\n");
            console.append("Query " + query.nr);
            
            //Query
            query.init();
            //query.runCheck();
            query.runQuery();
            
            //REST API
            for(String url : query.urlsReplaced(model)) {
                JSONObject result = REST.getObject("http://localhost:" + Server.PORT + url);
                
                System.out.println(url);
                System.out.println(result.toString(2));
                
                console.append(url + "\n");
                console.append(result.toString(2) + "\n");
            }
            
            if(!query.urls().isEmpty()) {
                latexTable.addRow(
                        query.nr,
                        query.urls().get(0)
                );
            }
            
            console.append("\n");
            console.append("==========================" + "\n");
            console.append("\n");
        }
        
        System.out.println(console);
        System.out.println(latexTable);
    }
    
    private abstract class BsbmQuery {
        
        private String queryOriginal;
        private String query;
        private String queryCheck;
        private int nr;
        
        public void loadQuery(int nr) {
            this.nr = nr;
            try {
                queryOriginal = IOUtils.toString(BsbmEvaluator.class.getResourceAsStream("/bsbmqueries/query" + nr + ".txt"), StandardCharsets.UTF_8);
                queryCheck = IOUtils.toString(BsbmEvaluator.class.getResourceAsStream("/bsbmqueries/query" + nr + "check.txt"), StandardCharsets.UTF_8);
            } catch (IOException ex) {
                System.err.println(ex.getMessage());
                //throw new RuntimeException(ex);
            }
            
            query = queryOriginal;
            for(Entry<String, String> e : queryParams().entrySet()) {
                query = query.replace("%" + e.getKey() + "%", e.getValue());
            }
        }
        
        public void runCheck() {
            System.out.println("Check for Query " + nr);
            System.out.println(queryCheck);
            QueryExecution qe = QueryExecutionFactory.create(queryCheck, model);
            ResultSetFormatter.out(qe.execSelect());
        }
        
        public void runQuery() {
            System.out.println("Query " + nr);
            System.out.println(query);
            QueryExecution qe = QueryExecutionFactory.create(query, model);
            
            if(nr == 12) {
                Model m = qe.execConstruct();
                StringWriter sw = new StringWriter();
                m.write(sw, "TTL");
                System.out.println(sw);
            } else if(nr == 9) {
                Model m = qe.execDescribe();
                StringWriter sw = new StringWriter();
                m.write(sw, "TTL");
                System.out.println(sw);
            } else {
                ResultSetFormatter.out(qe.execSelect());
            }
        }
        
        public abstract Map<String, String> queryParams();
        
        public abstract void init();
        
        public abstract List<String> urls();
        
        public List<String> urlsReplaced(Model model) {
            List<String> result = new ArrayList<>();
            for(String url : urls()) {
                for(Entry<String, String> e : queryParams().entrySet()) {
                    String val = e.getValue();
                    if(val.startsWith("<") && val.endsWith(">")) {
                        val = val.substring(1, val.length()-1);
                    }
                    
                    //\"2000-05-28T00:00:00\"^^xsd:dateTime
                    if(val.endsWith("^^xsd:dateTime")) {
                        val = val.substring(1, val.length() - "\"^^xsd:dateTime".length());
                        long epochMillis = LocalDateTime.parse(val).toInstant(ZoneOffset.UTC).toEpochMilli();
                        val = String.valueOf(epochMillis);
                    } else {
                        val = model.expandPrefix(val);
                        Long id = res2id.get(val);
                        if(id != null) {
                            val = String.valueOf(id);
                        }
                    }
                    
                    url = url.replace("%" + e.getKey() + "%", val);
                }
                result.add(url);
            }
            return result;
        }
        
    }
    
    //just products of a type filtered with numeric property
    //I took ProductType6, ProductFeature11, ProductFeature14, x=50
    private BsbmQuery query1 = new BsbmQuery() {
        
        @Override
        public void init() {
            loadQuery(1);
        }
        
        @Override
        public Map<String, String> queryParams() {
            Map<String, String> m = new HashMap<>();
            m.put("ProductType", "bsbm-inst:ProductType6");
            m.put("ProductFeature1", "bsbm-inst:ProductFeature11");
            m.put("ProductFeature2", "bsbm-inst:ProductFeature14");
            m.put("x", "50");
            return m;
        }

        @Override
        public List<String> urls() {
            return Arrays.asList(
                    "/product?rql=type=in=%ProductType%;productFeatureProductFeature=in=(%ProductFeature1%,%ProductFeature2%);productPropertyNumeric1>%x%"
            );
        }
        
    };
    //infos about a product: I took /dataFromProducer1/Product55
    //label resolve via extra calls
    private BsbmQuery query2 = new BsbmQuery() {
        
        @Override
        public void init() {
            loadQuery(2);
        }
        
        @Override
        public Map<String, String> queryParams() {
            Map<String, String> m = new HashMap<>();
            m.put("ProductXYZ", "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product55>");
            return m;
        }

        @Override
        public List<String> urls() {
            return Arrays.asList(
                    "/product/%ProductXYZ%"
            );
        }
        
    };
    //products with certain numerical properties and an optional feature having a label
    //need a feature that is not part of the product thus will be unbound
    private BsbmQuery query3 = new BsbmQuery() {
        
        @Override
        public void init() {
            loadQuery(3);
        }
        
        @Override
        public Map<String, String> queryParams() {
            Map<String, String> m = new HashMap<>();
            m.put("ProductType", "bsbm-inst:ProductType6");
            m.put("ProductFeature1", "bsbm-inst:ProductFeature11");
            m.put("ProductFeature2", "bsbm-inst:ProductFeature14"); //because Product95 does not have a feature 14
            m.put("x", "30"); //58
            m.put("y", "1000"); //838
            return m;
        }

        @Override
        public List<String> urls() {
            return Arrays.asList(
                    "/product?rql=type=in=(%ProductType%);productFeatureProductFeature=in=(%ProductFeature1%);productFeatureProductFeature=out=(%ProductFeature2%);productPropertyNumeric1>%x%;productPropertyNumeric3<%y%"
            );
        }
    };
    //product with features or products with other features (uses UNION)
    //because of UNION make two queries
    //offset changed to 0 (was 5)
    private BsbmQuery query4 = new BsbmQuery() {
        
        @Override
        public void init() {
            loadQuery(4);
        }
        
        @Override
        public Map<String, String> queryParams() {
            Map<String, String> m = new HashMap<>();
            m.put("ProductType", "bsbm-inst:ProductType6");
            m.put("ProductFeature1", "bsbm-inst:ProductFeature11");
            m.put("ProductFeature2", "bsbm-inst:ProductFeature14"); //ProductFeature16 ProductFeature185 ProductFeature19
            m.put("ProductFeature3", "bsbm-inst:ProductFeature16");
            m.put("x", "50"); //p1 = 58
            m.put("y", "300"); //p2 = 365
            return m;
        }

        @Override
        public List<String> urls() {
            return Arrays.asList(
                    "/product?rql=type=in=(%ProductType%);productFeatureProductFeature=in=(%ProductFeature1%);(productFeatureProductFeature=in=(%ProductFeature2%),productFeatureProductFeature=in=(%ProductFeature3%));productPropertyNumeric1>%x%;productPropertyNumeric2>%y%"
            );
        }
    };
    //search from a given product another product that has comparable numeric properties
    //filter can not be stated with RQL because of missing arithmetic
    private BsbmQuery query5 = new BsbmQuery() {
        
        @Override
        public void init() {
            loadQuery(5);
        }
        
        @Override
        public Map<String, String> queryParams() {
            Map<String, String> m = new HashMap<>();
            m.put("ProductXYZ", "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product20>");
            return m;
        }

        @Override
        public List<String> urls() {
            return Arrays.asList(
                    "/product/%ProductXYZ%",
                    "/product?rql=id=out=(%ProductXYZ%)"
            );
        }
    };
    //regex case
    private BsbmQuery query6 = new BsbmQuery() {
        
        @Override
        public void init() {
            loadQuery(6);
        }
        
        @Override
        public Map<String, String> queryParams() {
            Map<String, String> m = new HashMap<>();
            m.put("word1", "absolvers");
            return m;
        }

        @Override
        public List<String> urls() {
            return Arrays.asList(
                    "/product?rql=label=regex=%word1%"
            );
        }
    };
    //product, optional offer, filtered by date, optional review and rating
    //no date filter possible because no offer matches
    private BsbmQuery query7 = new BsbmQuery() {
        
        @Override
        public void init() {
            loadQuery(7);
        }
        
        @Override
        public Map<String, String> queryParams() {
            Map<String, String> m = new HashMap<>();
            m.put("ProductXYZ", "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product76>");
            m.put("currentDate", "\"2000-05-28T00:00:00\"^^xsd:dateTime");
            return m;
        }

        @Override
        public List<String> urls() {
            return Arrays.asList(
                    "/product/%ProductXYZ%",
                    "/offer?rql=product==%ProductXYZ%;validTo>%currentDate%",
                    "/review?rql=reviewFor==%ProductXYZ%"
            );
        }
    };
    //for lang string need =lang= somehow
    //TODO allow single lang string?
    private BsbmQuery query8 = new BsbmQuery() {
        
        @Override
        public void init() {
            loadQuery(8);
        }
        
        @Override
        public Map<String, String> queryParams() {
            Map<String, String> m = new HashMap<>();
            m.put("ProductXYZ", "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product59>");
            return m;
        }

        @Override
        public List<String> urls() {
            return Arrays.asList(
                    "/review?rql=reviewFor==%ProductXYZ%;text=lang=en"
                    ///person/%reviewerXYZ% to get name of reviewers
            );
        }
    };
    //DESCRIBE a reviewer of a review
    //returns single result RDF graph (like construct)
    private BsbmQuery query9 = new BsbmQuery() {
        
        @Override
        public void init() {
            loadQuery(9);
        }
        
        @Override
        public Map<String, String> queryParams() {
            Map<String, String> m = new HashMap<>();
            m.put("ReviewXYZ", "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite1/Review1>");
            
            //extra join variable
            //m.put("ReviewerXYZ", "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite1/Reviewer1>");
            return m;
        }

        @Override
        public List<String> urls() {
            return Arrays.asList(
                    "/review/%ReviewXYZ%"
                    //"/person/%ReviewerXYZ%"
            );
        }
    };
    //offer of a certain product, offer filtered by deliveryDays and date
    //TODO returns empty result
    private BsbmQuery query10 = new BsbmQuery() {
        
        @Override
        public void init() {
            loadQuery(10);
        }
        
        @Override
        public Map<String, String> queryParams() {
            Map<String, String> m = new HashMap<>();
            m.put("ProductXYZ", "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product59>");
            m.put("currentDate", "\"2000-05-28T00:00:00\"^^xsd:dateTime");
            return m;
        }

        @Override
        public List<String> urls() {
            return Arrays.asList(
                    "/offer?rql=product==%ProductXYZ%;deliveryDays<=3;validTo>%currentDate%"
            );
        }
    };
    //all in and out coming edges of a offer
    //TODO no offer with incoming edges
    private BsbmQuery query11 = new BsbmQuery() {
        
        @Override
        public void init() {
            loadQuery(11);
        }
        
        @Override
        public Map<String, String> queryParams() {
            Map<String, String> m = new HashMap<>();
            m.put("OfferXYZ", "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1>");
            return m;
        }

        @Override
        public List<String> urls() {
            return Arrays.asList(
                    "/offer/%OfferXYZ%"
            );
        }
    };
    //Construct query
    private BsbmQuery query12 = new BsbmQuery() {
        
        @Override
        public void init() {
            loadQuery(12);
        }
        
        @Override
        public Map<String, String> queryParams() {
            Map<String, String> m = new HashMap<>();
            m.put("OfferXYZ", "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer758>");
            return m;
        }

        @Override
        public List<String> urls() {
            return Arrays.asList(
                    "/offer/%OfferXYZ%"
            );
        }
    };
   
    
    private List<BsbmQuery> queries = Arrays.asList(
            //query1 //OK
            //query2 //OK (label extra calls to resolve)
            //query3 //OK =out= was used
            //query4 //OK (maybe - because only one product matches currently)
            //query5 //OK (BUT missing arithmetic - has to be done in client)
            //query6 //OK regex geht
            //query7 //OK 
            //query8   //OK lang string works
            //query9  //OK (describe)
            //query10 //OK vendor changed to country GB
            //query11 //OK (BUT incoming edge not there: discuss in text)
            //query12 //OK (client muss json keys rename machen)
    );
}
