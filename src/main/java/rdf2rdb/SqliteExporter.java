package rdf2rdb;

import freemarker.cache.ClassTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.TemplateExceptionHandler;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import rdf2rdb.SqliteConverter.SqliteConversion;
import rdf2rdb.SqliteConverter.Table;
import spark.ModelAndView;
import spark.template.freemarker.FreeMarkerEngine;

/**
 *

 */
public class SqliteExporter {
    
    private SqliteConversion conversion;
    private String freemarkerTemplateClasspath;
    private FreeMarkerEngine freeMarkerEngine;
    
    public SqliteExporter(SqliteConversion conversion) {
        this.freemarkerTemplateClasspath = "/sqlitetmpl";
        this.conversion = conversion;
        initFreemarker();
    }
    
    private void initFreemarker() {
        //template (freemarker)
        Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_28);
        freemarkerConfig.setDefaultEncoding("UTF-8");
        freemarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.HTML_DEBUG_HANDLER);
        freemarkerConfig.setLogTemplateExceptions(false);
        freemarkerConfig.setTemplateLoader(new ClassTemplateLoader(SqliteExporter.class, freemarkerTemplateClasspath));
        freeMarkerEngine = new FreeMarkerEngine(freemarkerConfig);
    }

    public void save(File dbFile) {
        try {
            Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbFile.getAbsolutePath());
            
            Statement stmt = connection.createStatement();
            
            System.out.println("create schema with " + conversion.getTables().size() + " tables");
            //conversion.getTables().forEach(t -> System.out.println(t.getName()));
            
            for(Table table : conversion.getTables()) {
                Map<String, Object> model = new HashMap<>();
                model.put("table", table);
                
                
                String createTable = freeMarkerEngine.render(new ModelAndView(model, "createTable.ftl"));
                try {
                    stmt.execute(createTable);
                } catch(SQLException e) {
                    throw new SQLException(createTable, e);
                }
            }
            
            System.out.println("insert into data");
            for(Table table : conversion.getTables()) {
                Map<String, Object> model = new HashMap<>();
                model.put("table", table);
                
                if(!table.getInserts().isEmpty()) {
                    String insertInto = "";
                    try {
                        insertInto = freeMarkerEngine.render(new ModelAndView(model, "insertInto.ftl"));
                        stmt.execute(insertInto);
                    } catch(Exception e) {
                        throw new SQLException(insertInto, e);
                    }
                }
            }
            
            stmt.close();
            
            connection.close();
        } catch(SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        
        for(Table table : conversion.getTables()) {
            Map<String, Object> model = new HashMap<>();
            model.put("table", table);
            
            
            String createTable = freeMarkerEngine.render(new ModelAndView(model, "createTable.ftl"));
            sb.append(createTable);
            
            if(!table.getInserts().isEmpty()) {
                String insertInto = freeMarkerEngine.render(new ModelAndView(model, "insertInto.ftl"));
                sb.append(insertInto);
            }
        }
        
        return sb.toString();
    }
    
}
