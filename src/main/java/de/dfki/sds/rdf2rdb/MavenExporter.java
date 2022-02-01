package de.dfki.sds.rdf2rdb;

import de.dfki.sds.rdf2rdb.RdfsAnalyzer.StorageClass;
import de.dfki.sds.rdf2rdb.SqliteConverter.Column;
import de.dfki.sds.rdf2rdb.SqliteConverter.SqliteConversion;
import de.dfki.sds.rdf2rdb.SqliteConverter.Table;
import freemarker.cache.ClassTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.TemplateExceptionHandler;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.text.CaseUtils;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import spark.ModelAndView;
import spark.template.freemarker.FreeMarkerEngine;

/**
 *

 */
public class MavenExporter {
    
    private SqliteConversion conversion;
    private String freemarkerTemplateClasspath;
    private FreeMarkerEngine freeMarkerEngine;
    
    public MavenExporter(SqliteConversion conversion) {
        this.conversion = conversion;
        this.freemarkerTemplateClasspath = "/maventmpl";
        initFreemarker();
    }
    
    private void initFreemarker() {
        //template (freemarker)
        Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_28);
        freemarkerConfig.setDefaultEncoding("UTF-8");
        freemarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.HTML_DEBUG_HANDLER);
        freemarkerConfig.setLogTemplateExceptions(false);
        freemarkerConfig.setTemplateLoader(new ClassTemplateLoader(MavenExporter.class, freemarkerTemplateClasspath));
        freeMarkerEngine = new FreeMarkerEngine(freemarkerConfig);
    }
    
    public void export(File projectFolder, MavenExportConfig config) throws IOException {
        projectFolder.mkdirs();
        
        //pom.xml (aggregator)
        //module1 (folder)
        //  pom.xml
        //  src
        //    main
        //      java
        //        <classpath>
        //          Class.java
        //module2 (folder)
        
        Map<String, Object> aggModel = new HashMap<>();
        aggModel.put("artifactId", config.getArtifactId());
        aggModel.put("groupId", config.getGroupId());
        aggModel.put("version", config.getVersion());
        if(config.getName() != null) {
            aggModel.put("name", config.getName());
        }
        if(config.getDesc() != null) {
            aggModel.put("desc", config.getDesc());
        }
        if(config.getPomExtra() != null) {
            aggModel.put("pomExtra", config.getPomExtra());
        }
        String pomAggContent = freeMarkerEngine.render(new ModelAndView(aggModel, "pom-agg.ftl"));
        File pomAggFile = new File(projectFolder, "pom.xml");
        FileUtils.writeStringToFile(pomAggFile, pomAggContent, StandardCharsets.UTF_8);
        
        //API ==================================================================
        
        String apiName = "api";
        
        File apiFolder = new File(projectFolder, apiName);
        apiFolder.mkdirs();
        
        Map<String, Object> pomModel = new HashMap<>();
        pomModel.put("parent_artifactId", config.getArtifactId());
        pomModel.put("parent_groupId", config.getGroupId());
        pomModel.put("parent_version", config.getVersion());
        pomModel.put("artifactId", config.getArtifactId() + "." + apiName);
        String pomContent = freeMarkerEngine.render(new ModelAndView(pomModel, "pom-api.ftl"));
        File pomFile = new File(apiFolder, "pom.xml");
        FileUtils.writeStringToFile(pomFile, pomContent, StandardCharsets.UTF_8);
        
        //src / main / java
        File javaFolder = new File(new File(new File(apiFolder, "src"), "main"), "java");
        javaFolder.mkdirs();
        
        //folder where classes are stored
        String apiClasspath = config.getClasspath().replace("-", "_") + "." + apiName;
        
        File codeFolder = javaFolder;
        String[] segments = apiClasspath.split("\\.");
        for(String segment : segments) {
            codeFolder = new File(codeFolder, segment);
        }
        codeFolder.mkdirs();
        File apiCodeFolder = codeFolder;
        
        List<JavaClass> javaClasses = dataClasses(apiClasspath, codeFolder);
        
        databaseController(apiClasspath, codeFolder, javaClasses);
        
        
        //SERVER ===============================================================
        
        String serverName = "server";
        
        File serverFolder = new File(projectFolder, serverName);
        serverFolder.mkdirs();
        
        //folder where classes are stored
        String serverClasspath = config.getClasspath().replace("-", "_") + "." + serverName;
        
        String serverArtifactId = config.getArtifactId() + "." + serverName;
        
        String serverScriptName = config.getArtifactId() + "-" + serverName;
        
        pomModel.put("artifactId", serverArtifactId);
        pomModel.put("api_artifactId", config.getArtifactId() + "." + apiName);
        pomModel.put("api_groupId", config.getGroupId());
        pomModel.put("api_version", config.getVersion());
        pomModel.put("jarName", serverScriptName + ".jar");
        pomModel.put("classpath", serverClasspath);
        String serverPomContent = freeMarkerEngine.render(new ModelAndView(pomModel, "pom-server.ftl"));
        File serverPomFile = new File(serverFolder, "pom.xml");
        FileUtils.writeStringToFile(serverPomFile, serverPomContent, StandardCharsets.UTF_8);
        
        //src / main / java
        javaFolder = new File(new File(new File(serverFolder, "src"), "main"), "java");
        javaFolder.mkdirs();
        
        codeFolder = javaFolder;
        segments = serverClasspath.split("\\.");
        for(String segment : segments) {
            codeFolder = new File(codeFolder, segment);
        }
        codeFolder.mkdirs();
        
        restEndpoints(projectFolder, serverClasspath, apiClasspath, codeFolder, javaClasses);
        
        Map<String, Object> scriptModel = new HashMap<>();
        scriptModel.put("serviceName", serverScriptName);
        scriptModel.put("jarName", serverScriptName + ".jar");
        scriptModel.put("jarParams", config.getJarParams());
        scriptModel.put("name", serverScriptName);
        String scriptContent = freeMarkerEngine.render(new ModelAndView(scriptModel, "runscript.ftl"));
        File scriptFile = new File(serverFolder, serverScriptName + ".sh");
        FileUtils.writeStringToFile(scriptFile, scriptContent, StandardCharsets.UTF_8);
        
        //CLIENT (in API) ===============================================================
        
        client(apiClasspath, apiCodeFolder, javaClasses);
    }
    
    private List<JavaClass> dataClasses(String classpath, File codeFolder) throws IOException {
        List<JavaClass> javaClasses = new ArrayList<>();
        
        //code: class for each entity table
        for(Table table : conversion.getTables()) {
            if(table.getName().equals(SqliteConverter.RES2ID_TABLE))
                continue;
            
            if(table.isN2M())
                continue;
            
            String className = CaseUtils.toCamelCase(table.getName(), true, '_');
            String endpointName = CaseUtils.toCamelCase(table.getName(), false, '_');
            
            Map<String, Object> classModel = new HashMap<>();
            classModel.put("package", classpath);
            classModel.put("name", className);

            JavaClass javaClass = new JavaClass(className);
            javaClass.table = table;
            javaClass.endpointName = endpointName;
            javaClasses.add(javaClass);
            
            //single ones
            for(Column col : table.getColumns()) {
                String attributeName = CaseUtils.toCamelCase(col.getName(), false, '_');
                String methodName = CaseUtils.toCamelCase(col.getName(), true, '_');
                
                JavaAttribute ja = new JavaAttribute(attributeName, methodName, getJavaTypeFrom(col.getType()));
                ja.column = col;
                ja.resultSetMethod = getResultSetMethod(col.getType());
                javaClass.attributes.add(ja);
                javaClass.singleAttributes.add(ja);
            }
            
            //outgoing n:m relations
            for(Table nmTable : conversion.getTables()) {
                if(!nmTable.isN2M())
                    continue;
                
                Column leftCol = nmTable.getColumns().get(0);
                Column rightCol = nmTable.getColumns().get(1);
                boolean hasLangCol = nmTable.getColumns().size() > 2 && nmTable.getColumns().get(2).getName().equals("lang");
                
                //if table is left joinable check with overlapping type domain
                //if left col points to rdfs:resource every table is potentially joinable (this is the case when rdf:type is there)
                if(leftCol.getOrigin().equals(RDFS.Resource) || leftCol.getOrigin().equals(table.getOrigin())) {
                    
                    String name;
                    if(nmTable.getOrigin().equals(RDF.type)) {
                        //special case for type property
                        name = "type";
                        
                    } else {
                        name = nmTable.getName();

                        //remove prefix for attribute name
                        String pattern = "mn_" + table.getName() + "_";
                        if(name.startsWith(pattern)) {
                            name = name.substring(pattern.length());
                        }
                    }
                    
                    //name of n:m table is name of attribute
                    String attributeName = CaseUtils.toCamelCase(name, false, '_');
                    String methodName = CaseUtils.toCamelCase(name, true, '_');
                    
                    //it is a list or set?
                    String typeSingle = hasLangCol ? "LangString" : getJavaTypeFrom(rightCol.getType());
                    String type = "List<" + typeSingle + ">";
                    
                    JavaAttribute ja = new JavaAttribute(attributeName, methodName, type);
                    ja.typeSingle = typeSingle;
                    ja.nmTable = nmTable;
                    ja.resultSetMethod = getResultSetMethod(rightCol.getType());
                    javaClass.listAttributes.add(ja);
                    javaClass.attributes.add(ja);
                }
            }
            
            classModel.put("javaClass", javaClass);
            String classContent = freeMarkerEngine.render(new ModelAndView(classModel, "dataclass.ftl"));
            
            File classFile = new File(codeFolder, className + ".java");
            FileUtils.writeStringToFile(classFile, classContent, StandardCharsets.UTF_8);
        }
        
        Map<String, Object> model = new HashMap<>();
        model.put("package", classpath);
        String langStringContent = freeMarkerEngine.render(new ModelAndView(model, "LangString.ftl"));
        File langStringFile = new File(codeFolder, "LangString.java");
        FileUtils.writeStringToFile(langStringFile, langStringContent, StandardCharsets.UTF_8);
        
        return javaClasses;
    }
    
    private void restEndpoints(File projectFolder, String classpath, String apiClasspath, File codeFolder, List<JavaClass> javaClasses) throws IOException {
        
        List<String> endpointClasses = new ArrayList<>();
        for(JavaClass javaClass : javaClasses) {
            
            String className = javaClass.getName() + "Resource";
            
            Map<String, Object> classModel = new HashMap<>();
            classModel.put("package", classpath);
            classModel.put("apiClasspath", apiClasspath);
            classModel.put("name", className);
            classModel.put("javaClass", javaClass);
            String classContent = freeMarkerEngine.render(new ModelAndView(classModel, "endpointclass.ftl"));
            
            File classFile = new File(codeFolder, className + ".java");
            FileUtils.writeStringToFile(classFile, classContent, StandardCharsets.UTF_8);
            
            endpointClasses.add(className);
        }
        
        //sort
        endpointClasses.sort((a,b) -> a.compareTo(b));
        javaClasses.sort((a,b) -> a.getEndpointName().compareTo(b.getEndpointName()));
        
        Map<String, Object> model = new HashMap<>();
        model.put("package", classpath);
        String jsonUtilsContent = freeMarkerEngine.render(new ModelAndView(model, "JSONUtils.ftl"));
        File jsonUtilsFile = new File(codeFolder, "JSONUtils.java");
        FileUtils.writeStringToFile(jsonUtilsFile, jsonUtilsContent, StandardCharsets.UTF_8);
        
        model.put("endpointClasses", endpointClasses);
        model.put("javaClasses", javaClasses);
        model.put("defaultPort", "8081");
        model.put("apiClasspath", apiClasspath);
        model.put("databaseFilePath", "../../" + projectFolder.getName() + ".db");
        String serverContent = freeMarkerEngine.render(new ModelAndView(model, "Server.ftl"));
        File serverFile = new File(codeFolder, "Server.java");
        FileUtils.writeStringToFile(serverFile, serverContent, StandardCharsets.UTF_8);
    }
    
    private void databaseController(String classpath, File codeFolder, List<JavaClass> javaClasses) throws IOException {
        Map<String, Object> model = new HashMap<>();
        model.put("package", classpath);
        
        String SqlUtilsContent = freeMarkerEngine.render(new ModelAndView(model, "SqlUtils.ftl"));
        File SqlUtilsFile = new File(codeFolder, "SqlUtils.java");
        FileUtils.writeStringToFile(SqlUtilsFile, SqlUtilsContent, StandardCharsets.UTF_8);
        
        model.put("javaClasses", javaClasses);
        
        String databaseControllerContent = freeMarkerEngine.render(new ModelAndView(model, "DatabaseController.ftl"));
        File databaseControllerFile = new File(codeFolder, "DatabaseController.java");
        FileUtils.writeStringToFile(databaseControllerFile, databaseControllerContent, StandardCharsets.UTF_8);
    }
    
    private void client(String classpath, File codeFolder, List<JavaClass> javaClasses) throws IOException {
        Map<String, Object> model = new HashMap<>();
        model.put("package", classpath);
        model.put("javaClasses", javaClasses);
        
        String clientContent = freeMarkerEngine.render(new ModelAndView(model, "Client.ftl"));
        File clientFile = new File(codeFolder, "Client.java");
        FileUtils.writeStringToFile(clientFile, clientContent, StandardCharsets.UTF_8);
    }
            
    
    private String getJavaTypeFrom(StorageClass sc) {
        switch(sc) {
            case TEXT: return "String";
            case INTEGER: return "Long"; //TODO could be boolean
            case BLOB: return "byte[]";
            case REAL: return "Double";
        }
        return "String";
    }
    
    private String getResultSetMethod(StorageClass sc) {
        switch(sc) {
            case TEXT: return "String";
            case INTEGER: return "Long";
            case BLOB: return "Blob";
            case REAL: return "Double";
        }
        return "String";
    }
    
    public class JavaClass {
        private String name;
        private String endpointName;
        private List<JavaAttribute> attributes;
        private List<JavaAttribute> singleAttributes;
        private List<JavaAttribute> listAttributes;
        private Table table;
        
        public JavaClass(String name) {
            this.name = name;
            this.attributes = new ArrayList<>();
            this.singleAttributes = new ArrayList<>();
            this.listAttributes = new ArrayList<>();
        }

        public String getName() {
            return name;
        }

        public List<JavaAttribute> getAttributes() {
            return attributes;
        }

        public List<JavaAttribute> getSingleAttributes() {
            return singleAttributes;
        }
        
        public List<JavaAttribute> getSingleAttributesWithoutId() {
            return singleAttributes.subList(1, singleAttributes.size());
        }

        public List<JavaAttribute> getListAttributes() {
            return listAttributes;
        }
        
        public Table getTable() {
            return table;
        }

        public String getEndpointName() {
            return endpointName;
        }
        
    }
    
    public class JavaAttribute {
        
        private String name;
        private String methodName;
        private String type;
        private String typeSingle;
        private Column column;
        private Table nmTable;
        private String resultSetMethod;

        public JavaAttribute(String name, String methodName, String type) {
            this.name = name;
            this.methodName = methodName;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        public String getTypeSingle() {
            return typeSingle;
        }
        
        public String getMethodName() {
            return methodName;
        }

        public Table getTable() {
            return nmTable;
        }

        public Column getColumn() {
            return column;
        }

        public String getResultSetMethod() {
            return resultSetMethod;
        }
        
    }
}
