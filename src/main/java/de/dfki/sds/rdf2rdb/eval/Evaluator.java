package de.dfki.sds.rdf2rdb.eval;

import de.dfki.sds.rdf2rdb.MavenExportConfig;
import de.dfki.sds.rdf2rdb.MavenExporter;
import de.dfki.sds.rdf2rdb.RdfsAnalyzer;
import de.dfki.sds.rdf2rdb.RdfsAnalyzer.Cardinality;
import de.dfki.sds.rdf2rdb.SqliteConverter;
import de.dfki.sds.rdf2rdb.SqliteConverter.SqliteConversion;
import de.dfki.sds.rdf2rdb.SqliteConverter.Table;
import de.dfki.sds.rdf2rdb.SqliteExporter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.tdb2.TDB2Factory;


/**
 *

 */
public class Evaluator {
    
    private boolean showSQL = false;
    private boolean writeSQLite = true;
    
    public Evaluator() {
        
    }
    
    public void run(List<Dataset> datasets) throws IOException {
        File genFolder = new File("gen");
        genFolder.mkdirs();
        
        LatexTable latexTable = new LatexTable();
        latexTable.addColumn("Nr.", "Dataset Number", "c", true);
        latexTable.addColumn("Name", "Dataset Name", "l", true);
        latexTable.addColumn("T", "Number of Triples", "r", true);
        //latexTable.addColumn("NU T", "Not Used Triples", "r", false);
        //latexTable.addColumn("NU P", "Not Used Properties", "r", false);
        latexTable.addColumn("Types", "Number of types", "r", true);
        latexTable.addColumn("Dang.Res.", "Dangling Resources", "r", false);
        latexTable.addColumn("Dang.Res. Prop.", "Dangling Resource Properties", "r", false);
        latexTable.addColumn("Lang.String Prop.", "LangString Properties", "r", false);
        latexTable.addColumn("Res.Prop.", "Number of Resource Properties", "r", true);
        latexTable.addColumn("Lit.Prop.", "Number of Literal Properties", "r", true);
        latexTable.addColumn("MT Inst.", "Multi-Typed Instances", "r", true);
        latexTable.addColumn("Avg.T p MTI", "Avg. Number of Types per Multi-Typed Instance", "r", true);
        latexTable.addColumn("MC Props.", "Multi-Card Properties", "r", true);
        latexTable.addColumn("Sk. BN", "Skolemized Blank Nodes", "r", true);
        latexTable.addColumn("BN Stmts", "Blank Nodes Statements", "r", true);
        latexTable.addColumn("D-Less MCP", "Domainless Multi Card. Properties", "r", true);
        latexTable.addColumn("R-Less MCP", "Rangeless Multi Card. Properties", "r", true);
        latexTable.addColumn("EC", "Explicit Classes", "r", true);
        latexTable.addColumn("EP", "Explicit Properties", "r", true);
        latexTable.addColumn("ED", "Explicit Datatypes", "r", true);
        latexTable.addColumn("ECont", "Explicit Containers", "r", true);
        latexTable.addColumn("NUll CPC", "Null Card. Property Count", "r", true);
        latexTable.addColumn("1:1 CPC", "1:1 Card. Property Count", "r", true);
        latexTable.addColumn("n:1 CPC", "n:1 Card. Property Count", "r", true);
        latexTable.addColumn("1:n CPC", "1:n Card. Property Count", "r", true);
        latexTable.addColumn("n:m CPC", "n:m Card. Property Count", "r", true);
        
        latexTable.addColumn("Text", "Text Storage Class Properties", "r", false);
        latexTable.addColumn("Int", "Int Storage Class Properties", "r", false);
        latexTable.addColumn("Real", "Real Storage Class Properties", "r", false);
        latexTable.addColumn("Blob", "Blob Storage Class Properties", "r", false);
        latexTable.addColumn("Warn.", "Warnings", "r", false);
        latexTable.addColumn("Tables", "Number of Tables", "r", true);
        latexTable.addColumn("NMTables", "Number of n:m Tables", "r", true);
        latexTable.addColumn("ETables", "Number of Entity Tables", "r", true);
        latexTable.addColumn("Avg.Col.", "Average Columns per Entity Table", "r", true);
        latexTable.addColumn("Sum.Col.", "Sum of all Columns (Entity Tables)", "r", true);
        latexTable.addColumn("Time (ms)", "Time in Milliseconds", "r", true);
        
        for(Dataset dataset : datasets) {
            String basename = FilenameUtils.getBaseName(dataset.getFile().getName());
            String ext = FilenameUtils.getExtension(dataset.getFile().getName());
            
            long begin = System.currentTimeMillis();
            
            System.out.println("load dataset "+ dataset.getName() +" into RAM");
            Model model;
            org.apache.jena.query.Dataset ds = null;
            if(dataset.getTdb2GraphUri() != null) {
                ds = TDB2Factory.connectDataset(dataset.getFile().getAbsolutePath());
                model = ds.getNamedModel(dataset.getTdb2GraphUri());
            } else {
                InputStream is = new FileInputStream(dataset.getFile());
                model = ModelFactory.createDefaultModel().read(is, null, ext.equals("rdf") ? "RDF/XML" : ext.toUpperCase());
                is.close();
            }
            
            //if(ext.equals("hdt")) {
                //use HDT2TTL

                //HDTFactory.createHDT().;
                //HDT hdt = HDTManager.loadHDT(dataset.getFile().getAbsolutePath(), null);
                //HDTGraph graph = new HDTGraph(hdt);
                //model = ModelFactory.createDefaultModel(); 
                //model = ModelFactory.createModelForGraph(graph);
                
            //} else {
                //read model from file
                
            //}
            
            //analyze rdf
            if(ds != null) ds.begin(ReadWrite.READ);
            System.out.println("analyze " + model.size() + " triples");
            if(ds != null) ds.end();
            
            RdfsAnalyzer analyzer = new RdfsAnalyzer();
            
            if(ds != null) ds.begin(ReadWrite.READ);
            analyzer.analyze(model);
            if(ds != null) ds.end();
            
            //analyzer.print();

            //convert rdf to sql
            System.out.println("convert to sqlite");
            SqliteConverter converter = new SqliteConverter();
            if(ds != null) ds.begin(ReadWrite.READ);
            SqliteConverter.SqliteConversion conversion = converter.convert(analyzer);
            if(ds != null) ds.end();
            
            long end = System.currentTimeMillis();
            
            //export
            System.out.println("export to sqlite");
            SqliteExporter exporter = new SqliteExporter(conversion);
            if(showSQL) {
                System.out.println(exporter.toString());
            }
            if(writeSQLite) {
                File dbFile = new File(genFolder, basename + ".db");
                FileUtils.deleteQuietly(dbFile);
                exporter.save(dbFile);
            }
            
            
            int scTextCount = analyzer.getStorageClassProperties().getOrDefault(RdfsAnalyzer.StorageClass.TEXT, new HashSet<>()).size();
            int scIntCount = analyzer.getStorageClassProperties().getOrDefault(RdfsAnalyzer.StorageClass.INTEGER, new HashSet<>()).size();
            int scRealCount = analyzer.getStorageClassProperties().getOrDefault(RdfsAnalyzer.StorageClass.REAL, new HashSet<>()).size();
            int scBlobCount = analyzer.getStorageClassProperties().getOrDefault(RdfsAnalyzer.StorageClass.BLOB, new HashSet<>()).size();
            
            int _nmPropCount = 0;
            int _1nPropCount = 0;
            int _n1PropCount = 0;
            int _11PropCount = 0;
            int nullCardPropCount = 0;
            Set<Property> props = new HashSet<>();
            props.addAll(analyzer.getDomainCardinality().keySet());
            props.addAll(analyzer.getRangeCardinality().keySet());
            for(Property p : props) {
                Cardinality d = analyzer.getDomainCardinality().get(p);
                Cardinality r = analyzer.getRangeCardinality().get(p);
                
                if(d == null || r == null) {
                    nullCardPropCount++;
                } else if(d == Cardinality.SINGLE && r == Cardinality.SINGLE) {
                    _11PropCount++;
                } else if(d == Cardinality.SINGLE && r == Cardinality.MULTI) {
                    _1nPropCount++;
                } else if(d == Cardinality.MULTI && r == Cardinality.SINGLE) {
                    _n1PropCount++;
                } else if(d == Cardinality.MULTI && r == Cardinality.MULTI) {
                    _nmPropCount++;
                }
            }
            
            MinAvgMaxSdDouble typeStat = new MinAvgMaxSdDouble();
            for(Resource mti : analyzer.getMultiTypedInstances()) {
                typeStat.add(analyzer.getInstance2types().get(mti).size());
            }
            
            MinAvgMaxSdDouble colStat = new MinAvgMaxSdDouble();
            for(Table t : conversion.getTables()) {
                if(t.isN2M())
                    continue;
                
                colStat.add(t.getColumns().size());
            }
            
            //put in table
            latexTable.addRow(
                datasets.indexOf(dataset) + 1,
                dataset.getName(),
                String.format("%d", analyzer.getSize()),
                //String.format("%d", analyzer.getNotAnalyzedStatements().size()),
                //String.format("%d", analyzer.getNotAnalyzedProperties().size()),
                String.format("%d", analyzer.getTypes().size()),
                String.format("%d", analyzer.getDanglingResources().size()),
                String.format("%d", analyzer.getDanglingResourceProperties().size()),
                String.format("%d", analyzer.getLangStringProperties().size()),
                String.format("%d", analyzer.getResourceProperties().size()),
                String.format("%d", analyzer.getLiteralProperties().size()),
                String.format("%d", analyzer.getMultiTypedInstances().size()),
                typeStat.toStringAvgSDLatex(3),
                String.format("%d", analyzer.getMultiCardProperties().size()),
                String.format("%d", analyzer.getSkolemizedBlankNodes().size()),
                String.format("%d", analyzer.getBlankNodeStatements().size()),
                String.format("%d", analyzer.getDomainlessMultiCardProperties().size()),
                String.format("%d", analyzer.getRangelessMultiCardProperties().size()),
                String.format("%d", analyzer.getExplicitClasses().size()),
                String.format("%d", analyzer.getExplicitProperties().size()),
                String.format("%d", analyzer.getExplicitDatatypes().size()),
                String.format("%d", analyzer.getExplicitContainers().size()),
                String.format("%d", nullCardPropCount),
                String.format("%d", _11PropCount),
                String.format("%d", _n1PropCount),
                String.format("%d", _1nPropCount),
                String.format("%d", _nmPropCount),
                String.format("%d", scTextCount),
                String.format("%d", scIntCount),
                String.format("%d", scRealCount),
                String.format("%d", scBlobCount),
                String.format("%d", analyzer.getWarnings().size()),
                String.format("%d", conversion.getTables().size()),
                String.format("%d", conversion.getTables().stream().filter(t -> t.isN2M()).count()),
                String.format("%d", conversion.getTables().stream().filter(t -> !t.isN2M()).count()),
                colStat.toStringAvgSDLatex(3),
                String.format("%d", (int) colStat.getSum()),
                (end - begin)
            );
            
            System.out.println();
        }
        
        System.out.println(latexTable.getCaption());
        System.out.println(latexTable);
        
        latexTable.saveCSV(new File(genFolder, "result.csv"));
    }
    
    
    public void generateCode(List<Dataset> datasets) throws IOException {
        File genFolder = new File("gen");
        genFolder.mkdirs();
        
        for(Dataset dataset : datasets) {
            String basename = FilenameUtils.getBaseName(dataset.getFile().getName());
            String ext = FilenameUtils.getExtension(dataset.getFile().getName());
            
            System.out.println("load dataset "+ dataset.getName() +" into RAM");
            Model model;
            //read model from file
            InputStream is = new FileInputStream(dataset.getFile());
            model = ModelFactory.createDefaultModel().read(is, null, ext.equals("rdf") ? "RDF/XML" : ext.toUpperCase());
            is.close();
            
            //analyze rdf
            System.out.println("analyze " + model.size() + " triples");
            RdfsAnalyzer analyzer = new RdfsAnalyzer();
            analyzer.analyze(model);
            //analyzer.print();

            //convert rdf to sql
            System.out.println("convert to sqlite");
            SqliteConverter converter = new SqliteConverter();
            SqliteConversion conversion = converter.convert(analyzer);

            System.out.println("export maven " + Arrays.asList(basename, "anonymous.group", "1.0.0-SNAPSHOT", "anonymous.group." + basename));
            MavenExporter exporter = new MavenExporter(conversion);
            
            MavenExportConfig config = new MavenExportConfig();
            config.setArtifactId(basename);
            config.setGroupId("anonymous.group");
            config.setVersion("1.0.0-SNAPSHOT");
            config.setClasspath("anonymous.group." + basename);
            config.setName("");
            config.setDesc("");
            
            exporter.export(new File(genFolder, basename), config);
            
            
            System.out.println();
        }
    }
    
}
