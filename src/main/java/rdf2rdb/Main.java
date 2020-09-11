package rdf2rdb;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 *

 */
public class Main {

    public static void main(String[] args) throws Exception {
        generate();
        //evalBsbm();
    }
    
    private static void generate() throws IOException {
        List<Dataset> datasets = Arrays.asList(
                
                new Dataset("TBL-C", new File("datasets/TBL-C.rdf"), Dataset.footnoteurl("http://www.w3.org/People/Berners-Lee/card.rdf")),
                
                new Dataset("CTB", new File("datasets/CTB.ttl"), Dataset.footnoteurl("https://lod-cloud.net/dataset/copyrighttermbank")),
                
                new Dataset("EAT", new File("datasets/EAT.nt"), Dataset.footnoteurl("https://lod-cloud.net/dataset/associations")),
                
                new Dataset("Pokedex", new File("datasets/Pokedex.nt"), Dataset.footnoteurl("https://lod-cloud.net/dataset/data-incubator-pokedex")),
                
                new Dataset("BOW", new File("datasets/BOW.ttl"), "https://betweenourworlds.org/ (Release 2020-06)"),
                
                new Dataset("S-IT", new File("datasets/S-IT.rdf"), Dataset.footnoteurl("https://www.salzburgerland.com/it/")),
                
                new Dataset("BSBM", new File("datasets/BSBM.ttl"), Dataset.footnoteurl("http://wifo5-03.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/"))
        );

        Evaluator evaluator = new Evaluator();
        
        //convert to sqlite and print out table
        evaluator.run(datasets);
        
        //generates maven projects with java code
        evaluator.generateCode(datasets);
    }

    private static void evalBsbm() throws IOException {
        BsbmEvaluator evaluator = new BsbmEvaluator(new File("datasets/bsbm.ttl"));
        evaluator.queries();
    }
    
}
