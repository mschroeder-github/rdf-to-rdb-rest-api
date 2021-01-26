package rdf2rdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Function;
import static java.util.stream.Collectors.toList;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceRequiredException;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.SKOS;
import rdf2rdb.RdfsAnalyzer.Cardinality;
import rdf2rdb.RdfsAnalyzer.MultiCardinalityProperty;
import rdf2rdb.RdfsAnalyzer.StorageClass;

/**
 *

 */
public class SqliteConverter {

    private List<Warning> warnings;
    private Map<Resource, Integer> res2id;
    
    public static final String RES2ID_TABLE = "_res_id";
    
    public SqliteConverter() {
        warnings = new ArrayList<>();
    }

    public SqliteConversion convert(RdfsAnalyzer analyzer) {
        SqliteConversion conversion = new SqliteConversion();

        typesToTables(conversion, analyzer);

        tablesWithColumns(conversion, analyzer);
        
        tablesSortColumns(conversion, analyzer);

        tablesWithMultiCardinality(conversion, analyzer);
        
        System.out.println("table inserts");
        tableInserts(conversion, analyzer);
        
        System.out.println("write resource id map");
        tableResourceIdMap(conversion);

        return conversion;
    }

    //classes (types) to table to list instances in these tables
    private void typesToTables(SqliteConversion conversion, RdfsAnalyzer analyzer) {
        //check for duplicate names
        Map<String, List<Resource>> name2duplicates = new HashMap<>();
        for (Resource type : analyzer.getTypes()) {
            name2duplicates.computeIfAbsent( getProperName(type), s -> new ArrayList<>()).add(type);
        }
        
        //for each type a table
        for (Resource type : analyzer.getTypes()) {
            String pn = getProperName(type);
            
            List<Resource> types = name2duplicates.get(pn);
            
            Table table = new Table(pn + (types.size() > 1 ? (types.indexOf(type)+1) : ""));
            table.setOrigin(type);

            Column idCol = new Column("id", StorageClass.INTEGER);
            idCol.setPrimary();
            idCol.setAutoincrement();
            idCol.setNotNull();
            table.columns.add(idCol);

            conversion.tables.add(table);
            conversion.type2table.put(type, table);
        }
    }

    //for each table make columns based on instances' properties
    private void tablesWithColumns(SqliteConversion conversion, RdfsAnalyzer analyzer) {
        //for each type's property a column
        for (Table table : conversion.getTables().toArray(new Table[0])) {
            List<Property> props = new ArrayList<>(analyzer.getType2prop().get(table.getOrigin()));
            
            Map<String, List<Column>> name2duplicates = new HashMap<>();
            
            for (Property prop : props) {
                if (prop.equals(RDF.type)) {
                    continue;
                }
                
                if(analyzer.getLangStringProperties().contains(prop)) {
                    //should be n:m table
                    continue;
                }

                String name = getProperName(prop);
                
                //single column assumption
                Column column = new Column(name, analyzer.getPropertyStorageClass().get(prop));
                //check for duplicate names
                name2duplicates.computeIfAbsent(name, s -> new ArrayList<>()).add(column);
                column.setOrigin(prop);
                if (column.type == null) {
                    column.type = StorageClass.INTEGER;
                }

                //update: do it when inserting data
                //handle dangling resources
                //boolean isDanglingProperty = analyzer.getDanglingResourceProperties().contains(prop);
                //if (isDanglingProperty) {
                    //we save the raw URI of dangling link
                //    column.type = StorageClass.TEXT;
                //}

                //check cardinality
                Cardinality domain = analyzer.getDomainCardinality().get(prop);
                Cardinality range = analyzer.getRangeCardinality().get(prop);
                if (range == Cardinality.MULTI) {
                    //?:m

                    if (domain == Cardinality.SINGLE) {
                        //1:m

                        //put column in property's range table (because 1:n relationship)
                        Set<Resource> ranges = analyzer.getPropertyRanges().get(prop);
                        if (ranges.isEmpty()) {
                            //this can happen when target is dangling resource or literal
                            
                            //not a problem anymore
                            
                        } else {
                            for (Resource rng : ranges) {
                                Table rngTable = conversion.type2table.get(rng);
                                if (rngTable != null) {
                                    
                                    column.name = table.getName() + "_" + column.name;
                                    
                                    rngTable.columns.add(column);
                                    //assign to column that is an incoming link
                                    //and also state the domain
                                    column.setIncomingDomain(table.origin);
                                    
                                    //we could state here that column is joinable with table
                                }
                            }
                        }
                    }
                    
                    //n:m is handled separately
                } else {
                    //n:1 or 1:1 not a problem because column is then part of table
                    conversion.prop2cols.computeIfAbsent(prop, p -> new HashSet<>()).add(column);
                    table.columns.add(column);
                }
            }//for each prop
            
            for(Entry<String, List<Column>> entry : new HashSet<>(name2duplicates.entrySet())) {
                if(entry.getValue().size() > 1) {
                    //change name with index
                    for(int i = 0; i < entry.getValue().size(); i++) {
                        entry.getValue().get(i).name += "" + (i+1);
                    }
                } else {
                    name2duplicates.remove(entry.getKey());
                }
            }
            
        }//for each table
    }

    private void tablesSortColumns(SqliteConversion conversion, RdfsAnalyzer analyzer) {
        List<Property> props = Arrays.asList(
                SKOS.prefLabel,
                SKOS.altLabel,
                RDFS.label,
                RDFS.comment
        );
        List<String> names = Arrays.asList(
                "name",
                "label",
                "title",
                "designator"
        );
        List<StorageClass> storageClasses = Arrays.asList(
                StorageClass.TEXT,
                StorageClass.INTEGER,
                StorageClass.REAL,
                StorageClass.BLOB
        );
        
        
        for(Table table : conversion.getTables()) {
            if(!table.isN2M()) {
                List<Column> reordered = new ArrayList<>();
                
                //id always first
                reordered.add(table.getColumns().remove(0));
                
                //sort by fixed ordering
                for(Property prop : props) {
                    for(Column col : table.getColumns().toArray(new Column[0])) {
                        if(col.getOrigin().equals(prop)) {
                            table.getColumns().remove(col);
                            reordered.add(col);
                            break;
                        }
                    }
                }
                
                //search for label or name or designtor 
                for(String name : names) {
                    for(Column col : table.getColumns().toArray(new Column[0])) {
                        if(col.getName().contains(name)) {
                            table.getColumns().remove(col);
                            reordered.add(col);
                            break;
                        }
                    }
                }
                
                //sort by storage class, if same, sort by name
                table.getColumns().sort((a,b) -> {
                
                    int aIdx = storageClasses.indexOf(a.getType());
                    int bIdx = storageClasses.indexOf(b.getType());
                    
                    int cmp = Integer.compare(aIdx, bIdx);
                    
                    if(cmp == 0) {
                        return a.getName().compareTo(b.getName());
                    }
                    
                    return cmp;
                });
                reordered.addAll(table.getColumns());
                
                
                table.columns = reordered;
            }
        }
    }
    
    private void tablesWithMultiCardinality(SqliteConversion conversion, RdfsAnalyzer analyzer) {
        Map<String, List<Table>> name2duplicates = new HashMap<>();
        
        for(MultiCardinalityProperty mcp : analyzer.getMultiCardProperties()) {
            
            if(mcp.getProperty().equals(RDF.type)) {
                continue;
            }
            
            boolean isLiteralProp = analyzer.getLiteralProperties().contains(mcp.getProperty());
            
            //use prefix "mn" to indicate a m:n table
            String tableName = "mn";
            
            //Domain ============================================================
            //domain is almost always an id, however if the resources do not have a type, they are not stored in the database (dangling resources)
            //that is why we create a URI text
            String leftColName;
            StorageClass leftColSC;
            if(!mcp.hasDomain()) {
                //we do not have a name so we use "src"
                //we do net have a domain so we use the raw URI here, so "link"
                leftColName = "src_link";
                leftColSC = StorageClass.TEXT;
            } else {
                String pn;
                Table domainTable = conversion.type2table.get(mcp.getDomain());
                if(domainTable != null) {
                    pn = domainTable.getName();
                } else {
                    pn = getProperName(mcp.getDomain());
                }
                //we use the domain name and id because it will be a reference
                leftColName = getProperName(mcp.getDomain()) + "_id";
                leftColSC = StorageClass.INTEGER;
                
                //use the domain in the table name
                tableName += "_" + pn;
            }
            Column leftCol = new Column(leftColName, leftColSC);
            //it will be primary, no dublicates
            leftCol.setPrimary();
            if(mcp.hasDomain()) {
                leftCol.setOrigin(mcp.getDomain());
            }
            
            //Property ============================================================
            
            //use the property name in the table name
            tableName += "_" + getProperName(mcp.getProperty());
            
            //Range ============================================================
            
            StorageClass rightColSC;
            String rightColName;
            if(isLiteralProp) {
                //just value
                rightColName = "value";
            } else {
                if(!mcp.hasRange()) {
                    //it is just target because we do not have a name
                    rightColName = "trg";
                } else {
                    String pn;
                    Table rangeTable = conversion.type2table.get(mcp.getRange());
                    if(rangeTable != null) {
                        pn = rangeTable.getName();
                    } else {
                        pn = getProperName(mcp.getRange());
                    }
                    rightColName = getProperName(mcp.getRange());
                    tableName += "_" + pn;
                }
            }
            
            if(isLiteralProp) {
                //literal
                rightColSC = analyzer.getPropertyStorageClass().get(mcp.getProperty());
                
            } else {
                //resource
                if(!mcp.hasRange()) {
                    //a dangling resource
                    rightColName += "_link";
                    rightColSC = StorageClass.TEXT;
                    
                } else {
                    //so it will be an id in the database
                    rightColName += "_id";
                    rightColSC = StorageClass.INTEGER;
                }
            }
            Column rightCol = new Column(rightColName, rightColSC);
            //it will be primary, no dublicates
            rightCol.setPrimary();
            if(mcp.hasRange()) {
                rightCol.setOrigin(mcp.getRange());
            }
            
            
            
            Table nmTable = new Table(tableName);
            nmTable.nm = true;
            nmTable.setOrigin(mcp.getProperty());
            nmTable.columns.add(leftCol);
            nmTable.columns.add(rightCol);
            
            if(analyzer.getLangStringProperties().contains(mcp.getProperty())) {
                Column langCol = new Column("lang", StorageClass.TEXT);
                langCol.setPrimary();
                nmTable.columns.add(langCol);
            }
            
            //duplicate column names
            Map<String, List<Column>> name2cols = new HashMap<>();
            for(Column col : nmTable.columns) {
                name2cols.computeIfAbsent(col.getName(), s -> new ArrayList<>()).add(col);      
            }
            for(Entry<String, List<Column>> entry : new HashSet<>(name2cols.entrySet())) {
                if(entry.getValue().size() > 1) {
                    //change name with index
                    for(int i = 0; i < entry.getValue().size(); i++) {
                        entry.getValue().get(i).name += "" + (i+1);
                    }
                }
            }
            
            //duplicate table names
            name2duplicates.computeIfAbsent(nmTable.getName(), s -> new ArrayList<>()).add(nmTable);
            
            conversion.tables.add(nmTable);
        }
        
        
        //duplicate name check
        //this can happen when e.g. domain is same, property is same, but range differs (e.g. rdf:langString vs xsd:string) 
        for(Entry<String, List<Table>> entry : new HashSet<>(name2duplicates.entrySet())) {
            if(entry.getValue().size() > 1) {
                //change name with index
                for(int i = 0; i < entry.getValue().size(); i++) {
                    entry.getValue().get(i).name += "" + (i+1);
                }
            } else {
                name2duplicates.remove(entry.getKey());
            }
        }
    }
    
    //deprecated: this version was without analyzer's multiCardProperties
    //see tablesWithMultiCardinality()
    @Deprecated
    private void nmTablesDeprecated(String name, SqliteConversion conversion, RdfsAnalyzer analyzer, Property prop) {
        boolean isDanglingProperty = analyzer.getDanglingResourceProperties().contains(prop);
        
        //create a table for it, but only once
        if (!conversion.prop2table.containsKey(prop)) {

            Set<Resource> domains = analyzer.getPropertyDomains().get(prop);
            Set<Resource> ranges = analyzer.getPropertyRanges().get(prop);

            List<Set[]> pairsOfSingleItemSets = getPairsOfSingleItemSets(domains, ranges);

            for (Set[] pairOfSingleItemSet : pairsOfSingleItemSets) {

                //Resource domRes = (Resource) ((Set)pairOfSingleItemSet[0]).iterator().next();
                //Resource rngRes = (Resource) ((Set)pairOfSingleItemSet[1]).iterator().next();
                Set<Resource> domainSet = (Set<Resource>) pairOfSingleItemSet[0];
                Set<Resource> rangeSet = (Set<Resource>) pairOfSingleItemSet[1];

                String tableName = "nm";
                if (!domainSet.isEmpty()) {
                    tableName += "_" + getProperName(domainSet.iterator().next());
                }
                tableName += "_" + name;
                if (!rangeSet.isEmpty()) {
                    tableName += "_" + getProperName(rangeSet.iterator().next());
                }

                //create n:m table
                Table nmTable = new Table(tableName);
                nmTable.nm = true;
                nmTable.setOrigin(prop);

                //to use a 'for' loop
                Set[] domAndRng = new Set[2];
                domAndRng[0] = pairOfSingleItemSet[0];
                domAndRng[1] = pairOfSingleItemSet[1];

                boolean isLiteralProp = analyzer.getLiteralProperties().contains(prop);

                //domain and range
                for (int i = 0; i < 2; i++) {
                    boolean domainCase = i == 0;
                    boolean rangeCase = i == 1;

                    //Column idCol = new Column(getProperNameFromCol(domAndRng[i]) + (i == 0 || !isLiteralProp ? "_id" : ""), StorageClass.INTEGER);
                    String colName = "";
                    if (domainCase) {
                        colName = domAndRng[i].size() == 1 ? getProperNameFromCol(domAndRng[i]) + "_id" : "src_id";
                    } else if (rangeCase) {
                        colName = (domAndRng[i].size() == 1 ? getProperNameFromCol(domAndRng[i]) : "trg");

                        if (isDanglingProperty) {
                            colName += "_link";
                        } else if (!isLiteralProp) {
                            colName += "_id";
                        }

                        //colName = getProperNameFromCol(domAndRng[i]) + (!isLiteralProp ? "_id" : "");
                    }

                    //assum an id
                    //but not the case if literal
                    StorageClass sc = StorageClass.INTEGER;
                    if (rangeCase && (isLiteralProp || isDanglingProperty)) {
                        sc = analyzer.getPropertyStorageClass().get(prop);
                    }

                    Column idCol = new Column(colName, sc);
                    idCol.setOrigin(null);
                    idCol.setPrimary();
                    idCol.setNotNull();
                    nmTable.columns.add(idCol);

                    //store left and right joinable tables (references)
                    for (Object obj : domAndRng[i]) {
                        Resource refType = (Resource) obj;

                        Table refTable = conversion.type2table.get(refType);

                        if (refTable != null) {
                            List<Table> joinable = domainCase ? nmTable.leftJoinable : nmTable.rightJoinable;

                            if (!joinable.contains(refTable)) {
                                joinable.add(refTable);
                            }
                        } else {
                            int b = 0;
                        }
                    }
                }

                conversion.tables.add(nmTable);
                conversion.prop2table.put(prop, nmTable);
            }
        }
    }
    
    private void tableInserts(SqliteConversion conversion, RdfsAnalyzer analyzer) {

        //resource to id (int) mapping
        res2id = new HashMap<>();
        //Map<Resource, Map<Resource, Integer>> res2type2id = new HashMap<>();

        //because of multi type problematic
        boolean hasMultiTypeInstances = !analyzer.getMultiTypedInstances().isEmpty();
        int globalId = 1;
        
        //for each entity table (not n:m) list all instances and give them id numbers
        for (Table table : conversion.getTables()) {
            Resource origin = table.getOrigin();
            if (!table.isN2M()) {

                int tableId = 1;
                for (Statement instStmt : toIterable(analyzer.getModel().listStatements(null, RDF.type, origin))) {
                    if (instStmt.getSubject().isURIResource()) {
                        
                        //this was an idea:
                        //if we want to assign id per resource per type
                        //Map<Resource, Integer> t2id = res2type2id.computeIfAbsent(instStmt.getSubject(), s -> new HashMap<>());
                        //if(t2id.containsKey(origin)) {
                            //does not happen
                        //}
                        //t2id.put(origin, tableId);
                        
                        //if instances have multiple types we have no direct instance to table allocation
                        if(hasMultiTypeInstances) {
                            if(res2id.containsKey(instStmt.getSubject())) {
                                //because of multi type this can happen
                                //we use global id so we can skip id assignment
                                continue;
                            }
                            
                            res2id.put(instStmt.getSubject(), globalId);
                            globalId++;
                        } else {
                            //nothing will happen because instace has a 1:1 table allocation
                            res2id.put(instStmt.getSubject(), tableId);
                            tableId++;
                        }
                    }
                    
                }//foar each instance
            }
        }

        System.out.println(res2id.size() + " instances");
        
        //for each table 
        for (Table table : conversion.getTables()) {

            //the table is a entity table (not n:m)
            Resource origin = table.getOrigin();
            if (!table.isN2M()) {
                //entity table
                
                //property position based on column index
                Map<Property, Integer> prop2index = new HashMap<>();
                Map<Property, Map<Resource, Integer>> prop2domain2index = new HashMap<>();
                int index = 0;
                for (Column c : table.getColumns()) {
                    
                    if (c.getOrigin() != null) {
                        if(c.isIncoming()) {
                            //this is the case when we have two equal incoming properties but different domain  
                            
                            prop2domain2index.computeIfAbsent(c.getOrigin().as(Property.class), p -> new HashMap<>()).put(c.getIncomingDomain(), index);
                            
                        } else {
                            prop2index.put(c.getOrigin().as(Property.class), index);
                        }
                    }
                    
                    index++;
                }

                //check duplicate id usage
                //Set<Integer> usedIds = new HashSet<>();
                
                //get all instances again
                for (Statement instStmt : toIterable(analyzer.getModel().listStatements(null, RDF.type, origin))) {
                    if (instStmt.getSubject().isURIResource()) {

                        //create an insert (first column is always id column for the entity)
                        Insert insert = new Insert(table.columns.size());
                        insert.data[0] = res2id.get(instStmt.getSubject());

                        //check duplicate id usage
                        //if(usedIds.contains((Integer) insert.data[0])) {
                        //    int a = 0;
                        //}
                        //usedIds.add((Integer) insert.data[0]);
                        
                        //outgoing
                        for (Statement propStmt : toIterable(analyzer.getModel().listStatements(instStmt.getSubject(), null, (RDFNode) null))) {
                            //put in property if exists
                            Integer i = prop2index.get(propStmt.getPredicate());
                            if (i != null) {
                                putObjectInData(propStmt, insert, i, res2id, analyzer, table);
                            }
                        }
                        
                        //incoming
                        //special incoming properties that result in 1:n which is why this entity (the "n" part) has the column 
                        for (Statement propStmt : toIterable(analyzer.getModel().listStatements(null, null, instStmt.getSubject()))) {
                            //put in property if exists
                            Map<Resource, Integer> domain2index = prop2domain2index.get(propStmt.getPredicate());
                            
                            if(domain2index != null) {
                                Set<Resource> types = analyzer.getInstance2types().get(propStmt.getSubject());
                                if(types != null) {
                                    for(Resource domain : types) {
                                        Integer i = domain2index.get(domain);
                                        if(i != null) {
                                            insert.data[i] = res2id.get(propStmt.getSubject());
                                        }
                                    }
                                }
                            }
                        }
                        
                        table.inserts.add(insert);
                    }
                }

            } else {
                //n:m
                //not entity table, it is a relational table
                
                Property originProp = origin.as(Property.class);
                
                //Set<List<Object>> uniqueRows = new HashSet<>();
                //Set<Integer> hashCodes = new HashSet<>();
                
                //all triples having the property
                for (Statement stmt : toIterable(analyzer.getModel().listStatements(null, originProp, (RDFNode) null))) {
                    
                    Column leftCol = table.columns.get(0);
                    Column rightCol = table.columns.get(1);
                    
                    boolean hasDomain = leftCol.getOrigin() != null;
                    Resource domain = leftCol.getOrigin();
                    boolean hasRange = rightCol.getOrigin() != null;
                    Resource range = rightCol.getOrigin();
                    
                    boolean isLiteralProperty = analyzer.getLiteralProperties().contains(originProp);
                    
                    //do not filter domain and range if rdf:type is used
                    if(!originProp.equals(RDF.type)) {
                    
                        //filter based on domain and range if available
                        if (hasDomain) {
                            if (!analyzer.getModel().contains(stmt.getSubject(), RDF.type, domain)) {
                                continue;
                            }
                        }
                        if (hasRange && !isLiteralProperty) {
                            //we have to check if object is of the range type
                            //do not check if literal property because in RDF it is not 'literal a datatype'
                            if (!analyzer.getModel().contains(stmt.getObject().asResource(), RDF.type, range)) {
                                continue;
                            }
                        }
                    
                    }
                    
                    Insert insert = new Insert(table.columns.size());
                    
                    if(!hasDomain) {
                        //no domain means dangling resource, so use raw URI
                        insert.data[0] = escapeSqliteString(stmt.getSubject().getURI());
                    } else {
                        //domain so it is in the DB so we have an ID
                        insert.data[0] = res2id.get(stmt.getSubject());
                    }
                    
                    if(isLiteralProperty) {
                        if(!hasRange) {
                            //a literal but no data type
                            putObjectInData(stmt, insert, 1, res2id, analyzer, table);
                        } else {
                            //a literal with data type
                            putObjectInData(stmt, insert, 1, res2id, analyzer, table);
                            
                        }
                    } else {
                        if(!hasRange) {
                            //a resource with no range: dangling resource
                            try {
                                insert.data[1] = escapeSqliteString(stmt.getObject().asResource().getURI());
                            } catch(ResourceRequiredException e) {
                                //this happens when the object is a literal but isLiteralProperty was false
                                System.err.println(stmt + " " + e.getMessage());
                            } 
                        } else {
                            //a resource in the database: has an ID
                            insert.data[1] = res2id.get(stmt.getObject().asResource());
                        }
                    }
                    
                    if(analyzer.getLangStringProperties().contains(originProp)) {
                        insert.data[2] = stmt.getObject().asLiteral().getLanguage();
                    }
                    
                    //int hash = Arrays.hashCode(insert.data);
                    //if(hashCodes.contains(hash)) {
                    //    int a = 0;
                    //}
                    //hashCodes.add(hash);
                    
                    //if(uniqueRows.contains(Arrays.asList(insert.data))) {
                    //    int a = 0;
                    //}
                    //uniqueRows.add(Arrays.asList(insert.data));
                    
                    //we only want not null values when rdf:type relation is made
                    if(originProp.equals(RDF.type)) {
                        if(insert.data[0] == null || insert.data[1] == null) {
                            continue;
                        }
                    }
                    
                    table.inserts.add(insert);
                    
                }//for statements

            }
        }
    }
    
    private void tableResourceIdMap(SqliteConversion conversion) {
        Table table = new Table(RES2ID_TABLE);
        Column col1 = new Column("uri", StorageClass.TEXT);
        col1.setPrimary();
        Column col2 = new Column("id", StorageClass.INTEGER);
        table.columns.add(col1);
        table.columns.add(col2);
        for(Entry<Resource, Integer> entry : res2id.entrySet()) {
            Insert insert = new Insert(new Object[] { entry.getKey().getURI(), entry.getValue() });
            table.inserts.add(insert);
        }
        conversion.tables.add(table);
    }
    
    private Iterable<Statement> toIterable(StmtIterator iter) {
        return () -> {
            return iter;
        };
    }
    
    private String escapeSqliteString(String str) {
        if(str == null)
            return str;
        
        //TODO better escape text?
        str = str.replace("'", "''");
        return str;
    }

    private void putObjectInData(Statement stmt, Insert insert, int index, Map<Resource, Integer> res2id, RdfsAnalyzer analyzer, Table table) {
        RDFNode obj = stmt.getObject();
        
        //boolean isDanglingProperty = analyzer.getDanglingResourceProperties().contains(stmt.getPredicate());

        if (obj.isResource()) {

            insert.data[index] = res2id.get(obj.asResource());

            if (insert.data[index] == null/* || isDanglingProperty*/) {
                table.columns.get(index).type = StorageClass.TEXT;
                insert.data[index] = escapeSqliteString(obj.asResource().getURI());
            }

        } else if (obj.isLiteral()) {
            Literal lit = obj.asLiteral();

            Function<Literal, Object> specialParse = analyzer.getSpecialParseProperties().get(stmt.getPredicate());
            
            //we use special parse functions if property has to parse the literal to int
            if(specialParse != null) {
                
                insert.data[index] = specialParse.apply(lit);
                
            } else {
                StorageClass sc = analyzer.getPropertyStorageClass().get(stmt.getPredicate());
                if (sc == StorageClass.TEXT) {
                    
                    insert.data[index] = escapeSqliteString(lit.getString());
                } else if (sc == StorageClass.REAL) {
                    insert.data[index] = lit.getDouble();
                } else if (sc == StorageClass.BLOB) {
                    //use the hex value (base64?)
                    insert.data[index] = lit.getLexicalForm();
                } else if (sc == StorageClass.INTEGER) {
                    //can be time and also boolean
                    if (lit.getValue() instanceof Long || lit.getValue() instanceof Short || lit.getValue() instanceof Integer) {
                        insert.data[index] = lit.getValue();
                    } else {
                        warnings.add(new Warning("integer value problem", stmt));
                    }
                }
            }
        }

    }

    //{a, b, c} x {d, e} => [ ({a},{d}), ({a},{e}), ({b},{d}), etc. ]
    //we return sets because set can be empty
    private List<Set[]> getPairsOfSingleItemSets(Set<Resource> domains, Set<Resource> ranges) {
        List<Set[]> result = new ArrayList<>();

        if (domains.isEmpty() && !ranges.isEmpty()) {

            for (Resource rng : ranges) {
                result.add(new Set[]{new HashSet<>(), new HashSet<>(Arrays.asList(rng))});
            }

        } else if (!domains.isEmpty() && ranges.isEmpty()) {

            for (Resource dom : domains) {
                result.add(new Set[]{new HashSet<>(Arrays.asList(dom)), new HashSet<>()});
            }

        } else {
            for (Resource dom : domains) {
                for (Resource rng : ranges) {
                    result.add(new Set[]{new HashSet<>(Arrays.asList(dom)), new HashSet<>(Arrays.asList(rng))});
                }
            }
        }

        return result;
    }

    private String getProperName(Resource res) {
        return getProperName(res, false);
    }

    private String getProperName(Resource res, boolean properCase) {
        String localname = StringUtility.getLocalName(res.getURI());
        if(localname == null) {
            int a = 0;
        }
        
        localname = StringUtility.replaceSymbols(localname, "");
        localname = StringUtility.umlautConversion(localname);
        localname = StringUtility.splitCamelCaseString(localname, "_").toLowerCase();
        if (properCase) {
            localname = StringUtility.toProperCase(localname);
        }
        if((""+localname.charAt(0)).matches("\\d")) {
            localname = "_" + localname;
        }
        return localname;
    }

    private String getProperNameFromCol(Collection<Resource> rs) {
        List<Resource> l = new ArrayList<>(rs);
        if (l.isEmpty()) {
            throw new RuntimeException("collection is empty");
        }
        if (l.size() == 1) {
            return getProperName(l.get(0));
        }

        l.sort((a, b) -> a.getURI().compareTo(b.getURI()));
        StringJoiner sj = new StringJoiner("");
        for (Resource r : l) {
            sj.add("" + getProperName(r).charAt(0));
        }
        return sj.toString();
    }

    public class SqliteConversion {

        private List<Table> tables;

        private Map<Resource, Table> type2table;
        private Map<Property, Set<Column>> prop2cols;
        private Map<Property, Table> prop2table;

        public SqliteConversion() {
            tables = new ArrayList<>();
            type2table = new HashMap<>();
            prop2cols = new HashMap<>();
            prop2table = new HashMap<>();
        }

        public List<Table> getTables() {
            return tables;
        }

        public Map<Resource, Table> getType2Table() {
            return type2table;
        }

    }

    public abstract class SchemaElement {

        protected String name;

        //resource or property
        protected Resource origin;

        public String getName() {
            return name;
        }

        public Resource getOrigin() {
            return origin;
        }

        public void setOrigin(Resource origin) {
            this.origin = origin;
        }

    }

    public class Table extends SchemaElement {

        private List<Column> columns;
        private List<Insert> inserts;

        //for n:m table
        private boolean nm;
        
        //use column's origin to get domain resp. range
        @Deprecated
        private List<Table> leftJoinable;
        @Deprecated
        private List<Table> rightJoinable;

        public Table(String name) {
            this.name = name;
            this.columns = new ArrayList<>();
            this.inserts = new ArrayList<>();
            this.leftJoinable = new ArrayList<>();
            this.rightJoinable = new ArrayList<>();
        }

        public List<Column> getColumns() {
            return columns;
        }
        
        public List<Column> getColumnsWithoutId() {
            return columns.subList(1, columns.size());
        }

        public List<Insert> getInserts() {
            return inserts;
        }

        public List<Column> getPrimaryKeyColumns() {
            return getColumns().stream().filter(c -> c.primary).collect(toList());
        }

        public boolean hasPrimaryKeyColumns() {
            return !getPrimaryKeyColumns().isEmpty();
        }

        public boolean hasSinglePrimaryKey() {
            return getPrimaryKeyColumns().size() == 1;
        }

        public boolean isN2M() {
            return nm;
        }

        @Override
        public String toString() {
            return "Table{" + "name=" + name + ", columns=" + columns.size() + ", inserts=" + inserts.size() + ", nm=" + nm + ", leftJoinable=" + leftJoinable.size() + ", rightJoinable=" + rightJoinable.size() + '}';
        }

    }

    public class Column extends SchemaElement {

        private StorageClass type;
        private boolean primary;
        private boolean autoincrement;
        private boolean notNull;
        
        //if 1:n and this column was put in the "n" table
        private boolean incoming;
        private Resource incomingDomain;

        public Column(String name, StorageClass type) {
            this.name = name;
            this.type = type;
        }

        public void setPrimary() {
            setPrimary(true);
        }

        public void setPrimary(boolean primary) {
            this.primary = primary;
        }

        public void setAutoincrement() {
            setAutoincrement(true);
        }

        public void setAutoincrement(boolean autoincrement) {
            this.autoincrement = autoincrement;
        }

        public void setNotNull() {
            setNotNull(true);
        }

        public void setNotNull(boolean notNull) {
            this.notNull = notNull;
        }

        public StorageClass getType() {
            return type;
        }

        public boolean isPrimary() {
            return primary;
        }

        public boolean isAutoincrement() {
            return autoincrement;
        }

        public boolean isNotNull() {
            return notNull;
        }

        public boolean isIncoming() {
            return incoming;
        }

        public void setIncoming() {
            setIncoming(true);
        }
        
        public void setIncoming(boolean incoming) {
            this.incoming = incoming;
        }

        public void setIncomingDomain(Resource incomingDomain) {
            this.incomingDomain = incomingDomain;
            setIncoming();
        }

        public Resource getIncomingDomain() {
            return incomingDomain;
        }
        
        @Override
        public String toString() {
            return "Column{" + "name=" + name + ", type=" + type + ", primary=" + primary + ", autoincrement=" + autoincrement + ", notNull=" + notNull + '}';
        }

    }

    public class Insert {

        private Object[] data;

        public Insert(int size) {
            data = new Object[size];
        }

        public Insert(Object[] data) {
            this.data = data;
        }

        public Object[] getData() {
            return data;
        }

    }
    
    public Map<Resource, Integer> getResourceIdMap() {
        return res2id;
    }
    
}
