package de.dfki.sds.rdf2rdb;

import de.dfki.sds.rdf2rdb.util.StringUtility;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import static java.util.stream.Collectors.toSet;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.rdf.model.AnonId;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.VOID;
import org.apache.jena.vocabulary.XSD;

/**
 * Analyzes an RDFS dataset which is the pre-step to convert RDF into another
 * form.
 *
 *
 */
public class RdfsAnalyzer {

    /*
    RDFS
    
    2. Classes

        2.1 rdfs:Resource
        2.8 rdf:Property
        2.2 rdfs:Class
        2.3 rdfs:Literal
        2.4 rdfs:Datatype
    
        2.5 rdf:langString
        2.6 rdf:HTML
        2.7 rdf:XMLLiteral
    
    3. Properties

        3.3 rdf:type
    
        3.1 rdfs:range
        3.2 rdfs:domain
        
        3.6 rdfs:label
        3.7 rdfs:comment
    
        3.4 rdfs:subClassOf
        3.5 rdfs:subPropertyOf
     */
 /*
    NULL. The value is a NULL value.
    INTEGER. The value is a signed integer, stored in 1, 2, 3, 4, 6, or 8 bytes depending on the magnitude of the value.
    REAL. The value is a floating point value, stored as an 8-byte IEEE floating point number.
    TEXT. The value is a text string, stored using the database encoding (UTF-8, UTF-16BE or UTF-16LE).
    BLOB. The value is a blob of data, stored exactly as it was input.
     */
    public enum StorageClass {
        INTEGER,
        REAL,
        TEXT,
        BLOB
    }

    public enum Cardinality {
        SINGLE,
        MULTI
    }

    public class MultiCardinalityProperty {

        private Resource domain;
        private Property property;
        private Resource range;

        public MultiCardinalityProperty(Resource domain, Property property, Resource range) {
            this.domain = domain;
            this.property = property;
            this.range = range;
        }

        public Resource getDomain() {
            return domain;
        }

        public boolean hasDomain() {
            return domain != null;
        }

        public Property getProperty() {
            return property;
        }

        public Resource getRange() {
            return range;
        }

        public boolean hasRange() {
            return range != null;
        }

        @Override
        public String toString() {
            return "MultiCardinalityProperty{" + "domain=" + domain + ", property=" + property + ", range=" + range + '}';
        }
        
        public String getName() {
            StringBuilder sb = new StringBuilder();
            
            if(hasDomain()) {
                sb.append(StringUtility.getLocalName(getDomain().getURI())).append(" ");
            }
            
            sb.append(StringUtility.getLocalName(getProperty().getURI()));
            
            if(hasRange()) {
                sb.append(" ").append(StringUtility.getLocalName(getRange().getURI()));
            }
            
            return sb.toString();
        }

    }

    private Model model;
    private long size;
    private Set<Resource> types;

    private Model analyzedStatements;

    private Map<Resource, Set<Property>> type2prop;

    private Set<Property> literalProperties;
    private Set<Property> resourceProperties;

    private Map<Property, Set<Resource>> propertyDomains;
    private Map<Property, Set<Resource>> propertyRanges;

    //no statement about it in the dataset
    private Set<Resource> danglingResources;
    private Set<Property> danglingResourceProperties;

    private Set<Property> langStringProperties;

    @Deprecated
    private Set<Property> untypedResourceProperties;

    private Map<Property, StorageClass> property2storageClass;
    private Map<StorageClass, Set<Property>> storageClass2properties;

    private Map<Resource, Set<Resource>> instance2types;
    private Map<Resource, Set<Resource>> type2instances;

    private Map<Property, Cardinality> domainCardinality;
    private Map<Property, Cardinality> rangeCardinality;

    private List<Warning> warnings;

    private List<String> filterTypesWithNamespaces;

    private Set<Resource> explicitClasses;
    private Set<Property> explicitProperties;
    private Set<Resource> explicitDatatypes;
    private Set<Resource> explicitContainers;

    private Set<Property> domainlessMultiCardProperties;
    private Set<Property> rangelessMultiCardProperties;

    private List<MultiCardinalityProperty> multiCardProperties;

    private Set<Statement> blankNodeStatements;
    private Set<AnonId> skolemizedBlankNodes;

    private Map<Property, Function<Literal, Object>> specialParseProperties;

    private Function<Literal, Object> boolParser = lit -> {
        return lit.getBoolean() ? 1 : 0;
    };
    private Function<Literal, Object> dateParser = lit -> {
        return LocalDate.parse(lit.getLexicalForm()).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
    };
    private Function<Literal, Object> dateTimeParser = lit -> {
        Object v = lit.getValue();
        XSDDateTime dt = (XSDDateTime) v;
        long epochMillis = dt.asCalendar().toInstant().toEpochMilli();
        return epochMillis;
        //return LocalDateTime.parse(lit.getLexicalForm()).toInstant(ZoneOffset.UTC).toEpochMilli();
    };

    private long analyzedStatementsCount = 0;

    public RdfsAnalyzer() {
        types = new HashSet<>();
        analyzedStatements = ModelFactory.createDefaultModel();
        type2prop = new HashMap<>();
        literalProperties = new HashSet<>();
        resourceProperties = new HashSet<>();
        warnings = new ArrayList<>();
        danglingResources = new HashSet<>();
        property2storageClass = new HashMap<>();
        storageClass2properties = new HashMap<>();
        instance2types = new HashMap<>();
        type2instances = new HashMap<>();
        rangeCardinality = new HashMap<>();
        domainCardinality = new HashMap<>();
        filterTypesWithNamespaces = new ArrayList<>();
        filterTypesWithNamespaces.add(RDF.uri);
        filterTypesWithNamespaces.add(RDFS.uri);
        filterTypesWithNamespaces.add(OWL.NS);
        filterTypesWithNamespaces.add(VOID.NS);

        explicitClasses = new HashSet<>();
        explicitProperties = new HashSet<>();
        explicitDatatypes = new HashSet<>();
        explicitContainers = new HashSet<>();

        propertyDomains = new HashMap<>();
        propertyRanges = new HashMap<>();

        danglingResourceProperties = new HashSet<>();
        untypedResourceProperties = new HashSet<>();

        domainlessMultiCardProperties = new HashSet<>();
        rangelessMultiCardProperties = new HashSet<>();

        multiCardProperties = new ArrayList<>();

        blankNodeStatements = new HashSet<>();
        skolemizedBlankNodes = new HashSet<>();

        specialParseProperties = new HashMap<>();

        langStringProperties = new HashSet<>();
    }

    public RdfsAnalyzer analyze(Model model) {
        this.model = model;

        skolemize();

        size = model.size();

        //TODO subClassOf
        //TODO subPropertyOf
        types = new HashSet<>();

        //explicit T-Box
        for (Statement stmt : toIterable(model.listStatements(null, RDF.type, (RDFNode) null))) {
            //analyzedStatements.add(stmt);
            analyzedStatementsCount++;

            if (stmt.getSubject().isAnon() || stmt.getObject().isAnon()) {
                blankNodeStatements.add(stmt);
                continue;
            }

            if (stmt.getObject().asResource().equals(RDFS.Class)) {
                explicitClasses.add(stmt.getSubject());
            } else if (stmt.getObject().asResource().equals(RDF.Property)) {
                explicitProperties.add(stmt.getSubject().as(Property.class));
            } else if (stmt.getObject().asResource().equals(RDFS.Datatype)) {
                explicitDatatypes.add(stmt.getSubject());
            } else if (stmt.getObject().asResource().equals(RDF.List)) {
                explicitContainers.add(stmt.getSubject());
            } else if (stmt.getObject().asResource().equals(RDF.Bag)) {
                explicitContainers.add(stmt.getSubject());
            }

            Resource type = stmt.getObject().asResource();

            boolean add = true;
            for (String ns : filterTypesWithNamespaces) {
                if (type.getURI().startsWith(ns)) {
                    add = false;
                    break;
                }
            }

            if (add) {
                types.add(type);
            }

        }

        explicitClasses.removeIf(cls -> {
            boolean add = true;
            for (String ns : filterTypesWithNamespaces) {
                if (cls.getURI().startsWith(ns)) {
                    add = false;
                    break;
                }
            }
            return !add;
        });
        
        types.addAll(explicitClasses);
        
        //explicit domains and ranges
        for (Property prop : explicitProperties) {
            for (Statement domainStmt : toIterable(model.listStatements(prop, RDFS.domain, (RDFNode) null))) {
                //analyzedStatements.add(domainStmt);
                analyzedStatementsCount++;

                propertyDomains.computeIfAbsent(prop, p -> new HashSet<>()).add(domainStmt.getObject().asResource());
            }
            for (Statement rangeStmt : toIterable(model.listStatements(prop, RDFS.range, (RDFNode) null))) {
                //analyzedStatements.add(rangeStmt);
                analyzedStatementsCount++;

                propertyRanges.computeIfAbsent(prop, p -> new HashSet<>()).add(rangeStmt.getObject().asResource());
            }
        }

        //helper maps for infer cardinality
        //Map<Resource, Set<Resource>> prop2domains = new HashMap<>();
        //Map<Resource, Set<RDFNode>> prop2ranges = new HashMap<>();
        //for each type we collect instances and their properties
        //everything that does not have a type will be not visited here
        for (Resource type : types) {

            //explicit properties also (they is maybe no assertion triple)
            for (Statement domainStmt : toIterable(model.listStatements(null, RDFS.domain, type))) {
                Property pred = ResourceFactory.createProperty(domainStmt.getSubject().getURI());

                Set<Property> propsOfType = type2prop.computeIfAbsent(type, t -> new HashSet<>());
                propsOfType.add(pred);

                Set<Resource> propDom = propertyDomains.computeIfAbsent(pred, p -> new HashSet<>());
                Set<Resource> propRng = propertyRanges.computeIfAbsent(pred, p -> new HashSet<>());

                propDom.add(type);

                for (Statement rangeStmt : toIterable(model.listStatements(pred, RDFS.range, (RDFNode) null))) {
                    propRng.add(rangeStmt.getResource());

                    if (rangeStmt.getResource().getURI().startsWith(XSD.NS)) {
                        literalProperties.add(pred);

                        //only pred is used in given statement
                        StorageClass storageClass = getStorageClassFromDatatype(rangeStmt.getResource().getURI(), ResourceFactory.createStatement(RDF.nil, pred, RDF.nil));

                        property2storageClass.put(pred, storageClass);
                        storageClass2properties.computeIfAbsent(storageClass, sc -> new HashSet<>()).add(pred);

                    } else {
                        resourceProperties.add(pred);
                    }
                }
            }

            for (Statement instStmt : toIterable(model.listStatements(null, RDF.type, type))) {
                //analyzedStatements.add(instStmt);
                analyzedStatementsCount++;

                if (instStmt.getSubject().isAnon()) {
                    blankNodeStatements.add(instStmt);
                    continue;
                }

                //for multi type check
                //can also be used to find all typed resources
                instance2types.computeIfAbsent(instStmt.getSubject(), s -> new HashSet<>()).add(type);
                type2instances.computeIfAbsent(type, t -> new HashSet<>()).add(instStmt.getSubject());

                for (Statement propStmt : toIterable(model.listStatements(instStmt.getSubject(), null, (RDFNode) null))) {
                    //analyzedStatements.add(propStmt);
                    analyzedStatementsCount++;

                    if (propStmt.getPredicate().isAnon() || propStmt.getObject().isAnon()) {
                        blankNodeStatements.add(propStmt);
                        continue;
                    }

                    Property pred = propStmt.getPredicate();

                    //infer property's domains and ranges
                    Set<Resource> propDom = propertyDomains.computeIfAbsent(pred, p -> new HashSet<>());
                    Set<Resource> propRng = propertyRanges.computeIfAbsent(pred, p -> new HashSet<>());

                    Set<Property> propsOfType = type2prop.computeIfAbsent(type, t -> new HashSet<>());
                    propsOfType.add(pred);

                    //inferred domain
                    for (Statement stmt : toIterable(model.listStatements(propStmt.getSubject(), RDF.type, (RDFNode) null))) {
                        propDom.add(stmt.getObject().asResource());
                    }

                    //Cardinality ==============================================
                    //infer the cardinality for later 1:1, 1:n, n:1, n:m
                    //cardinality(pred, propStmt, prop2domains, prop2ranges);
                    //Object ===================================================
                    //infer property's type by looking at the object
                    if (propStmt.getObject().isLiteral()) {

                        Literal lit = propStmt.getObject().asLiteral();

                        String dt = propStmt.getObject().asLiteral().getDatatypeURI();
                        if (dt != null) {
                            propRng.add(ResourceFactory.createResource(dt));
                        } else {
                            propRng.add(RDFS.Literal);
                        }

                        boolean hasLang = lit.getLanguage() != null && !lit.getLanguage().isEmpty();

                        if (hasLang) {
                            langStringProperties.add(pred);
                        }

                        if (resourceProperties.contains(pred)) {
                            warn("statement's property has resource and literal object", propStmt);
                        } else {
                            literalProperties.add(pred);
                        }

                        //storage class
                        StorageClass storageClass = inferStorageClass(propStmt);
                        if (property2storageClass.containsKey(pred)) {
                            StorageClass expected = property2storageClass.get(pred);
                            if (expected != storageClass) {
                                warn("property's literal's storageClass was expected to be " + expected + ", but was inferred to " + storageClass, propStmt);
                            }
                        }
                        property2storageClass.put(pred, storageClass);
                        storageClass2properties.computeIfAbsent(storageClass, sc -> new HashSet<>()).add(pred);

                    } else if (propStmt.getObject().isResource()) {

                        if (literalProperties.contains(pred)) {
                            warn("statement's property has literal and resource object", propStmt);
                        } else {
                            resourceProperties.add(pred);
                        }

                        Resource obj = propStmt.getObject().asResource();

                        if (!types.contains(obj) && !model.contains(propStmt.getObject().asResource(), null)) {
                            danglingResources.add(propStmt.getObject().asResource());
                            danglingResourceProperties.add(pred);
                            //because column contains link
                            property2storageClass.put(pred, StorageClass.INTEGER); //update: this is changed to TEXT when table inserts are made
                        }

                        //inferred range
                        for (Statement stmt : toIterable(model.listStatements(obj, RDF.type, (RDFNode) null))) {
                            propRng.add(stmt.getObject().asResource());
                        }

                    } else {
                        warn("statement's object is neither literal nor resource", propStmt);
                    }
                }
            }
        }

        cardinalityAnalysis();

        multiCardinalityProperties();

        //untypedAnalysis();
        //multiTypeCleanup();
        
        return this;
    }

    private Iterable<Statement> toIterable(StmtIterator iter) {
        return () -> {
            return iter;
        };
    }

    private void multiTypeCleanup() {
        Set<Set<Resource>> setOfTypes = new HashSet<>();

        Set<Resource> mtis = getMultiTypedInstances();
        for (Resource res : mtis) {

            Set<Resource> types = instance2types.get(res);

            setOfTypes.add(types);
        }
    }

    //deprecated: use cardinalityAnalysis()
    @Deprecated
    private void cardinality(Property pred, Statement propStmt, Map<Resource, Set<Resource>> prop2domains, Map<Resource, Set<RDFNode>> prop2ranges) {

        if (!domainCardinality.containsKey(pred)) {
            domainCardinality.put(pred, Cardinality.SINGLE);
        }
        if (!rangeCardinality.containsKey(pred)) {
            rangeCardinality.put(pred, Cardinality.SINGLE);
        }

        //same domain object uses multiple properties => range differs
        if (rangeCardinality.get(pred) != Cardinality.MULTI) {
            Set<Resource> domains = prop2domains.computeIfAbsent(pred, p -> new HashSet<>());
            if (domains.contains(propStmt.getSubject())) {
                rangeCardinality.put(pred, Cardinality.MULTI);
                prop2domains.remove(pred);
            } else {
                domains.add(propStmt.getSubject());
            }
        }
        //same range object referred by the same property again => domain differs
        if (domainCardinality.get(pred) != Cardinality.MULTI) {
            Set<RDFNode> ranges = prop2ranges.computeIfAbsent(pred, p -> new HashSet<>());
            if (ranges.contains(propStmt.getObject())) {
                domainCardinality.put(pred, Cardinality.MULTI);
                prop2ranges.remove(pred);
            } else {
                ranges.add(propStmt.getObject());
            }
        }
    }

    //remove blanknodes
    private void skolemize() {
        Map<AnonId, Resource> id2res = new HashMap<>();

        Model toBeRemoved = ModelFactory.createDefaultModel();
        Model toBeAdded = ModelFactory.createDefaultModel();

        for (Statement stmt : toIterable(model.listStatements())) {

            //fast filter
            if (!stmt.getSubject().isAnon() && !stmt.getPredicate().isAnon() && !stmt.getObject().isAnon()) {
                continue;
            }

            List<Resource> resList = new ArrayList<>(Arrays.asList(stmt.getSubject(), stmt.getPredicate()));
            if (stmt.getObject().isResource()) {
                resList.add(stmt.getObject().asResource());
            }

            RDFNode[] toBeReplaced = new RDFNode[3];

            for (int i = 0; i < resList.size(); i++) {
                Resource anonRes = resList.get(i);

                if (anonRes.isAnon()) {

                    Resource uriRes;
                    if (id2res.containsKey(anonRes.getId())) {
                        uriRes = id2res.get(anonRes.getId());
                    } else {
                        uriRes = ResourceFactory.createResource("uuid:" + UUID.randomUUID().toString());
                        id2res.put(anonRes.getId(), uriRes);
                    }

                    toBeReplaced[i] = uriRes;
                }
            }

            //original
            RDFNode[] nodes = new RDFNode[]{stmt.getSubject(), stmt.getPredicate(), stmt.getObject()};
            for (int i = 0; i < toBeReplaced.length; i++) {
                if (toBeReplaced[i] == null) {
                    toBeReplaced[i] = nodes[i];
                }
            }

            //replace
            toBeRemoved.add(stmt);
            toBeAdded.add(toBeReplaced[0].asResource(), toBeReplaced[1].as(Property.class), toBeReplaced[2]);
        }

        model.remove(toBeRemoved);
        model.add(toBeAdded);

        skolemizedBlankNodes.addAll(id2res.keySet());
    }

    //use this to define the cardinality by hand
    public void addProperty(Property property, Cardinality domainCard, Cardinality rangeCard) {
        explicitProperties.add(property);
        
        domainCardinality.put(property, domainCard);
        rangeCardinality.put(property, rangeCard);
        
        multiCardinalityProperty(property);
    }
    
    public void addPropertyDomain(Property property, Resource domain) {
        propertyDomains.computeIfAbsent(property, p -> new HashSet<>()).add(domain);
    }
    
    public void addPropertyRange(Property property, Resource range) {
        propertyRanges.computeIfAbsent(property, p -> new HashSet<>()).add(range);
    }

    private void multiCardinalityProperties() {
        Set<Property> props = new HashSet<>();
        props.addAll(domainCardinality.keySet());
        props.addAll(rangeCardinality.keySet());

        for (Property prop : props) {
            multiCardinalityProperty(prop);
        }
    }

    private void multiCardinalityProperty(Property prop) {
        boolean hasMultiTypeInstances = !getMultiTypedInstances().isEmpty();

        //no n:m table for rdf:type
        //if multi type instance we need a nm_type relation to see the type
        if (!hasMultiTypeInstances && prop.equals(RDF.type)) {
            return;
        }

        if (prop.equals(RDFS.domain) || prop.equals(RDFS.range)) {
            return;
        }

        //special rdf:type case
        if (prop.equals(RDF.type)) {
            multiCardProperties.add(new MultiCardinalityProperty(RDFS.Resource, prop, RDFS.Class));
            return;
        }

        if (prop.equals(RDFS.label)) {
            return;
        }
        
        boolean isLiteralProperty = literalProperties.contains(prop);
        boolean isLangStringProperty = langStringProperties.contains(prop);

        //n:m
        if (isLangStringProperty
                || (domainCardinality.get(prop) == Cardinality.MULTI && rangeCardinality.get(prop) == Cardinality.MULTI)) {

            if (prop == null) {
                int a = 0;
            }

            if (propertyDomains == null) {
                int a = 0;
            }

            Set<Resource> domains = propertyDomains.get(prop);
            Set<Resource> ranges = propertyRanges.get(prop);

            if (domains == null) {
                int a = 0;
            }

            //remove the domain or range if the referred type does not have any instances
            domains.removeIf(type -> !model.contains(null, RDF.type, type));
            //only cleanup ranges if it is not a literal property
            if (!literalProperties.contains(prop)) {
                ranges.removeIf(type -> !model.contains(null, RDF.type, type));
            }

            //classify
            if (domains.isEmpty()) {
                domainlessMultiCardProperties.add(prop);
            }
            if (ranges.isEmpty()) {
                rangelessMultiCardProperties.add(prop);
            }

            //create multi cardinality property entries
            if (!domains.isEmpty() && ranges.isEmpty()) {
                for (Resource dom : domains) {
                    multiCardProperties.add(new MultiCardinalityProperty(dom, prop, null));
                }

            } else if (domains.isEmpty() && !ranges.isEmpty()) {
                for (Resource rng : ranges) {
                    multiCardProperties.add(new MultiCardinalityProperty(null, prop, rng));
                }

            } else {
                for (Resource dom : domains) {
                    for (Resource rng : ranges) {
                        multiCardProperties.add(new MultiCardinalityProperty(dom, prop, rng));
                    }
                }
            }
        } //1:n with no range
        else if (domainCardinality.get(prop) == Cardinality.SINGLE
                && rangeCardinality.get(prop) == Cardinality.MULTI) {

            //System.out.println(prop + " is 1:n");
            //has range: continue
            if (!isLiteralProperty && propertyRanges.get(prop) != null && !propertyRanges.get(prop).isEmpty()) {
                return;
            }

            if (propertyDomains.get(prop) == null || propertyDomains.get(prop).isEmpty()) {
                return;
            }

            Set<Resource> domains = propertyDomains.get(prop);
            //remove the domain or range if the referred type does not have any instances
            domains.removeIf(type -> !model.contains(null, RDF.type, type));

            for (Resource dom : domains) {
                multiCardProperties.add(new MultiCardinalityProperty(dom, prop, null));
            }
        }
    }

    private void cardinalityAnalysis() {
        Map<Property, Map<Resource, Set<RDFNode>>> p2s2os = new HashMap<>();
        Map<Property, Map<RDFNode, Set<Resource>>> p2o2ss = new HashMap<>();

        for (Statement stmt : toIterable(model.listStatements())) {

            Property pred = stmt.getPredicate();

            if (!domainCardinality.containsKey(pred)) {
                domainCardinality.put(pred, Cardinality.SINGLE);
            }
            if (!rangeCardinality.containsKey(pred)) {
                rangeCardinality.put(pred, Cardinality.SINGLE);
            }

            if (rangeCardinality.get(pred) != Cardinality.MULTI) {
                Map<Resource, Set<RDFNode>> s2os = p2s2os.computeIfAbsent(pred, p -> new HashMap<>());
                Set<RDFNode> os = s2os.computeIfAbsent(stmt.getSubject(), s -> new HashSet<>());
                os.add(stmt.getObject());
                if (os.size() > 1) {
                    rangeCardinality.put(pred, Cardinality.MULTI);
                    //System.out.println(pred + " range multi");
                    p2s2os.remove(pred);
                }
            }

            if (domainCardinality.get(pred) != Cardinality.MULTI) {
                Map<RDFNode, Set<Resource>> o2ss = p2o2ss.computeIfAbsent(pred, p -> new HashMap<>());
                Set<Resource> ss = o2ss.computeIfAbsent(stmt.getObject(), s -> new HashSet<>());
                ss.add(stmt.getSubject());
                if (ss.size() > 1) {
                    domainCardinality.put(pred, Cardinality.MULTI);
                    //System.out.println(pred + " domain multi");
                    p2o2ss.remove(pred);
                }
            }
        }
    }

    //TODO necessary?
    //not necessary anymore
    @Deprecated
    private void untypedAnalysis() {
        Set<Resource> untyped = getUntypedResources();
        for (Statement stmt : toIterable(model.listStatements())) {
            if (untyped.contains(stmt.getSubject())) {
                untypedResourceProperties.add(stmt.getPredicate());
            }
            if (stmt.getObject().isResource() && untyped.contains(stmt.getObject().asResource())) {
                untypedResourceProperties.add(stmt.getPredicate());
            }
        }

        Set<Resource> untypedSubjects = new HashSet<>();
        for (Resource subj : model.listSubjects().toList()) {
            if (!model.contains(subj, RDF.type, (RDFNode) null)) {
                untypedSubjects.add(subj);
            }
        }
        Set<Resource> untypedObjects = new HashSet<>();
        for (RDFNode obj : model.listObjects().toList()) {
            if (!obj.isURIResource()) {
                continue;
            }

            if (!model.contains(obj.asResource(), RDF.type, (RDFNode) null)) {
                untypedObjects.add(obj.asResource());
            }
        }

        Set<Resource> untypedResources = new HashSet<>();
        untypedResources.addAll(untypedSubjects);
        untypedResources.addAll(untypedObjects);

        for (Statement stmt : toIterable(model.listStatements())) {
            if (untypedResources.contains(stmt.getSubject())) {
                untypedResourceProperties.add(stmt.getPredicate());
            }
            if (stmt.getObject().isResource() && untypedResources.contains(stmt.getObject().asResource())) {
                untypedResourceProperties.add(stmt.getPredicate());
            }
        }
    }

    private StorageClass inferStorageClass(Statement stmt) {
        Literal l = stmt.getLiteral();

        String lexical = l.getLexicalForm();
        String dt = l.getDatatypeURI();

        if (dt != null) {
            //integer because we will use millis
            return getStorageClassFromDatatype(dt, stmt);
        } else {
            //check via parsing
            try {
                if (!(lexical.equalsIgnoreCase("true") || lexical.equalsIgnoreCase("false"))) {
                    throw new Exception();
                }
                //special literal bool to int converter
                specialParseProperties.put(stmt.getPredicate(), boolParser);
                return StorageClass.INTEGER;
            } catch (Exception e1) {
                try {
                    Long.parseLong(lexical);
                    return StorageClass.INTEGER;
                } catch (Exception e2) {
                    try {
                        Double.parseDouble(lexical);
                        return StorageClass.REAL;
                    } catch (Exception e3) {
                        return StorageClass.TEXT;
                    }
                }
            }
        }
    }

    private StorageClass getStorageClassFromDatatype(String dt, Statement stmt) {
        if (dt.equals(XSD.date.getURI()) || dt.equals(XSD.dateTime.getURI())) {
            if (dt.equals(XSD.date.getURI())) {
                specialParseProperties.put(stmt.getPredicate(), dateParser);
            } else if (dt.equals(XSD.dateTime.getURI())) {
                specialParseProperties.put(stmt.getPredicate(), dateTimeParser);
            }
            return StorageClass.INTEGER;
        } else if (dt.equals(XSD.xboolean.getURI())) {
            return StorageClass.INTEGER;
        } else if (dt.equals(XSD.xdouble.getURI())) {
            return StorageClass.REAL;
        } else if (dt.equals(XSD.xfloat.getURI())) {
            return StorageClass.REAL;
        } else if (dt.equals(XSD.xstring.getURI())) {
            return StorageClass.TEXT;
        } else if (dt.equals(XSD.xlong.getURI())) {
            return StorageClass.INTEGER;
        } else if (dt.equals(XSD.xshort.getURI())) {
            return StorageClass.INTEGER;
        } else if (dt.equals(XSD.xint.getURI())) {
            return StorageClass.INTEGER;
        } else if (dt.equals(XSD.hexBinary.getURI())) {
            return StorageClass.BLOB;
        } else if (dt.equals(XSD.integer.getURI())) {
            return StorageClass.INTEGER;
        } else if (dt.equals(XSD.unsignedShort)) {
            return StorageClass.INTEGER;
        } else if (dt.equals(XSD.unsignedLong)) {
            return StorageClass.INTEGER;
        } else if (dt.equals(XSD.decimal)) {
            return StorageClass.REAL;
        }
        return StorageClass.TEXT;
    }

    private void warn(String text) {
        Warning warning = new Warning(text);
        warnings.add(warning);
    }

    private void warn(String text, Statement stmt) {
        Warning warning = new Warning(text);
        warning.setRelatedStatement(stmt);
        warnings.add(warning);
    }

    public long getSize() {
        return size;
    }

    public Set<Resource> getTypes() {
        return types;
    }

    public Model getAnalyzedStatements() {
        return analyzedStatements;
    }

    public long getAnalyzedStatementsCount() {
        return analyzedStatementsCount;
    }

    public Model getNotAnalyzedStatements() {
        Model m = ModelFactory.createDefaultModel();
        m.add(model);
        m.remove(analyzedStatements);
        return m;
    }

    public Set<Property> getNotAnalyzedProperties() {
        Set<Property> props = new HashSet<>();
        getNotAnalyzedStatements().listStatements().toList().forEach(stmt -> props.add(stmt.getPredicate()));
        return props;
    }

    public Map<Resource, Set<Property>> getType2prop() {
        return type2prop;
    }

    public Set<Property> getLiteralProperties() {
        return literalProperties;
    }

    public Set<Property> getResourceProperties() {
        return resourceProperties;
    }

    public Set<Resource> getDanglingResources() {
        return danglingResources;
    }

    public Set<Resource> getMultiTypedInstances() {
        return instance2types.entrySet().stream().filter(e -> e.getValue().size() > 1).map(e -> e.getKey()).collect(toSet());
    }

    public Map<Property, StorageClass> getPropertyStorageClass() {
        return property2storageClass;
    }

    public Map<StorageClass, Set<Property>> getStorageClassProperties() {
        return storageClass2properties;
    }

    public Map<Resource, Set<Resource>> getInstance2types() {
        return instance2types;
    }

    public Map<Resource, Set<Resource>> getType2instances() {
        return type2instances;
    }
    
    public Map<Property, Cardinality> getDomainCardinality() {
        return domainCardinality;
    }

    public Map<Property, Cardinality> getRangeCardinality() {
        return rangeCardinality;
    }

    public List<Warning> getWarnings() {
        return warnings;
    }

    public List<String> getFilterTypesWithNamespaces() {
        return filterTypesWithNamespaces;
    }

    public Map<Property, Set<Resource>> getPropertyDomains() {
        return propertyDomains;
    }

    public Map<Property, Set<Resource>> getPropertyRanges() {
        return propertyRanges;
    }

    public Set<Resource> getExplicitClasses() {
        return explicitClasses;
    }

    public Set<Property> getExplicitProperties() {
        return explicitProperties;
    }

    public Set<Resource> getExplicitDatatypes() {
        return explicitDatatypes;
    }

    public Set<Resource> getExplicitContainers() {
        return explicitContainers;
    }

    public Set<Property> getDanglingResourceProperties() {
        return danglingResourceProperties;
    }

    //no type statement about it in the dataset
    @Deprecated
    public Set<Resource> getUntypedResources() {
        Set<Resource> untyped = model.listSubjects().toSet();
        untyped.removeAll(instance2types.keySet());
        untyped.removeAll(danglingResources);
        return untyped;
    }

    public Set<Property> getUntypedResourceProperties() {
        return untypedResourceProperties;
    }

    public Set<Property> getDomainlessMultiCardProperties() {
        return domainlessMultiCardProperties;
    }

    public Set<Property> getRangelessMultiCardProperties() {
        return rangelessMultiCardProperties;
    }

    public List<MultiCardinalityProperty> getMultiCardProperties() {
        return multiCardProperties;
    }

    public Set<Statement> getBlankNodeStatements() {
        return blankNodeStatements;
    }

    public Set<AnonId> getSkolemizedBlankNodes() {
        return skolemizedBlankNodes;
    }

    public Map<Property, Function<Literal, Object>> getSpecialParseProperties() {
        return specialParseProperties;
    }

    public Set<Property> getLangStringProperties() {
        return langStringProperties;
    }
    
    
    
    public Model getModel() {
        return model;
    }

    public void print() {
        System.out.println(getSize());
        System.out.println(getAnalyzedStatements().size());

        System.out.println("types");
        getTypes().forEach(t -> System.out.println("\t" + t));
        //getNotAnalyzedStatements().listStatements().toList().forEach(stmt -> System.out.println(stmt));

        System.out.println("not analyzed properties");
        getNotAnalyzedProperties().forEach(prop -> System.out.println(prop));

        System.out.println("literal properties");
        getLiteralProperties().forEach(prop -> System.out.println("\t" + prop));

        System.out.println("resource properties");
        getResourceProperties().forEach(prop -> System.out.println("\t" + prop));

        System.out.println("dangling resources: " + getDanglingResources().size());
        //getDanglingResources().forEach(prop -> System.out.println("\t" + prop));

        System.out.println("multi-typed resources: " + getMultiTypedInstances().size());
        getMultiTypedInstances().forEach(prop -> System.out.println("\t" + prop));

        //System.out.println("untyped resources: " + getUntypedResources().size());
        //getUntypedResources().forEach(res -> System.out.println("\t" + res));
        System.out.println("untyped properties: " + getUntypedResourceProperties().size());
        getUntypedResourceProperties().forEach(prop -> System.out.println("\t" + prop));

        if (!warnings.isEmpty()) {
            System.out.println("WARNINGS:");
            warnings.forEach(w -> System.out.println(w));
        }
    }

}
