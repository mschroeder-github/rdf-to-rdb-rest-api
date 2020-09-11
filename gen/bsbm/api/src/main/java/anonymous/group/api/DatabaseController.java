package anonymous.group.api;

import java.sql.*;
import java.io.File;
import java.util.List;
import java.util.ArrayList;
import java.util.StringJoiner;
import cz.jirutka.rsql.parser.ast.*;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Controls the database.
 */
public class DatabaseController {

    private File databaseFile;
    private Connection connection;
    
    public DatabaseController(File databaseFile) {
        this.databaseFile = databaseFile;
    }

    private <T> List<T> selectWithNM(Long id, String leftColName, String nmTableName, Class<T> type) {
        String query = "SELECT * FROM " + nmTableName + " WHERE " + leftColName + " = " + id;
        List<T> result = SqlUtils.supply(connection, c -> {
            List<T> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                if(type.equals(String.class)) {
                    list.add((T) SqlUtils.getString(rs, 2));
                } else if(type.equals(Long.class)) {
                    list.add((T) SqlUtils.getLong(rs, 2));
                } else if(type.equals(Double.class)) {
                    list.add((T) SqlUtils.getDouble(rs, 2));
                } else if(type.equals(LangString.class)) {
                    list.add((T) new LangString(SqlUtils.getString(rs, 2), SqlUtils.getString(rs, 3)));
                }
            }
            
            return list;
        });
        return result;
    }

    public Map<String, Long> getResourceIdMap() {
        StringBuilder querySB = new StringBuilder("SELECT * FROM _res_id");
        String query = querySB.toString();

        Map<String, Long> result = SqlUtils.supply(connection, c -> {

            Map<String, Long> map = new HashMap<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                map.put(rs.getString(1), rs.getLong(2));
            }
            ps.close();

            return map;
        });

        return result;
    }

    //==========================================================================
    // Review

    private RSQLVisitor<Boolean, Review> ReviewVisitor = new RSQLVisitor<Boolean, Review>() {

        @Override
        public Boolean visit(AndNode and, Review record) {
            for(Node n : and.getChildren()) {
                if(n instanceof AndNode) {
                    if(!visit((AndNode)n, record))
                        return false;
                } else if(n instanceof OrNode) {
                    if(!visit((OrNode)n, record))
                        return false;
                } else if(n instanceof ComparisonNode) {
                    if(!visit((ComparisonNode)n, record))
                        return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visit(OrNode or, Review record) {
            for(Node n : or.getChildren()) {
                if(n instanceof AndNode) {
                    if(visit((AndNode)n, record))
                        return true;
                } else if(n instanceof OrNode) {
                    if(visit((OrNode)n, record))
                        return true;
                } else if(n instanceof ComparisonNode) {
                    if(visit((ComparisonNode)n, record))
                        return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(ComparisonNode cn, Review record) {

            Set<String> actual = record.getStringSet(cn.getSelector());
            if(actual == null) {
                return false;
            }
            Set<String> expected = new HashSet<>(cn.getArguments());

            if(cn.getOperator().getSymbol().equals("=lt=") || 
               cn.getOperator().getSymbol().equals("=gt=") ||
               cn.getOperator().getSymbol().equals("=le=") ||
               cn.getOperator().getSymbol().equals("=ge=")) {
            
                double actualDouble = Double.parseDouble(actual.iterator().next());
                double expectedDouble = Double.parseDouble(expected.iterator().next());
                
                if(cn.getOperator().getSymbol().equals("=lt=")) {
                    return actualDouble < expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=gt=")) {
                    return actualDouble > expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=le=")) {
                    return actualDouble <= expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=ge=")) {
                    return actualDouble >= expectedDouble;
                }
            } else if(cn.getOperator().getSymbol().equals("==")) {
                return actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("!=")) {
                return !actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("=in=")) {
                return actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=out=")) {
                return !actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=regex=")) {
                String patternStr = expected.iterator().next();
                Pattern pattern = Pattern.compile(patternStr);
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        Matcher matcher = pattern.matcher(act);
                        if(matcher.find())
                            return true;
                    }
                }
                return false;
            } else if(cn.getOperator().getSymbol().equals("=lang=")) {
                String lang = expected.iterator().next();
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        if(act.endsWith("@" + lang))
                            return true;
                    }
                }
                return false;
            }

            return false;
        }
    };

    public List<Review> selectReview() {
        return selectReview(null, null, null, null);
    }

    public List<Review> selectReview(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM review\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        if(limit != null) {
            querySB.append("\nLIMIT " + limit);
        }
        if(offset != null) {
            querySB.append("\nOFFSET " + offset);
        }

        String query = querySB.toString();

        List<Review> result = SqlUtils.supply(connection, c -> {
            List<Review> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                Review record = new Review();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setTitle(SqlUtils.getString(rs, "title"));
                record.setPublisher(SqlUtils.getString(rs, "publisher"));
                record.setDate(SqlUtils.getLong(rs, "date"));
                record.setRating1(SqlUtils.getLong(rs, "rating1"));
                record.setRating2(SqlUtils.getLong(rs, "rating2"));
                record.setRating3(SqlUtils.getLong(rs, "rating3"));
                record.setRating4(SqlUtils.getLong(rs, "rating4"));
                record.setReviewDate(SqlUtils.getLong(rs, "review_date"));
                record.setReviewFor(SqlUtils.getLong(rs, "review_for"));
                record.setReviewer(SqlUtils.getLong(rs, "reviewer"));

                record.setText(selectWithNM(record.getId(), "review_id", "mn_review_text", LangString.class));
                record.setType(selectWithNM(record.getId(), "resource_id", "mn_resource_type_class", Long.class));
                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(ReviewVisitor, record));
        }

        return result;
    }

    public List<Review> insertReview(List<Review> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO review VALUES (?,?,?,?,?,?,?,?,?,?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(Review record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getTitle());
                ps.setObject(3, record.getPublisher());
                ps.setObject(4, record.getDate());
                ps.setObject(5, record.getRating1());
                ps.setObject(6, record.getRating2());
                ps.setObject(7, record.getRating3());
                ps.setObject(8, record.getRating4());
                ps.setObject(9, record.getReviewDate());
                ps.setObject(10, record.getReviewFor());
                ps.setObject(11, record.getReviewer());
                ps.addBatch();
            }
            ps.executeBatch();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            for(int i = records.size()-1; i >= 0; i--) {
                //does only return the last generated id
                if(!generatedKeys.next())
                    break;

                long id = generatedKeys.getLong(1);
                records.get(i).setId(id);
            }
            c.commit();
            ps.close();

            boolean wasRecordsAdded = false;

            //n:m tables
            query = "INSERT INTO mn_review_text VALUES (?,?,?)";

            ps = c.prepareStatement(query);

            for(Review record : records) {
                if(!record.hasId() || !record.hasText() || record.getText().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getText()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;

            query = "INSERT INTO mn_resource_type_class VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(Review record : records) {
                if(!record.hasId() || !record.hasType() || record.getType().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getType()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;


            c.setAutoCommit(true);
        });

        return records;
    }

    public List<Review> updateReview(List<Review> records, boolean put) {    
        for(Review record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasTitle()) {
                    paramSJ.add("title = ?");
                }
                if(put || record.hasPublisher()) {
                    paramSJ.add("publisher = ?");
                }
                if(put || record.hasDate()) {
                    paramSJ.add("date = ?");
                }
                if(put || record.hasRating1()) {
                    paramSJ.add("rating1 = ?");
                }
                if(put || record.hasRating2()) {
                    paramSJ.add("rating2 = ?");
                }
                if(put || record.hasRating3()) {
                    paramSJ.add("rating3 = ?");
                }
                if(put || record.hasRating4()) {
                    paramSJ.add("rating4 = ?");
                }
                if(put || record.hasReviewDate()) {
                    paramSJ.add("review_date = ?");
                }
                if(put || record.hasReviewFor()) {
                    paramSJ.add("review_for = ?");
                }
                if(put || record.hasReviewer()) {
                    paramSJ.add("reviewer = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE review SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasTitle()) {
                    ps.setObject(index, record.getTitle());
                    index++;
                }
                if(put || record.hasPublisher()) {
                    ps.setObject(index, record.getPublisher());
                    index++;
                }
                if(put || record.hasDate()) {
                    ps.setObject(index, record.getDate());
                    index++;
                }
                if(put || record.hasRating1()) {
                    ps.setObject(index, record.getRating1());
                    index++;
                }
                if(put || record.hasRating2()) {
                    ps.setObject(index, record.getRating2());
                    index++;
                }
                if(put || record.hasRating3()) {
                    ps.setObject(index, record.getRating3());
                    index++;
                }
                if(put || record.hasRating4()) {
                    ps.setObject(index, record.getRating4());
                    index++;
                }
                if(put || record.hasReviewDate()) {
                    ps.setObject(index, record.getReviewDate());
                    index++;
                }
                if(put || record.hasReviewFor()) {
                    ps.setObject(index, record.getReviewFor());
                    index++;
                }
                if(put || record.hasReviewer()) {
                    ps.setObject(index, record.getReviewer());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
                if(put || (record.hasText() && !record.getText().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_review_text WHERE review_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasText() && !record.getText().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_review_text VALUES (?,?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getText()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
                if(put || (record.hasType() && !record.getType().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_resource_type_class WHERE resource_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasType() && !record.getType().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_resource_type_class VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getType()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
            });
        }

        return records;
    }

    public void deleteReview() {
        deleteReview(null);
    }    

    public void deleteReview(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM review");
        String in = "";
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            in = "IN (" + sj.toString() + ")";
            querySB.append(" WHERE id " + in);
        }

        final String finalIn = in;
        final String query = querySB.toString();

        SqlUtils.run(connection, c -> {
            PreparedStatement ps = c.prepareStatement(query);
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_review_text" + (finalIn.isEmpty() ? "" : " WHERE review_id " + finalIn));
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_resource_type_class" + (finalIn.isEmpty() ? "" : " WHERE resource_id " + finalIn));
            ps.execute();
            ps.close();

        });
    }

    //==========================================================================
    // ProductType10

    private RSQLVisitor<Boolean, ProductType10> ProductType10Visitor = new RSQLVisitor<Boolean, ProductType10>() {

        @Override
        public Boolean visit(AndNode and, ProductType10 record) {
            for(Node n : and.getChildren()) {
                if(n instanceof AndNode) {
                    if(!visit((AndNode)n, record))
                        return false;
                } else if(n instanceof OrNode) {
                    if(!visit((OrNode)n, record))
                        return false;
                } else if(n instanceof ComparisonNode) {
                    if(!visit((ComparisonNode)n, record))
                        return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visit(OrNode or, ProductType10 record) {
            for(Node n : or.getChildren()) {
                if(n instanceof AndNode) {
                    if(visit((AndNode)n, record))
                        return true;
                } else if(n instanceof OrNode) {
                    if(visit((OrNode)n, record))
                        return true;
                } else if(n instanceof ComparisonNode) {
                    if(visit((ComparisonNode)n, record))
                        return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(ComparisonNode cn, ProductType10 record) {

            Set<String> actual = record.getStringSet(cn.getSelector());
            if(actual == null) {
                return false;
            }
            Set<String> expected = new HashSet<>(cn.getArguments());

            if(cn.getOperator().getSymbol().equals("=lt=") || 
               cn.getOperator().getSymbol().equals("=gt=") ||
               cn.getOperator().getSymbol().equals("=le=") ||
               cn.getOperator().getSymbol().equals("=ge=")) {
            
                double actualDouble = Double.parseDouble(actual.iterator().next());
                double expectedDouble = Double.parseDouble(expected.iterator().next());
                
                if(cn.getOperator().getSymbol().equals("=lt=")) {
                    return actualDouble < expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=gt=")) {
                    return actualDouble > expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=le=")) {
                    return actualDouble <= expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=ge=")) {
                    return actualDouble >= expectedDouble;
                }
            } else if(cn.getOperator().getSymbol().equals("==")) {
                return actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("!=")) {
                return !actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("=in=")) {
                return actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=out=")) {
                return !actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=regex=")) {
                String patternStr = expected.iterator().next();
                Pattern pattern = Pattern.compile(patternStr);
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        Matcher matcher = pattern.matcher(act);
                        if(matcher.find())
                            return true;
                    }
                }
                return false;
            } else if(cn.getOperator().getSymbol().equals("=lang=")) {
                String lang = expected.iterator().next();
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        if(act.endsWith("@" + lang))
                            return true;
                    }
                }
                return false;
            }

            return false;
        }
    };

    public List<ProductType10> selectProductType10() {
        return selectProductType10(null, null, null, null);
    }

    public List<ProductType10> selectProductType10(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM product_type10\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        if(limit != null) {
            querySB.append("\nLIMIT " + limit);
        }
        if(offset != null) {
            querySB.append("\nOFFSET " + offset);
        }

        String query = querySB.toString();

        List<ProductType10> result = SqlUtils.supply(connection, c -> {
            List<ProductType10> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                ProductType10 record = new ProductType10();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setLabel(SqlUtils.getString(rs, "label"));
                record.setComment(SqlUtils.getString(rs, "comment"));
                record.setProductPropertyTextual1(SqlUtils.getString(rs, "product_property_textual1"));
                record.setProductPropertyTextual2(SqlUtils.getString(rs, "product_property_textual2"));
                record.setProductPropertyTextual3(SqlUtils.getString(rs, "product_property_textual3"));
                record.setProductPropertyTextual4(SqlUtils.getString(rs, "product_property_textual4"));
                record.setProductPropertyTextual5(SqlUtils.getString(rs, "product_property_textual5"));
                record.setProductPropertyTextual6(SqlUtils.getString(rs, "product_property_textual6"));
                record.setPublisher(SqlUtils.getString(rs, "publisher"));
                record.setDate(SqlUtils.getLong(rs, "date"));
                record.setProducer(SqlUtils.getLong(rs, "producer"));
                record.setProductPropertyNumeric1(SqlUtils.getLong(rs, "product_property_numeric1"));
                record.setProductPropertyNumeric2(SqlUtils.getLong(rs, "product_property_numeric2"));
                record.setProductPropertyNumeric3(SqlUtils.getLong(rs, "product_property_numeric3"));
                record.setProductPropertyNumeric4(SqlUtils.getLong(rs, "product_property_numeric4"));
                record.setProductPropertyNumeric5(SqlUtils.getLong(rs, "product_property_numeric5"));
                record.setProductPropertyNumeric6(SqlUtils.getLong(rs, "product_property_numeric6"));

                record.setProductFeatureProductFeature(selectWithNM(record.getId(), "product_type10_id", "mn_product_type10_product_feature_product_feature", Long.class));
                record.setType(selectWithNM(record.getId(), "resource_id", "mn_resource_type_class", Long.class));
                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(ProductType10Visitor, record));
        }

        return result;
    }

    public List<ProductType10> insertProductType10(List<ProductType10> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO product_type10 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(ProductType10 record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getLabel());
                ps.setObject(3, record.getComment());
                ps.setObject(4, record.getProductPropertyTextual1());
                ps.setObject(5, record.getProductPropertyTextual2());
                ps.setObject(6, record.getProductPropertyTextual3());
                ps.setObject(7, record.getProductPropertyTextual4());
                ps.setObject(8, record.getProductPropertyTextual5());
                ps.setObject(9, record.getProductPropertyTextual6());
                ps.setObject(10, record.getPublisher());
                ps.setObject(11, record.getDate());
                ps.setObject(12, record.getProducer());
                ps.setObject(13, record.getProductPropertyNumeric1());
                ps.setObject(14, record.getProductPropertyNumeric2());
                ps.setObject(15, record.getProductPropertyNumeric3());
                ps.setObject(16, record.getProductPropertyNumeric4());
                ps.setObject(17, record.getProductPropertyNumeric5());
                ps.setObject(18, record.getProductPropertyNumeric6());
                ps.addBatch();
            }
            ps.executeBatch();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            for(int i = records.size()-1; i >= 0; i--) {
                //does only return the last generated id
                if(!generatedKeys.next())
                    break;

                long id = generatedKeys.getLong(1);
                records.get(i).setId(id);
            }
            c.commit();
            ps.close();

            boolean wasRecordsAdded = false;

            //n:m tables
            query = "INSERT INTO mn_product_type10_product_feature_product_feature VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType10 record : records) {
                if(!record.hasId() || !record.hasProductFeatureProductFeature() || record.getProductFeatureProductFeature().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getProductFeatureProductFeature()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;

            query = "INSERT INTO mn_resource_type_class VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType10 record : records) {
                if(!record.hasId() || !record.hasType() || record.getType().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getType()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;


            c.setAutoCommit(true);
        });

        return records;
    }

    public List<ProductType10> updateProductType10(List<ProductType10> records, boolean put) {    
        for(ProductType10 record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasLabel()) {
                    paramSJ.add("label = ?");
                }
                if(put || record.hasComment()) {
                    paramSJ.add("comment = ?");
                }
                if(put || record.hasProductPropertyTextual1()) {
                    paramSJ.add("product_property_textual1 = ?");
                }
                if(put || record.hasProductPropertyTextual2()) {
                    paramSJ.add("product_property_textual2 = ?");
                }
                if(put || record.hasProductPropertyTextual3()) {
                    paramSJ.add("product_property_textual3 = ?");
                }
                if(put || record.hasProductPropertyTextual4()) {
                    paramSJ.add("product_property_textual4 = ?");
                }
                if(put || record.hasProductPropertyTextual5()) {
                    paramSJ.add("product_property_textual5 = ?");
                }
                if(put || record.hasProductPropertyTextual6()) {
                    paramSJ.add("product_property_textual6 = ?");
                }
                if(put || record.hasPublisher()) {
                    paramSJ.add("publisher = ?");
                }
                if(put || record.hasDate()) {
                    paramSJ.add("date = ?");
                }
                if(put || record.hasProducer()) {
                    paramSJ.add("producer = ?");
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    paramSJ.add("product_property_numeric1 = ?");
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    paramSJ.add("product_property_numeric2 = ?");
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    paramSJ.add("product_property_numeric3 = ?");
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    paramSJ.add("product_property_numeric4 = ?");
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    paramSJ.add("product_property_numeric5 = ?");
                }
                if(put || record.hasProductPropertyNumeric6()) {
                    paramSJ.add("product_property_numeric6 = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE product_type10 SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasLabel()) {
                    ps.setObject(index, record.getLabel());
                    index++;
                }
                if(put || record.hasComment()) {
                    ps.setObject(index, record.getComment());
                    index++;
                }
                if(put || record.hasProductPropertyTextual1()) {
                    ps.setObject(index, record.getProductPropertyTextual1());
                    index++;
                }
                if(put || record.hasProductPropertyTextual2()) {
                    ps.setObject(index, record.getProductPropertyTextual2());
                    index++;
                }
                if(put || record.hasProductPropertyTextual3()) {
                    ps.setObject(index, record.getProductPropertyTextual3());
                    index++;
                }
                if(put || record.hasProductPropertyTextual4()) {
                    ps.setObject(index, record.getProductPropertyTextual4());
                    index++;
                }
                if(put || record.hasProductPropertyTextual5()) {
                    ps.setObject(index, record.getProductPropertyTextual5());
                    index++;
                }
                if(put || record.hasProductPropertyTextual6()) {
                    ps.setObject(index, record.getProductPropertyTextual6());
                    index++;
                }
                if(put || record.hasPublisher()) {
                    ps.setObject(index, record.getPublisher());
                    index++;
                }
                if(put || record.hasDate()) {
                    ps.setObject(index, record.getDate());
                    index++;
                }
                if(put || record.hasProducer()) {
                    ps.setObject(index, record.getProducer());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    ps.setObject(index, record.getProductPropertyNumeric1());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    ps.setObject(index, record.getProductPropertyNumeric2());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    ps.setObject(index, record.getProductPropertyNumeric3());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    ps.setObject(index, record.getProductPropertyNumeric4());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    ps.setObject(index, record.getProductPropertyNumeric5());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric6()) {
                    ps.setObject(index, record.getProductPropertyNumeric6());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
                if(put || (record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_product_type10_product_feature_product_feature WHERE product_type10_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_product_type10_product_feature_product_feature VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getProductFeatureProductFeature()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
                if(put || (record.hasType() && !record.getType().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_resource_type_class WHERE resource_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasType() && !record.getType().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_resource_type_class VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getType()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
            });
        }

        return records;
    }

    public void deleteProductType10() {
        deleteProductType10(null);
    }    

    public void deleteProductType10(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM product_type10");
        String in = "";
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            in = "IN (" + sj.toString() + ")";
            querySB.append(" WHERE id " + in);
        }

        final String finalIn = in;
        final String query = querySB.toString();

        SqlUtils.run(connection, c -> {
            PreparedStatement ps = c.prepareStatement(query);
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_product_type10_product_feature_product_feature" + (finalIn.isEmpty() ? "" : " WHERE product_type10_id " + finalIn));
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_resource_type_class" + (finalIn.isEmpty() ? "" : " WHERE resource_id " + finalIn));
            ps.execute();
            ps.close();

        });
    }

    //==========================================================================
    // ProductType21

    private RSQLVisitor<Boolean, ProductType21> ProductType21Visitor = new RSQLVisitor<Boolean, ProductType21>() {

        @Override
        public Boolean visit(AndNode and, ProductType21 record) {
            for(Node n : and.getChildren()) {
                if(n instanceof AndNode) {
                    if(!visit((AndNode)n, record))
                        return false;
                } else if(n instanceof OrNode) {
                    if(!visit((OrNode)n, record))
                        return false;
                } else if(n instanceof ComparisonNode) {
                    if(!visit((ComparisonNode)n, record))
                        return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visit(OrNode or, ProductType21 record) {
            for(Node n : or.getChildren()) {
                if(n instanceof AndNode) {
                    if(visit((AndNode)n, record))
                        return true;
                } else if(n instanceof OrNode) {
                    if(visit((OrNode)n, record))
                        return true;
                } else if(n instanceof ComparisonNode) {
                    if(visit((ComparisonNode)n, record))
                        return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(ComparisonNode cn, ProductType21 record) {

            Set<String> actual = record.getStringSet(cn.getSelector());
            if(actual == null) {
                return false;
            }
            Set<String> expected = new HashSet<>(cn.getArguments());

            if(cn.getOperator().getSymbol().equals("=lt=") || 
               cn.getOperator().getSymbol().equals("=gt=") ||
               cn.getOperator().getSymbol().equals("=le=") ||
               cn.getOperator().getSymbol().equals("=ge=")) {
            
                double actualDouble = Double.parseDouble(actual.iterator().next());
                double expectedDouble = Double.parseDouble(expected.iterator().next());
                
                if(cn.getOperator().getSymbol().equals("=lt=")) {
                    return actualDouble < expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=gt=")) {
                    return actualDouble > expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=le=")) {
                    return actualDouble <= expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=ge=")) {
                    return actualDouble >= expectedDouble;
                }
            } else if(cn.getOperator().getSymbol().equals("==")) {
                return actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("!=")) {
                return !actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("=in=")) {
                return actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=out=")) {
                return !actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=regex=")) {
                String patternStr = expected.iterator().next();
                Pattern pattern = Pattern.compile(patternStr);
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        Matcher matcher = pattern.matcher(act);
                        if(matcher.find())
                            return true;
                    }
                }
                return false;
            } else if(cn.getOperator().getSymbol().equals("=lang=")) {
                String lang = expected.iterator().next();
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        if(act.endsWith("@" + lang))
                            return true;
                    }
                }
                return false;
            }

            return false;
        }
    };

    public List<ProductType21> selectProductType21() {
        return selectProductType21(null, null, null, null);
    }

    public List<ProductType21> selectProductType21(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM product_type21\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        if(limit != null) {
            querySB.append("\nLIMIT " + limit);
        }
        if(offset != null) {
            querySB.append("\nOFFSET " + offset);
        }

        String query = querySB.toString();

        List<ProductType21> result = SqlUtils.supply(connection, c -> {
            List<ProductType21> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                ProductType21 record = new ProductType21();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setLabel(SqlUtils.getString(rs, "label"));
                record.setComment(SqlUtils.getString(rs, "comment"));
                record.setProductPropertyTextual1(SqlUtils.getString(rs, "product_property_textual1"));
                record.setProductPropertyTextual2(SqlUtils.getString(rs, "product_property_textual2"));
                record.setProductPropertyTextual3(SqlUtils.getString(rs, "product_property_textual3"));
                record.setProductPropertyTextual4(SqlUtils.getString(rs, "product_property_textual4"));
                record.setProductPropertyTextual5(SqlUtils.getString(rs, "product_property_textual5"));
                record.setProductPropertyTextual6(SqlUtils.getString(rs, "product_property_textual6"));
                record.setPublisher(SqlUtils.getString(rs, "publisher"));
                record.setDate(SqlUtils.getLong(rs, "date"));
                record.setProducer(SqlUtils.getLong(rs, "producer"));
                record.setProductPropertyNumeric1(SqlUtils.getLong(rs, "product_property_numeric1"));
                record.setProductPropertyNumeric2(SqlUtils.getLong(rs, "product_property_numeric2"));
                record.setProductPropertyNumeric3(SqlUtils.getLong(rs, "product_property_numeric3"));
                record.setProductPropertyNumeric4(SqlUtils.getLong(rs, "product_property_numeric4"));
                record.setProductPropertyNumeric5(SqlUtils.getLong(rs, "product_property_numeric5"));
                record.setProductPropertyNumeric6(SqlUtils.getLong(rs, "product_property_numeric6"));

                record.setProductFeatureProductFeature(selectWithNM(record.getId(), "product_type21_id", "mn_product_type21_product_feature_product_feature", Long.class));
                record.setType(selectWithNM(record.getId(), "resource_id", "mn_resource_type_class", Long.class));
                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(ProductType21Visitor, record));
        }

        return result;
    }

    public List<ProductType21> insertProductType21(List<ProductType21> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO product_type21 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(ProductType21 record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getLabel());
                ps.setObject(3, record.getComment());
                ps.setObject(4, record.getProductPropertyTextual1());
                ps.setObject(5, record.getProductPropertyTextual2());
                ps.setObject(6, record.getProductPropertyTextual3());
                ps.setObject(7, record.getProductPropertyTextual4());
                ps.setObject(8, record.getProductPropertyTextual5());
                ps.setObject(9, record.getProductPropertyTextual6());
                ps.setObject(10, record.getPublisher());
                ps.setObject(11, record.getDate());
                ps.setObject(12, record.getProducer());
                ps.setObject(13, record.getProductPropertyNumeric1());
                ps.setObject(14, record.getProductPropertyNumeric2());
                ps.setObject(15, record.getProductPropertyNumeric3());
                ps.setObject(16, record.getProductPropertyNumeric4());
                ps.setObject(17, record.getProductPropertyNumeric5());
                ps.setObject(18, record.getProductPropertyNumeric6());
                ps.addBatch();
            }
            ps.executeBatch();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            for(int i = records.size()-1; i >= 0; i--) {
                //does only return the last generated id
                if(!generatedKeys.next())
                    break;

                long id = generatedKeys.getLong(1);
                records.get(i).setId(id);
            }
            c.commit();
            ps.close();

            boolean wasRecordsAdded = false;

            //n:m tables
            query = "INSERT INTO mn_product_type21_product_feature_product_feature VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType21 record : records) {
                if(!record.hasId() || !record.hasProductFeatureProductFeature() || record.getProductFeatureProductFeature().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getProductFeatureProductFeature()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;

            query = "INSERT INTO mn_resource_type_class VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType21 record : records) {
                if(!record.hasId() || !record.hasType() || record.getType().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getType()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;


            c.setAutoCommit(true);
        });

        return records;
    }

    public List<ProductType21> updateProductType21(List<ProductType21> records, boolean put) {    
        for(ProductType21 record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasLabel()) {
                    paramSJ.add("label = ?");
                }
                if(put || record.hasComment()) {
                    paramSJ.add("comment = ?");
                }
                if(put || record.hasProductPropertyTextual1()) {
                    paramSJ.add("product_property_textual1 = ?");
                }
                if(put || record.hasProductPropertyTextual2()) {
                    paramSJ.add("product_property_textual2 = ?");
                }
                if(put || record.hasProductPropertyTextual3()) {
                    paramSJ.add("product_property_textual3 = ?");
                }
                if(put || record.hasProductPropertyTextual4()) {
                    paramSJ.add("product_property_textual4 = ?");
                }
                if(put || record.hasProductPropertyTextual5()) {
                    paramSJ.add("product_property_textual5 = ?");
                }
                if(put || record.hasProductPropertyTextual6()) {
                    paramSJ.add("product_property_textual6 = ?");
                }
                if(put || record.hasPublisher()) {
                    paramSJ.add("publisher = ?");
                }
                if(put || record.hasDate()) {
                    paramSJ.add("date = ?");
                }
                if(put || record.hasProducer()) {
                    paramSJ.add("producer = ?");
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    paramSJ.add("product_property_numeric1 = ?");
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    paramSJ.add("product_property_numeric2 = ?");
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    paramSJ.add("product_property_numeric3 = ?");
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    paramSJ.add("product_property_numeric4 = ?");
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    paramSJ.add("product_property_numeric5 = ?");
                }
                if(put || record.hasProductPropertyNumeric6()) {
                    paramSJ.add("product_property_numeric6 = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE product_type21 SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasLabel()) {
                    ps.setObject(index, record.getLabel());
                    index++;
                }
                if(put || record.hasComment()) {
                    ps.setObject(index, record.getComment());
                    index++;
                }
                if(put || record.hasProductPropertyTextual1()) {
                    ps.setObject(index, record.getProductPropertyTextual1());
                    index++;
                }
                if(put || record.hasProductPropertyTextual2()) {
                    ps.setObject(index, record.getProductPropertyTextual2());
                    index++;
                }
                if(put || record.hasProductPropertyTextual3()) {
                    ps.setObject(index, record.getProductPropertyTextual3());
                    index++;
                }
                if(put || record.hasProductPropertyTextual4()) {
                    ps.setObject(index, record.getProductPropertyTextual4());
                    index++;
                }
                if(put || record.hasProductPropertyTextual5()) {
                    ps.setObject(index, record.getProductPropertyTextual5());
                    index++;
                }
                if(put || record.hasProductPropertyTextual6()) {
                    ps.setObject(index, record.getProductPropertyTextual6());
                    index++;
                }
                if(put || record.hasPublisher()) {
                    ps.setObject(index, record.getPublisher());
                    index++;
                }
                if(put || record.hasDate()) {
                    ps.setObject(index, record.getDate());
                    index++;
                }
                if(put || record.hasProducer()) {
                    ps.setObject(index, record.getProducer());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    ps.setObject(index, record.getProductPropertyNumeric1());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    ps.setObject(index, record.getProductPropertyNumeric2());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    ps.setObject(index, record.getProductPropertyNumeric3());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    ps.setObject(index, record.getProductPropertyNumeric4());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    ps.setObject(index, record.getProductPropertyNumeric5());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric6()) {
                    ps.setObject(index, record.getProductPropertyNumeric6());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
                if(put || (record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_product_type21_product_feature_product_feature WHERE product_type21_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_product_type21_product_feature_product_feature VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getProductFeatureProductFeature()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
                if(put || (record.hasType() && !record.getType().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_resource_type_class WHERE resource_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasType() && !record.getType().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_resource_type_class VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getType()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
            });
        }

        return records;
    }

    public void deleteProductType21() {
        deleteProductType21(null);
    }    

    public void deleteProductType21(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM product_type21");
        String in = "";
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            in = "IN (" + sj.toString() + ")";
            querySB.append(" WHERE id " + in);
        }

        final String finalIn = in;
        final String query = querySB.toString();

        SqlUtils.run(connection, c -> {
            PreparedStatement ps = c.prepareStatement(query);
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_product_type21_product_feature_product_feature" + (finalIn.isEmpty() ? "" : " WHERE product_type21_id " + finalIn));
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_resource_type_class" + (finalIn.isEmpty() ? "" : " WHERE resource_id " + finalIn));
            ps.execute();
            ps.close();

        });
    }

    //==========================================================================
    // Producer

    private RSQLVisitor<Boolean, Producer> ProducerVisitor = new RSQLVisitor<Boolean, Producer>() {

        @Override
        public Boolean visit(AndNode and, Producer record) {
            for(Node n : and.getChildren()) {
                if(n instanceof AndNode) {
                    if(!visit((AndNode)n, record))
                        return false;
                } else if(n instanceof OrNode) {
                    if(!visit((OrNode)n, record))
                        return false;
                } else if(n instanceof ComparisonNode) {
                    if(!visit((ComparisonNode)n, record))
                        return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visit(OrNode or, Producer record) {
            for(Node n : or.getChildren()) {
                if(n instanceof AndNode) {
                    if(visit((AndNode)n, record))
                        return true;
                } else if(n instanceof OrNode) {
                    if(visit((OrNode)n, record))
                        return true;
                } else if(n instanceof ComparisonNode) {
                    if(visit((ComparisonNode)n, record))
                        return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(ComparisonNode cn, Producer record) {

            Set<String> actual = record.getStringSet(cn.getSelector());
            if(actual == null) {
                return false;
            }
            Set<String> expected = new HashSet<>(cn.getArguments());

            if(cn.getOperator().getSymbol().equals("=lt=") || 
               cn.getOperator().getSymbol().equals("=gt=") ||
               cn.getOperator().getSymbol().equals("=le=") ||
               cn.getOperator().getSymbol().equals("=ge=")) {
            
                double actualDouble = Double.parseDouble(actual.iterator().next());
                double expectedDouble = Double.parseDouble(expected.iterator().next());
                
                if(cn.getOperator().getSymbol().equals("=lt=")) {
                    return actualDouble < expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=gt=")) {
                    return actualDouble > expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=le=")) {
                    return actualDouble <= expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=ge=")) {
                    return actualDouble >= expectedDouble;
                }
            } else if(cn.getOperator().getSymbol().equals("==")) {
                return actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("!=")) {
                return !actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("=in=")) {
                return actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=out=")) {
                return !actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=regex=")) {
                String patternStr = expected.iterator().next();
                Pattern pattern = Pattern.compile(patternStr);
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        Matcher matcher = pattern.matcher(act);
                        if(matcher.find())
                            return true;
                    }
                }
                return false;
            } else if(cn.getOperator().getSymbol().equals("=lang=")) {
                String lang = expected.iterator().next();
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        if(act.endsWith("@" + lang))
                            return true;
                    }
                }
                return false;
            }

            return false;
        }
    };

    public List<Producer> selectProducer() {
        return selectProducer(null, null, null, null);
    }

    public List<Producer> selectProducer(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM producer\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        if(limit != null) {
            querySB.append("\nLIMIT " + limit);
        }
        if(offset != null) {
            querySB.append("\nOFFSET " + offset);
        }

        String query = querySB.toString();

        List<Producer> result = SqlUtils.supply(connection, c -> {
            List<Producer> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                Producer record = new Producer();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setLabel(SqlUtils.getString(rs, "label"));
                record.setComment(SqlUtils.getString(rs, "comment"));
                record.setCountry(SqlUtils.getString(rs, "country"));
                record.setHomepage(SqlUtils.getString(rs, "homepage"));
                record.setPublisher(SqlUtils.getString(rs, "publisher"));
                record.setDate(SqlUtils.getLong(rs, "date"));

                record.setType(selectWithNM(record.getId(), "resource_id", "mn_resource_type_class", Long.class));
                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(ProducerVisitor, record));
        }

        return result;
    }

    public List<Producer> insertProducer(List<Producer> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO producer VALUES (?,?,?,?,?,?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(Producer record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getLabel());
                ps.setObject(3, record.getComment());
                ps.setObject(4, record.getCountry());
                ps.setObject(5, record.getHomepage());
                ps.setObject(6, record.getPublisher());
                ps.setObject(7, record.getDate());
                ps.addBatch();
            }
            ps.executeBatch();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            for(int i = records.size()-1; i >= 0; i--) {
                //does only return the last generated id
                if(!generatedKeys.next())
                    break;

                long id = generatedKeys.getLong(1);
                records.get(i).setId(id);
            }
            c.commit();
            ps.close();

            boolean wasRecordsAdded = false;

            //n:m tables
            query = "INSERT INTO mn_resource_type_class VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(Producer record : records) {
                if(!record.hasId() || !record.hasType() || record.getType().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getType()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;


            c.setAutoCommit(true);
        });

        return records;
    }

    public List<Producer> updateProducer(List<Producer> records, boolean put) {    
        for(Producer record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasLabel()) {
                    paramSJ.add("label = ?");
                }
                if(put || record.hasComment()) {
                    paramSJ.add("comment = ?");
                }
                if(put || record.hasCountry()) {
                    paramSJ.add("country = ?");
                }
                if(put || record.hasHomepage()) {
                    paramSJ.add("homepage = ?");
                }
                if(put || record.hasPublisher()) {
                    paramSJ.add("publisher = ?");
                }
                if(put || record.hasDate()) {
                    paramSJ.add("date = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE producer SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasLabel()) {
                    ps.setObject(index, record.getLabel());
                    index++;
                }
                if(put || record.hasComment()) {
                    ps.setObject(index, record.getComment());
                    index++;
                }
                if(put || record.hasCountry()) {
                    ps.setObject(index, record.getCountry());
                    index++;
                }
                if(put || record.hasHomepage()) {
                    ps.setObject(index, record.getHomepage());
                    index++;
                }
                if(put || record.hasPublisher()) {
                    ps.setObject(index, record.getPublisher());
                    index++;
                }
                if(put || record.hasDate()) {
                    ps.setObject(index, record.getDate());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
                if(put || (record.hasType() && !record.getType().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_resource_type_class WHERE resource_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasType() && !record.getType().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_resource_type_class VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getType()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
            });
        }

        return records;
    }

    public void deleteProducer() {
        deleteProducer(null);
    }    

    public void deleteProducer(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM producer");
        String in = "";
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            in = "IN (" + sj.toString() + ")";
            querySB.append(" WHERE id " + in);
        }

        final String finalIn = in;
        final String query = querySB.toString();

        SqlUtils.run(connection, c -> {
            PreparedStatement ps = c.prepareStatement(query);
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_resource_type_class" + (finalIn.isEmpty() ? "" : " WHERE resource_id " + finalIn));
            ps.execute();
            ps.close();

        });
    }

    //==========================================================================
    // Person

    private RSQLVisitor<Boolean, Person> PersonVisitor = new RSQLVisitor<Boolean, Person>() {

        @Override
        public Boolean visit(AndNode and, Person record) {
            for(Node n : and.getChildren()) {
                if(n instanceof AndNode) {
                    if(!visit((AndNode)n, record))
                        return false;
                } else if(n instanceof OrNode) {
                    if(!visit((OrNode)n, record))
                        return false;
                } else if(n instanceof ComparisonNode) {
                    if(!visit((ComparisonNode)n, record))
                        return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visit(OrNode or, Person record) {
            for(Node n : or.getChildren()) {
                if(n instanceof AndNode) {
                    if(visit((AndNode)n, record))
                        return true;
                } else if(n instanceof OrNode) {
                    if(visit((OrNode)n, record))
                        return true;
                } else if(n instanceof ComparisonNode) {
                    if(visit((ComparisonNode)n, record))
                        return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(ComparisonNode cn, Person record) {

            Set<String> actual = record.getStringSet(cn.getSelector());
            if(actual == null) {
                return false;
            }
            Set<String> expected = new HashSet<>(cn.getArguments());

            if(cn.getOperator().getSymbol().equals("=lt=") || 
               cn.getOperator().getSymbol().equals("=gt=") ||
               cn.getOperator().getSymbol().equals("=le=") ||
               cn.getOperator().getSymbol().equals("=ge=")) {
            
                double actualDouble = Double.parseDouble(actual.iterator().next());
                double expectedDouble = Double.parseDouble(expected.iterator().next());
                
                if(cn.getOperator().getSymbol().equals("=lt=")) {
                    return actualDouble < expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=gt=")) {
                    return actualDouble > expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=le=")) {
                    return actualDouble <= expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=ge=")) {
                    return actualDouble >= expectedDouble;
                }
            } else if(cn.getOperator().getSymbol().equals("==")) {
                return actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("!=")) {
                return !actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("=in=")) {
                return actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=out=")) {
                return !actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=regex=")) {
                String patternStr = expected.iterator().next();
                Pattern pattern = Pattern.compile(patternStr);
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        Matcher matcher = pattern.matcher(act);
                        if(matcher.find())
                            return true;
                    }
                }
                return false;
            } else if(cn.getOperator().getSymbol().equals("=lang=")) {
                String lang = expected.iterator().next();
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        if(act.endsWith("@" + lang))
                            return true;
                    }
                }
                return false;
            }

            return false;
        }
    };

    public List<Person> selectPerson() {
        return selectPerson(null, null, null, null);
    }

    public List<Person> selectPerson(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM person\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        if(limit != null) {
            querySB.append("\nLIMIT " + limit);
        }
        if(offset != null) {
            querySB.append("\nOFFSET " + offset);
        }

        String query = querySB.toString();

        List<Person> result = SqlUtils.supply(connection, c -> {
            List<Person> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                Person record = new Person();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setName(SqlUtils.getString(rs, "name"));
                record.setCountry(SqlUtils.getString(rs, "country"));
                record.setMboxsha1sum(SqlUtils.getString(rs, "mboxsha1sum"));
                record.setPublisher(SqlUtils.getString(rs, "publisher"));
                record.setDate(SqlUtils.getLong(rs, "date"));

                record.setType(selectWithNM(record.getId(), "resource_id", "mn_resource_type_class", Long.class));
                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(PersonVisitor, record));
        }

        return result;
    }

    public List<Person> insertPerson(List<Person> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO person VALUES (?,?,?,?,?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(Person record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getName());
                ps.setObject(3, record.getCountry());
                ps.setObject(4, record.getMboxsha1sum());
                ps.setObject(5, record.getPublisher());
                ps.setObject(6, record.getDate());
                ps.addBatch();
            }
            ps.executeBatch();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            for(int i = records.size()-1; i >= 0; i--) {
                //does only return the last generated id
                if(!generatedKeys.next())
                    break;

                long id = generatedKeys.getLong(1);
                records.get(i).setId(id);
            }
            c.commit();
            ps.close();

            boolean wasRecordsAdded = false;

            //n:m tables
            query = "INSERT INTO mn_resource_type_class VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(Person record : records) {
                if(!record.hasId() || !record.hasType() || record.getType().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getType()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;


            c.setAutoCommit(true);
        });

        return records;
    }

    public List<Person> updatePerson(List<Person> records, boolean put) {    
        for(Person record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasName()) {
                    paramSJ.add("name = ?");
                }
                if(put || record.hasCountry()) {
                    paramSJ.add("country = ?");
                }
                if(put || record.hasMboxsha1sum()) {
                    paramSJ.add("mboxsha1sum = ?");
                }
                if(put || record.hasPublisher()) {
                    paramSJ.add("publisher = ?");
                }
                if(put || record.hasDate()) {
                    paramSJ.add("date = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE person SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasName()) {
                    ps.setObject(index, record.getName());
                    index++;
                }
                if(put || record.hasCountry()) {
                    ps.setObject(index, record.getCountry());
                    index++;
                }
                if(put || record.hasMboxsha1sum()) {
                    ps.setObject(index, record.getMboxsha1sum());
                    index++;
                }
                if(put || record.hasPublisher()) {
                    ps.setObject(index, record.getPublisher());
                    index++;
                }
                if(put || record.hasDate()) {
                    ps.setObject(index, record.getDate());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
                if(put || (record.hasType() && !record.getType().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_resource_type_class WHERE resource_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasType() && !record.getType().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_resource_type_class VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getType()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
            });
        }

        return records;
    }

    public void deletePerson() {
        deletePerson(null);
    }    

    public void deletePerson(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM person");
        String in = "";
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            in = "IN (" + sj.toString() + ")";
            querySB.append(" WHERE id " + in);
        }

        final String finalIn = in;
        final String query = querySB.toString();

        SqlUtils.run(connection, c -> {
            PreparedStatement ps = c.prepareStatement(query);
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_resource_type_class" + (finalIn.isEmpty() ? "" : " WHERE resource_id " + finalIn));
            ps.execute();
            ps.close();

        });
    }

    //==========================================================================
    // ProductType6

    private RSQLVisitor<Boolean, ProductType6> ProductType6Visitor = new RSQLVisitor<Boolean, ProductType6>() {

        @Override
        public Boolean visit(AndNode and, ProductType6 record) {
            for(Node n : and.getChildren()) {
                if(n instanceof AndNode) {
                    if(!visit((AndNode)n, record))
                        return false;
                } else if(n instanceof OrNode) {
                    if(!visit((OrNode)n, record))
                        return false;
                } else if(n instanceof ComparisonNode) {
                    if(!visit((ComparisonNode)n, record))
                        return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visit(OrNode or, ProductType6 record) {
            for(Node n : or.getChildren()) {
                if(n instanceof AndNode) {
                    if(visit((AndNode)n, record))
                        return true;
                } else if(n instanceof OrNode) {
                    if(visit((OrNode)n, record))
                        return true;
                } else if(n instanceof ComparisonNode) {
                    if(visit((ComparisonNode)n, record))
                        return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(ComparisonNode cn, ProductType6 record) {

            Set<String> actual = record.getStringSet(cn.getSelector());
            if(actual == null) {
                return false;
            }
            Set<String> expected = new HashSet<>(cn.getArguments());

            if(cn.getOperator().getSymbol().equals("=lt=") || 
               cn.getOperator().getSymbol().equals("=gt=") ||
               cn.getOperator().getSymbol().equals("=le=") ||
               cn.getOperator().getSymbol().equals("=ge=")) {
            
                double actualDouble = Double.parseDouble(actual.iterator().next());
                double expectedDouble = Double.parseDouble(expected.iterator().next());
                
                if(cn.getOperator().getSymbol().equals("=lt=")) {
                    return actualDouble < expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=gt=")) {
                    return actualDouble > expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=le=")) {
                    return actualDouble <= expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=ge=")) {
                    return actualDouble >= expectedDouble;
                }
            } else if(cn.getOperator().getSymbol().equals("==")) {
                return actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("!=")) {
                return !actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("=in=")) {
                return actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=out=")) {
                return !actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=regex=")) {
                String patternStr = expected.iterator().next();
                Pattern pattern = Pattern.compile(patternStr);
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        Matcher matcher = pattern.matcher(act);
                        if(matcher.find())
                            return true;
                    }
                }
                return false;
            } else if(cn.getOperator().getSymbol().equals("=lang=")) {
                String lang = expected.iterator().next();
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        if(act.endsWith("@" + lang))
                            return true;
                    }
                }
                return false;
            }

            return false;
        }
    };

    public List<ProductType6> selectProductType6() {
        return selectProductType6(null, null, null, null);
    }

    public List<ProductType6> selectProductType6(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM product_type6\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        if(limit != null) {
            querySB.append("\nLIMIT " + limit);
        }
        if(offset != null) {
            querySB.append("\nOFFSET " + offset);
        }

        String query = querySB.toString();

        List<ProductType6> result = SqlUtils.supply(connection, c -> {
            List<ProductType6> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                ProductType6 record = new ProductType6();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setLabel(SqlUtils.getString(rs, "label"));
                record.setComment(SqlUtils.getString(rs, "comment"));
                record.setProductPropertyTextual1(SqlUtils.getString(rs, "product_property_textual1"));
                record.setProductPropertyTextual2(SqlUtils.getString(rs, "product_property_textual2"));
                record.setProductPropertyTextual3(SqlUtils.getString(rs, "product_property_textual3"));
                record.setProductPropertyTextual4(SqlUtils.getString(rs, "product_property_textual4"));
                record.setProductPropertyTextual5(SqlUtils.getString(rs, "product_property_textual5"));
                record.setProductPropertyTextual6(SqlUtils.getString(rs, "product_property_textual6"));
                record.setPublisher(SqlUtils.getString(rs, "publisher"));
                record.setDate(SqlUtils.getLong(rs, "date"));
                record.setProducer(SqlUtils.getLong(rs, "producer"));
                record.setProductPropertyNumeric1(SqlUtils.getLong(rs, "product_property_numeric1"));
                record.setProductPropertyNumeric2(SqlUtils.getLong(rs, "product_property_numeric2"));
                record.setProductPropertyNumeric3(SqlUtils.getLong(rs, "product_property_numeric3"));
                record.setProductPropertyNumeric4(SqlUtils.getLong(rs, "product_property_numeric4"));
                record.setProductPropertyNumeric5(SqlUtils.getLong(rs, "product_property_numeric5"));
                record.setProductPropertyNumeric6(SqlUtils.getLong(rs, "product_property_numeric6"));

                record.setProductFeatureProductFeature(selectWithNM(record.getId(), "product_type6_id", "mn_product_type6_product_feature_product_feature", Long.class));
                record.setType(selectWithNM(record.getId(), "resource_id", "mn_resource_type_class", Long.class));
                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(ProductType6Visitor, record));
        }

        return result;
    }

    public List<ProductType6> insertProductType6(List<ProductType6> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO product_type6 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(ProductType6 record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getLabel());
                ps.setObject(3, record.getComment());
                ps.setObject(4, record.getProductPropertyTextual1());
                ps.setObject(5, record.getProductPropertyTextual2());
                ps.setObject(6, record.getProductPropertyTextual3());
                ps.setObject(7, record.getProductPropertyTextual4());
                ps.setObject(8, record.getProductPropertyTextual5());
                ps.setObject(9, record.getProductPropertyTextual6());
                ps.setObject(10, record.getPublisher());
                ps.setObject(11, record.getDate());
                ps.setObject(12, record.getProducer());
                ps.setObject(13, record.getProductPropertyNumeric1());
                ps.setObject(14, record.getProductPropertyNumeric2());
                ps.setObject(15, record.getProductPropertyNumeric3());
                ps.setObject(16, record.getProductPropertyNumeric4());
                ps.setObject(17, record.getProductPropertyNumeric5());
                ps.setObject(18, record.getProductPropertyNumeric6());
                ps.addBatch();
            }
            ps.executeBatch();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            for(int i = records.size()-1; i >= 0; i--) {
                //does only return the last generated id
                if(!generatedKeys.next())
                    break;

                long id = generatedKeys.getLong(1);
                records.get(i).setId(id);
            }
            c.commit();
            ps.close();

            boolean wasRecordsAdded = false;

            //n:m tables
            query = "INSERT INTO mn_product_type6_product_feature_product_feature VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType6 record : records) {
                if(!record.hasId() || !record.hasProductFeatureProductFeature() || record.getProductFeatureProductFeature().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getProductFeatureProductFeature()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;

            query = "INSERT INTO mn_resource_type_class VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType6 record : records) {
                if(!record.hasId() || !record.hasType() || record.getType().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getType()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;


            c.setAutoCommit(true);
        });

        return records;
    }

    public List<ProductType6> updateProductType6(List<ProductType6> records, boolean put) {    
        for(ProductType6 record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasLabel()) {
                    paramSJ.add("label = ?");
                }
                if(put || record.hasComment()) {
                    paramSJ.add("comment = ?");
                }
                if(put || record.hasProductPropertyTextual1()) {
                    paramSJ.add("product_property_textual1 = ?");
                }
                if(put || record.hasProductPropertyTextual2()) {
                    paramSJ.add("product_property_textual2 = ?");
                }
                if(put || record.hasProductPropertyTextual3()) {
                    paramSJ.add("product_property_textual3 = ?");
                }
                if(put || record.hasProductPropertyTextual4()) {
                    paramSJ.add("product_property_textual4 = ?");
                }
                if(put || record.hasProductPropertyTextual5()) {
                    paramSJ.add("product_property_textual5 = ?");
                }
                if(put || record.hasProductPropertyTextual6()) {
                    paramSJ.add("product_property_textual6 = ?");
                }
                if(put || record.hasPublisher()) {
                    paramSJ.add("publisher = ?");
                }
                if(put || record.hasDate()) {
                    paramSJ.add("date = ?");
                }
                if(put || record.hasProducer()) {
                    paramSJ.add("producer = ?");
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    paramSJ.add("product_property_numeric1 = ?");
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    paramSJ.add("product_property_numeric2 = ?");
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    paramSJ.add("product_property_numeric3 = ?");
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    paramSJ.add("product_property_numeric4 = ?");
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    paramSJ.add("product_property_numeric5 = ?");
                }
                if(put || record.hasProductPropertyNumeric6()) {
                    paramSJ.add("product_property_numeric6 = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE product_type6 SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasLabel()) {
                    ps.setObject(index, record.getLabel());
                    index++;
                }
                if(put || record.hasComment()) {
                    ps.setObject(index, record.getComment());
                    index++;
                }
                if(put || record.hasProductPropertyTextual1()) {
                    ps.setObject(index, record.getProductPropertyTextual1());
                    index++;
                }
                if(put || record.hasProductPropertyTextual2()) {
                    ps.setObject(index, record.getProductPropertyTextual2());
                    index++;
                }
                if(put || record.hasProductPropertyTextual3()) {
                    ps.setObject(index, record.getProductPropertyTextual3());
                    index++;
                }
                if(put || record.hasProductPropertyTextual4()) {
                    ps.setObject(index, record.getProductPropertyTextual4());
                    index++;
                }
                if(put || record.hasProductPropertyTextual5()) {
                    ps.setObject(index, record.getProductPropertyTextual5());
                    index++;
                }
                if(put || record.hasProductPropertyTextual6()) {
                    ps.setObject(index, record.getProductPropertyTextual6());
                    index++;
                }
                if(put || record.hasPublisher()) {
                    ps.setObject(index, record.getPublisher());
                    index++;
                }
                if(put || record.hasDate()) {
                    ps.setObject(index, record.getDate());
                    index++;
                }
                if(put || record.hasProducer()) {
                    ps.setObject(index, record.getProducer());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    ps.setObject(index, record.getProductPropertyNumeric1());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    ps.setObject(index, record.getProductPropertyNumeric2());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    ps.setObject(index, record.getProductPropertyNumeric3());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    ps.setObject(index, record.getProductPropertyNumeric4());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    ps.setObject(index, record.getProductPropertyNumeric5());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric6()) {
                    ps.setObject(index, record.getProductPropertyNumeric6());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
                if(put || (record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_product_type6_product_feature_product_feature WHERE product_type6_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_product_type6_product_feature_product_feature VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getProductFeatureProductFeature()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
                if(put || (record.hasType() && !record.getType().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_resource_type_class WHERE resource_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasType() && !record.getType().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_resource_type_class VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getType()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
            });
        }

        return records;
    }

    public void deleteProductType6() {
        deleteProductType6(null);
    }    

    public void deleteProductType6(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM product_type6");
        String in = "";
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            in = "IN (" + sj.toString() + ")";
            querySB.append(" WHERE id " + in);
        }

        final String finalIn = in;
        final String query = querySB.toString();

        SqlUtils.run(connection, c -> {
            PreparedStatement ps = c.prepareStatement(query);
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_product_type6_product_feature_product_feature" + (finalIn.isEmpty() ? "" : " WHERE product_type6_id " + finalIn));
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_resource_type_class" + (finalIn.isEmpty() ? "" : " WHERE resource_id " + finalIn));
            ps.execute();
            ps.close();

        });
    }

    //==========================================================================
    // ProductType7

    private RSQLVisitor<Boolean, ProductType7> ProductType7Visitor = new RSQLVisitor<Boolean, ProductType7>() {

        @Override
        public Boolean visit(AndNode and, ProductType7 record) {
            for(Node n : and.getChildren()) {
                if(n instanceof AndNode) {
                    if(!visit((AndNode)n, record))
                        return false;
                } else if(n instanceof OrNode) {
                    if(!visit((OrNode)n, record))
                        return false;
                } else if(n instanceof ComparisonNode) {
                    if(!visit((ComparisonNode)n, record))
                        return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visit(OrNode or, ProductType7 record) {
            for(Node n : or.getChildren()) {
                if(n instanceof AndNode) {
                    if(visit((AndNode)n, record))
                        return true;
                } else if(n instanceof OrNode) {
                    if(visit((OrNode)n, record))
                        return true;
                } else if(n instanceof ComparisonNode) {
                    if(visit((ComparisonNode)n, record))
                        return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(ComparisonNode cn, ProductType7 record) {

            Set<String> actual = record.getStringSet(cn.getSelector());
            if(actual == null) {
                return false;
            }
            Set<String> expected = new HashSet<>(cn.getArguments());

            if(cn.getOperator().getSymbol().equals("=lt=") || 
               cn.getOperator().getSymbol().equals("=gt=") ||
               cn.getOperator().getSymbol().equals("=le=") ||
               cn.getOperator().getSymbol().equals("=ge=")) {
            
                double actualDouble = Double.parseDouble(actual.iterator().next());
                double expectedDouble = Double.parseDouble(expected.iterator().next());
                
                if(cn.getOperator().getSymbol().equals("=lt=")) {
                    return actualDouble < expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=gt=")) {
                    return actualDouble > expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=le=")) {
                    return actualDouble <= expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=ge=")) {
                    return actualDouble >= expectedDouble;
                }
            } else if(cn.getOperator().getSymbol().equals("==")) {
                return actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("!=")) {
                return !actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("=in=")) {
                return actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=out=")) {
                return !actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=regex=")) {
                String patternStr = expected.iterator().next();
                Pattern pattern = Pattern.compile(patternStr);
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        Matcher matcher = pattern.matcher(act);
                        if(matcher.find())
                            return true;
                    }
                }
                return false;
            } else if(cn.getOperator().getSymbol().equals("=lang=")) {
                String lang = expected.iterator().next();
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        if(act.endsWith("@" + lang))
                            return true;
                    }
                }
                return false;
            }

            return false;
        }
    };

    public List<ProductType7> selectProductType7() {
        return selectProductType7(null, null, null, null);
    }

    public List<ProductType7> selectProductType7(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM product_type7\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        if(limit != null) {
            querySB.append("\nLIMIT " + limit);
        }
        if(offset != null) {
            querySB.append("\nOFFSET " + offset);
        }

        String query = querySB.toString();

        List<ProductType7> result = SqlUtils.supply(connection, c -> {
            List<ProductType7> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                ProductType7 record = new ProductType7();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setLabel(SqlUtils.getString(rs, "label"));
                record.setComment(SqlUtils.getString(rs, "comment"));
                record.setProductPropertyTextual1(SqlUtils.getString(rs, "product_property_textual1"));
                record.setProductPropertyTextual2(SqlUtils.getString(rs, "product_property_textual2"));
                record.setProductPropertyTextual3(SqlUtils.getString(rs, "product_property_textual3"));
                record.setProductPropertyTextual4(SqlUtils.getString(rs, "product_property_textual4"));
                record.setProductPropertyTextual5(SqlUtils.getString(rs, "product_property_textual5"));
                record.setProductPropertyTextual6(SqlUtils.getString(rs, "product_property_textual6"));
                record.setPublisher(SqlUtils.getString(rs, "publisher"));
                record.setDate(SqlUtils.getLong(rs, "date"));
                record.setProducer(SqlUtils.getLong(rs, "producer"));
                record.setProductPropertyNumeric1(SqlUtils.getLong(rs, "product_property_numeric1"));
                record.setProductPropertyNumeric2(SqlUtils.getLong(rs, "product_property_numeric2"));
                record.setProductPropertyNumeric3(SqlUtils.getLong(rs, "product_property_numeric3"));
                record.setProductPropertyNumeric4(SqlUtils.getLong(rs, "product_property_numeric4"));
                record.setProductPropertyNumeric5(SqlUtils.getLong(rs, "product_property_numeric5"));
                record.setProductPropertyNumeric6(SqlUtils.getLong(rs, "product_property_numeric6"));

                record.setProductFeatureProductFeature(selectWithNM(record.getId(), "product_type7_id", "mn_product_type7_product_feature_product_feature", Long.class));
                record.setType(selectWithNM(record.getId(), "resource_id", "mn_resource_type_class", Long.class));
                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(ProductType7Visitor, record));
        }

        return result;
    }

    public List<ProductType7> insertProductType7(List<ProductType7> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO product_type7 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(ProductType7 record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getLabel());
                ps.setObject(3, record.getComment());
                ps.setObject(4, record.getProductPropertyTextual1());
                ps.setObject(5, record.getProductPropertyTextual2());
                ps.setObject(6, record.getProductPropertyTextual3());
                ps.setObject(7, record.getProductPropertyTextual4());
                ps.setObject(8, record.getProductPropertyTextual5());
                ps.setObject(9, record.getProductPropertyTextual6());
                ps.setObject(10, record.getPublisher());
                ps.setObject(11, record.getDate());
                ps.setObject(12, record.getProducer());
                ps.setObject(13, record.getProductPropertyNumeric1());
                ps.setObject(14, record.getProductPropertyNumeric2());
                ps.setObject(15, record.getProductPropertyNumeric3());
                ps.setObject(16, record.getProductPropertyNumeric4());
                ps.setObject(17, record.getProductPropertyNumeric5());
                ps.setObject(18, record.getProductPropertyNumeric6());
                ps.addBatch();
            }
            ps.executeBatch();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            for(int i = records.size()-1; i >= 0; i--) {
                //does only return the last generated id
                if(!generatedKeys.next())
                    break;

                long id = generatedKeys.getLong(1);
                records.get(i).setId(id);
            }
            c.commit();
            ps.close();

            boolean wasRecordsAdded = false;

            //n:m tables
            query = "INSERT INTO mn_product_type7_product_feature_product_feature VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType7 record : records) {
                if(!record.hasId() || !record.hasProductFeatureProductFeature() || record.getProductFeatureProductFeature().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getProductFeatureProductFeature()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;

            query = "INSERT INTO mn_resource_type_class VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType7 record : records) {
                if(!record.hasId() || !record.hasType() || record.getType().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getType()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;


            c.setAutoCommit(true);
        });

        return records;
    }

    public List<ProductType7> updateProductType7(List<ProductType7> records, boolean put) {    
        for(ProductType7 record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasLabel()) {
                    paramSJ.add("label = ?");
                }
                if(put || record.hasComment()) {
                    paramSJ.add("comment = ?");
                }
                if(put || record.hasProductPropertyTextual1()) {
                    paramSJ.add("product_property_textual1 = ?");
                }
                if(put || record.hasProductPropertyTextual2()) {
                    paramSJ.add("product_property_textual2 = ?");
                }
                if(put || record.hasProductPropertyTextual3()) {
                    paramSJ.add("product_property_textual3 = ?");
                }
                if(put || record.hasProductPropertyTextual4()) {
                    paramSJ.add("product_property_textual4 = ?");
                }
                if(put || record.hasProductPropertyTextual5()) {
                    paramSJ.add("product_property_textual5 = ?");
                }
                if(put || record.hasProductPropertyTextual6()) {
                    paramSJ.add("product_property_textual6 = ?");
                }
                if(put || record.hasPublisher()) {
                    paramSJ.add("publisher = ?");
                }
                if(put || record.hasDate()) {
                    paramSJ.add("date = ?");
                }
                if(put || record.hasProducer()) {
                    paramSJ.add("producer = ?");
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    paramSJ.add("product_property_numeric1 = ?");
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    paramSJ.add("product_property_numeric2 = ?");
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    paramSJ.add("product_property_numeric3 = ?");
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    paramSJ.add("product_property_numeric4 = ?");
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    paramSJ.add("product_property_numeric5 = ?");
                }
                if(put || record.hasProductPropertyNumeric6()) {
                    paramSJ.add("product_property_numeric6 = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE product_type7 SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasLabel()) {
                    ps.setObject(index, record.getLabel());
                    index++;
                }
                if(put || record.hasComment()) {
                    ps.setObject(index, record.getComment());
                    index++;
                }
                if(put || record.hasProductPropertyTextual1()) {
                    ps.setObject(index, record.getProductPropertyTextual1());
                    index++;
                }
                if(put || record.hasProductPropertyTextual2()) {
                    ps.setObject(index, record.getProductPropertyTextual2());
                    index++;
                }
                if(put || record.hasProductPropertyTextual3()) {
                    ps.setObject(index, record.getProductPropertyTextual3());
                    index++;
                }
                if(put || record.hasProductPropertyTextual4()) {
                    ps.setObject(index, record.getProductPropertyTextual4());
                    index++;
                }
                if(put || record.hasProductPropertyTextual5()) {
                    ps.setObject(index, record.getProductPropertyTextual5());
                    index++;
                }
                if(put || record.hasProductPropertyTextual6()) {
                    ps.setObject(index, record.getProductPropertyTextual6());
                    index++;
                }
                if(put || record.hasPublisher()) {
                    ps.setObject(index, record.getPublisher());
                    index++;
                }
                if(put || record.hasDate()) {
                    ps.setObject(index, record.getDate());
                    index++;
                }
                if(put || record.hasProducer()) {
                    ps.setObject(index, record.getProducer());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    ps.setObject(index, record.getProductPropertyNumeric1());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    ps.setObject(index, record.getProductPropertyNumeric2());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    ps.setObject(index, record.getProductPropertyNumeric3());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    ps.setObject(index, record.getProductPropertyNumeric4());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    ps.setObject(index, record.getProductPropertyNumeric5());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric6()) {
                    ps.setObject(index, record.getProductPropertyNumeric6());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
                if(put || (record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_product_type7_product_feature_product_feature WHERE product_type7_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_product_type7_product_feature_product_feature VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getProductFeatureProductFeature()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
                if(put || (record.hasType() && !record.getType().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_resource_type_class WHERE resource_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasType() && !record.getType().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_resource_type_class VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getType()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
            });
        }

        return records;
    }

    public void deleteProductType7() {
        deleteProductType7(null);
    }    

    public void deleteProductType7(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM product_type7");
        String in = "";
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            in = "IN (" + sj.toString() + ")";
            querySB.append(" WHERE id " + in);
        }

        final String finalIn = in;
        final String query = querySB.toString();

        SqlUtils.run(connection, c -> {
            PreparedStatement ps = c.prepareStatement(query);
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_product_type7_product_feature_product_feature" + (finalIn.isEmpty() ? "" : " WHERE product_type7_id " + finalIn));
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_resource_type_class" + (finalIn.isEmpty() ? "" : " WHERE resource_id " + finalIn));
            ps.execute();
            ps.close();

        });
    }

    //==========================================================================
    // ProductType8

    private RSQLVisitor<Boolean, ProductType8> ProductType8Visitor = new RSQLVisitor<Boolean, ProductType8>() {

        @Override
        public Boolean visit(AndNode and, ProductType8 record) {
            for(Node n : and.getChildren()) {
                if(n instanceof AndNode) {
                    if(!visit((AndNode)n, record))
                        return false;
                } else if(n instanceof OrNode) {
                    if(!visit((OrNode)n, record))
                        return false;
                } else if(n instanceof ComparisonNode) {
                    if(!visit((ComparisonNode)n, record))
                        return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visit(OrNode or, ProductType8 record) {
            for(Node n : or.getChildren()) {
                if(n instanceof AndNode) {
                    if(visit((AndNode)n, record))
                        return true;
                } else if(n instanceof OrNode) {
                    if(visit((OrNode)n, record))
                        return true;
                } else if(n instanceof ComparisonNode) {
                    if(visit((ComparisonNode)n, record))
                        return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(ComparisonNode cn, ProductType8 record) {

            Set<String> actual = record.getStringSet(cn.getSelector());
            if(actual == null) {
                return false;
            }
            Set<String> expected = new HashSet<>(cn.getArguments());

            if(cn.getOperator().getSymbol().equals("=lt=") || 
               cn.getOperator().getSymbol().equals("=gt=") ||
               cn.getOperator().getSymbol().equals("=le=") ||
               cn.getOperator().getSymbol().equals("=ge=")) {
            
                double actualDouble = Double.parseDouble(actual.iterator().next());
                double expectedDouble = Double.parseDouble(expected.iterator().next());
                
                if(cn.getOperator().getSymbol().equals("=lt=")) {
                    return actualDouble < expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=gt=")) {
                    return actualDouble > expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=le=")) {
                    return actualDouble <= expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=ge=")) {
                    return actualDouble >= expectedDouble;
                }
            } else if(cn.getOperator().getSymbol().equals("==")) {
                return actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("!=")) {
                return !actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("=in=")) {
                return actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=out=")) {
                return !actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=regex=")) {
                String patternStr = expected.iterator().next();
                Pattern pattern = Pattern.compile(patternStr);
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        Matcher matcher = pattern.matcher(act);
                        if(matcher.find())
                            return true;
                    }
                }
                return false;
            } else if(cn.getOperator().getSymbol().equals("=lang=")) {
                String lang = expected.iterator().next();
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        if(act.endsWith("@" + lang))
                            return true;
                    }
                }
                return false;
            }

            return false;
        }
    };

    public List<ProductType8> selectProductType8() {
        return selectProductType8(null, null, null, null);
    }

    public List<ProductType8> selectProductType8(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM product_type8\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        if(limit != null) {
            querySB.append("\nLIMIT " + limit);
        }
        if(offset != null) {
            querySB.append("\nOFFSET " + offset);
        }

        String query = querySB.toString();

        List<ProductType8> result = SqlUtils.supply(connection, c -> {
            List<ProductType8> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                ProductType8 record = new ProductType8();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setLabel(SqlUtils.getString(rs, "label"));
                record.setComment(SqlUtils.getString(rs, "comment"));
                record.setProductPropertyTextual1(SqlUtils.getString(rs, "product_property_textual1"));
                record.setProductPropertyTextual2(SqlUtils.getString(rs, "product_property_textual2"));
                record.setProductPropertyTextual3(SqlUtils.getString(rs, "product_property_textual3"));
                record.setProductPropertyTextual4(SqlUtils.getString(rs, "product_property_textual4"));
                record.setProductPropertyTextual5(SqlUtils.getString(rs, "product_property_textual5"));
                record.setProductPropertyTextual6(SqlUtils.getString(rs, "product_property_textual6"));
                record.setPublisher(SqlUtils.getString(rs, "publisher"));
                record.setDate(SqlUtils.getLong(rs, "date"));
                record.setProducer(SqlUtils.getLong(rs, "producer"));
                record.setProductPropertyNumeric1(SqlUtils.getLong(rs, "product_property_numeric1"));
                record.setProductPropertyNumeric2(SqlUtils.getLong(rs, "product_property_numeric2"));
                record.setProductPropertyNumeric3(SqlUtils.getLong(rs, "product_property_numeric3"));
                record.setProductPropertyNumeric4(SqlUtils.getLong(rs, "product_property_numeric4"));
                record.setProductPropertyNumeric5(SqlUtils.getLong(rs, "product_property_numeric5"));

                record.setProductFeatureProductFeature(selectWithNM(record.getId(), "product_type8_id", "mn_product_type8_product_feature_product_feature", Long.class));
                record.setType(selectWithNM(record.getId(), "resource_id", "mn_resource_type_class", Long.class));
                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(ProductType8Visitor, record));
        }

        return result;
    }

    public List<ProductType8> insertProductType8(List<ProductType8> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO product_type8 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(ProductType8 record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getLabel());
                ps.setObject(3, record.getComment());
                ps.setObject(4, record.getProductPropertyTextual1());
                ps.setObject(5, record.getProductPropertyTextual2());
                ps.setObject(6, record.getProductPropertyTextual3());
                ps.setObject(7, record.getProductPropertyTextual4());
                ps.setObject(8, record.getProductPropertyTextual5());
                ps.setObject(9, record.getProductPropertyTextual6());
                ps.setObject(10, record.getPublisher());
                ps.setObject(11, record.getDate());
                ps.setObject(12, record.getProducer());
                ps.setObject(13, record.getProductPropertyNumeric1());
                ps.setObject(14, record.getProductPropertyNumeric2());
                ps.setObject(15, record.getProductPropertyNumeric3());
                ps.setObject(16, record.getProductPropertyNumeric4());
                ps.setObject(17, record.getProductPropertyNumeric5());
                ps.addBatch();
            }
            ps.executeBatch();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            for(int i = records.size()-1; i >= 0; i--) {
                //does only return the last generated id
                if(!generatedKeys.next())
                    break;

                long id = generatedKeys.getLong(1);
                records.get(i).setId(id);
            }
            c.commit();
            ps.close();

            boolean wasRecordsAdded = false;

            //n:m tables
            query = "INSERT INTO mn_product_type8_product_feature_product_feature VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType8 record : records) {
                if(!record.hasId() || !record.hasProductFeatureProductFeature() || record.getProductFeatureProductFeature().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getProductFeatureProductFeature()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;

            query = "INSERT INTO mn_resource_type_class VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType8 record : records) {
                if(!record.hasId() || !record.hasType() || record.getType().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getType()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;


            c.setAutoCommit(true);
        });

        return records;
    }

    public List<ProductType8> updateProductType8(List<ProductType8> records, boolean put) {    
        for(ProductType8 record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasLabel()) {
                    paramSJ.add("label = ?");
                }
                if(put || record.hasComment()) {
                    paramSJ.add("comment = ?");
                }
                if(put || record.hasProductPropertyTextual1()) {
                    paramSJ.add("product_property_textual1 = ?");
                }
                if(put || record.hasProductPropertyTextual2()) {
                    paramSJ.add("product_property_textual2 = ?");
                }
                if(put || record.hasProductPropertyTextual3()) {
                    paramSJ.add("product_property_textual3 = ?");
                }
                if(put || record.hasProductPropertyTextual4()) {
                    paramSJ.add("product_property_textual4 = ?");
                }
                if(put || record.hasProductPropertyTextual5()) {
                    paramSJ.add("product_property_textual5 = ?");
                }
                if(put || record.hasProductPropertyTextual6()) {
                    paramSJ.add("product_property_textual6 = ?");
                }
                if(put || record.hasPublisher()) {
                    paramSJ.add("publisher = ?");
                }
                if(put || record.hasDate()) {
                    paramSJ.add("date = ?");
                }
                if(put || record.hasProducer()) {
                    paramSJ.add("producer = ?");
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    paramSJ.add("product_property_numeric1 = ?");
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    paramSJ.add("product_property_numeric2 = ?");
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    paramSJ.add("product_property_numeric3 = ?");
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    paramSJ.add("product_property_numeric4 = ?");
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    paramSJ.add("product_property_numeric5 = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE product_type8 SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasLabel()) {
                    ps.setObject(index, record.getLabel());
                    index++;
                }
                if(put || record.hasComment()) {
                    ps.setObject(index, record.getComment());
                    index++;
                }
                if(put || record.hasProductPropertyTextual1()) {
                    ps.setObject(index, record.getProductPropertyTextual1());
                    index++;
                }
                if(put || record.hasProductPropertyTextual2()) {
                    ps.setObject(index, record.getProductPropertyTextual2());
                    index++;
                }
                if(put || record.hasProductPropertyTextual3()) {
                    ps.setObject(index, record.getProductPropertyTextual3());
                    index++;
                }
                if(put || record.hasProductPropertyTextual4()) {
                    ps.setObject(index, record.getProductPropertyTextual4());
                    index++;
                }
                if(put || record.hasProductPropertyTextual5()) {
                    ps.setObject(index, record.getProductPropertyTextual5());
                    index++;
                }
                if(put || record.hasProductPropertyTextual6()) {
                    ps.setObject(index, record.getProductPropertyTextual6());
                    index++;
                }
                if(put || record.hasPublisher()) {
                    ps.setObject(index, record.getPublisher());
                    index++;
                }
                if(put || record.hasDate()) {
                    ps.setObject(index, record.getDate());
                    index++;
                }
                if(put || record.hasProducer()) {
                    ps.setObject(index, record.getProducer());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    ps.setObject(index, record.getProductPropertyNumeric1());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    ps.setObject(index, record.getProductPropertyNumeric2());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    ps.setObject(index, record.getProductPropertyNumeric3());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    ps.setObject(index, record.getProductPropertyNumeric4());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    ps.setObject(index, record.getProductPropertyNumeric5());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
                if(put || (record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_product_type8_product_feature_product_feature WHERE product_type8_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_product_type8_product_feature_product_feature VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getProductFeatureProductFeature()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
                if(put || (record.hasType() && !record.getType().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_resource_type_class WHERE resource_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasType() && !record.getType().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_resource_type_class VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getType()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
            });
        }

        return records;
    }

    public void deleteProductType8() {
        deleteProductType8(null);
    }    

    public void deleteProductType8(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM product_type8");
        String in = "";
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            in = "IN (" + sj.toString() + ")";
            querySB.append(" WHERE id " + in);
        }

        final String finalIn = in;
        final String query = querySB.toString();

        SqlUtils.run(connection, c -> {
            PreparedStatement ps = c.prepareStatement(query);
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_product_type8_product_feature_product_feature" + (finalIn.isEmpty() ? "" : " WHERE product_type8_id " + finalIn));
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_resource_type_class" + (finalIn.isEmpty() ? "" : " WHERE resource_id " + finalIn));
            ps.execute();
            ps.close();

        });
    }

    //==========================================================================
    // ProductType9

    private RSQLVisitor<Boolean, ProductType9> ProductType9Visitor = new RSQLVisitor<Boolean, ProductType9>() {

        @Override
        public Boolean visit(AndNode and, ProductType9 record) {
            for(Node n : and.getChildren()) {
                if(n instanceof AndNode) {
                    if(!visit((AndNode)n, record))
                        return false;
                } else if(n instanceof OrNode) {
                    if(!visit((OrNode)n, record))
                        return false;
                } else if(n instanceof ComparisonNode) {
                    if(!visit((ComparisonNode)n, record))
                        return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visit(OrNode or, ProductType9 record) {
            for(Node n : or.getChildren()) {
                if(n instanceof AndNode) {
                    if(visit((AndNode)n, record))
                        return true;
                } else if(n instanceof OrNode) {
                    if(visit((OrNode)n, record))
                        return true;
                } else if(n instanceof ComparisonNode) {
                    if(visit((ComparisonNode)n, record))
                        return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(ComparisonNode cn, ProductType9 record) {

            Set<String> actual = record.getStringSet(cn.getSelector());
            if(actual == null) {
                return false;
            }
            Set<String> expected = new HashSet<>(cn.getArguments());

            if(cn.getOperator().getSymbol().equals("=lt=") || 
               cn.getOperator().getSymbol().equals("=gt=") ||
               cn.getOperator().getSymbol().equals("=le=") ||
               cn.getOperator().getSymbol().equals("=ge=")) {
            
                double actualDouble = Double.parseDouble(actual.iterator().next());
                double expectedDouble = Double.parseDouble(expected.iterator().next());
                
                if(cn.getOperator().getSymbol().equals("=lt=")) {
                    return actualDouble < expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=gt=")) {
                    return actualDouble > expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=le=")) {
                    return actualDouble <= expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=ge=")) {
                    return actualDouble >= expectedDouble;
                }
            } else if(cn.getOperator().getSymbol().equals("==")) {
                return actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("!=")) {
                return !actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("=in=")) {
                return actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=out=")) {
                return !actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=regex=")) {
                String patternStr = expected.iterator().next();
                Pattern pattern = Pattern.compile(patternStr);
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        Matcher matcher = pattern.matcher(act);
                        if(matcher.find())
                            return true;
                    }
                }
                return false;
            } else if(cn.getOperator().getSymbol().equals("=lang=")) {
                String lang = expected.iterator().next();
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        if(act.endsWith("@" + lang))
                            return true;
                    }
                }
                return false;
            }

            return false;
        }
    };

    public List<ProductType9> selectProductType9() {
        return selectProductType9(null, null, null, null);
    }

    public List<ProductType9> selectProductType9(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM product_type9\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        if(limit != null) {
            querySB.append("\nLIMIT " + limit);
        }
        if(offset != null) {
            querySB.append("\nOFFSET " + offset);
        }

        String query = querySB.toString();

        List<ProductType9> result = SqlUtils.supply(connection, c -> {
            List<ProductType9> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                ProductType9 record = new ProductType9();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setLabel(SqlUtils.getString(rs, "label"));
                record.setComment(SqlUtils.getString(rs, "comment"));
                record.setProductPropertyTextual1(SqlUtils.getString(rs, "product_property_textual1"));
                record.setProductPropertyTextual2(SqlUtils.getString(rs, "product_property_textual2"));
                record.setProductPropertyTextual3(SqlUtils.getString(rs, "product_property_textual3"));
                record.setProductPropertyTextual4(SqlUtils.getString(rs, "product_property_textual4"));
                record.setProductPropertyTextual5(SqlUtils.getString(rs, "product_property_textual5"));
                record.setProductPropertyTextual6(SqlUtils.getString(rs, "product_property_textual6"));
                record.setPublisher(SqlUtils.getString(rs, "publisher"));
                record.setDate(SqlUtils.getLong(rs, "date"));
                record.setProducer(SqlUtils.getLong(rs, "producer"));
                record.setProductPropertyNumeric1(SqlUtils.getLong(rs, "product_property_numeric1"));
                record.setProductPropertyNumeric2(SqlUtils.getLong(rs, "product_property_numeric2"));
                record.setProductPropertyNumeric3(SqlUtils.getLong(rs, "product_property_numeric3"));
                record.setProductPropertyNumeric4(SqlUtils.getLong(rs, "product_property_numeric4"));
                record.setProductPropertyNumeric5(SqlUtils.getLong(rs, "product_property_numeric5"));
                record.setProductPropertyNumeric6(SqlUtils.getLong(rs, "product_property_numeric6"));

                record.setProductFeatureProductFeature(selectWithNM(record.getId(), "product_type9_id", "mn_product_type9_product_feature_product_feature", Long.class));
                record.setType(selectWithNM(record.getId(), "resource_id", "mn_resource_type_class", Long.class));
                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(ProductType9Visitor, record));
        }

        return result;
    }

    public List<ProductType9> insertProductType9(List<ProductType9> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO product_type9 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(ProductType9 record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getLabel());
                ps.setObject(3, record.getComment());
                ps.setObject(4, record.getProductPropertyTextual1());
                ps.setObject(5, record.getProductPropertyTextual2());
                ps.setObject(6, record.getProductPropertyTextual3());
                ps.setObject(7, record.getProductPropertyTextual4());
                ps.setObject(8, record.getProductPropertyTextual5());
                ps.setObject(9, record.getProductPropertyTextual6());
                ps.setObject(10, record.getPublisher());
                ps.setObject(11, record.getDate());
                ps.setObject(12, record.getProducer());
                ps.setObject(13, record.getProductPropertyNumeric1());
                ps.setObject(14, record.getProductPropertyNumeric2());
                ps.setObject(15, record.getProductPropertyNumeric3());
                ps.setObject(16, record.getProductPropertyNumeric4());
                ps.setObject(17, record.getProductPropertyNumeric5());
                ps.setObject(18, record.getProductPropertyNumeric6());
                ps.addBatch();
            }
            ps.executeBatch();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            for(int i = records.size()-1; i >= 0; i--) {
                //does only return the last generated id
                if(!generatedKeys.next())
                    break;

                long id = generatedKeys.getLong(1);
                records.get(i).setId(id);
            }
            c.commit();
            ps.close();

            boolean wasRecordsAdded = false;

            //n:m tables
            query = "INSERT INTO mn_product_type9_product_feature_product_feature VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType9 record : records) {
                if(!record.hasId() || !record.hasProductFeatureProductFeature() || record.getProductFeatureProductFeature().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getProductFeatureProductFeature()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;

            query = "INSERT INTO mn_resource_type_class VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType9 record : records) {
                if(!record.hasId() || !record.hasType() || record.getType().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getType()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;


            c.setAutoCommit(true);
        });

        return records;
    }

    public List<ProductType9> updateProductType9(List<ProductType9> records, boolean put) {    
        for(ProductType9 record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasLabel()) {
                    paramSJ.add("label = ?");
                }
                if(put || record.hasComment()) {
                    paramSJ.add("comment = ?");
                }
                if(put || record.hasProductPropertyTextual1()) {
                    paramSJ.add("product_property_textual1 = ?");
                }
                if(put || record.hasProductPropertyTextual2()) {
                    paramSJ.add("product_property_textual2 = ?");
                }
                if(put || record.hasProductPropertyTextual3()) {
                    paramSJ.add("product_property_textual3 = ?");
                }
                if(put || record.hasProductPropertyTextual4()) {
                    paramSJ.add("product_property_textual4 = ?");
                }
                if(put || record.hasProductPropertyTextual5()) {
                    paramSJ.add("product_property_textual5 = ?");
                }
                if(put || record.hasProductPropertyTextual6()) {
                    paramSJ.add("product_property_textual6 = ?");
                }
                if(put || record.hasPublisher()) {
                    paramSJ.add("publisher = ?");
                }
                if(put || record.hasDate()) {
                    paramSJ.add("date = ?");
                }
                if(put || record.hasProducer()) {
                    paramSJ.add("producer = ?");
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    paramSJ.add("product_property_numeric1 = ?");
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    paramSJ.add("product_property_numeric2 = ?");
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    paramSJ.add("product_property_numeric3 = ?");
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    paramSJ.add("product_property_numeric4 = ?");
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    paramSJ.add("product_property_numeric5 = ?");
                }
                if(put || record.hasProductPropertyNumeric6()) {
                    paramSJ.add("product_property_numeric6 = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE product_type9 SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasLabel()) {
                    ps.setObject(index, record.getLabel());
                    index++;
                }
                if(put || record.hasComment()) {
                    ps.setObject(index, record.getComment());
                    index++;
                }
                if(put || record.hasProductPropertyTextual1()) {
                    ps.setObject(index, record.getProductPropertyTextual1());
                    index++;
                }
                if(put || record.hasProductPropertyTextual2()) {
                    ps.setObject(index, record.getProductPropertyTextual2());
                    index++;
                }
                if(put || record.hasProductPropertyTextual3()) {
                    ps.setObject(index, record.getProductPropertyTextual3());
                    index++;
                }
                if(put || record.hasProductPropertyTextual4()) {
                    ps.setObject(index, record.getProductPropertyTextual4());
                    index++;
                }
                if(put || record.hasProductPropertyTextual5()) {
                    ps.setObject(index, record.getProductPropertyTextual5());
                    index++;
                }
                if(put || record.hasProductPropertyTextual6()) {
                    ps.setObject(index, record.getProductPropertyTextual6());
                    index++;
                }
                if(put || record.hasPublisher()) {
                    ps.setObject(index, record.getPublisher());
                    index++;
                }
                if(put || record.hasDate()) {
                    ps.setObject(index, record.getDate());
                    index++;
                }
                if(put || record.hasProducer()) {
                    ps.setObject(index, record.getProducer());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    ps.setObject(index, record.getProductPropertyNumeric1());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    ps.setObject(index, record.getProductPropertyNumeric2());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    ps.setObject(index, record.getProductPropertyNumeric3());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    ps.setObject(index, record.getProductPropertyNumeric4());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    ps.setObject(index, record.getProductPropertyNumeric5());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric6()) {
                    ps.setObject(index, record.getProductPropertyNumeric6());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
                if(put || (record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_product_type9_product_feature_product_feature WHERE product_type9_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_product_type9_product_feature_product_feature VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getProductFeatureProductFeature()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
                if(put || (record.hasType() && !record.getType().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_resource_type_class WHERE resource_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasType() && !record.getType().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_resource_type_class VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getType()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
            });
        }

        return records;
    }

    public void deleteProductType9() {
        deleteProductType9(null);
    }    

    public void deleteProductType9(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM product_type9");
        String in = "";
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            in = "IN (" + sj.toString() + ")";
            querySB.append(" WHERE id " + in);
        }

        final String finalIn = in;
        final String query = querySB.toString();

        SqlUtils.run(connection, c -> {
            PreparedStatement ps = c.prepareStatement(query);
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_product_type9_product_feature_product_feature" + (finalIn.isEmpty() ? "" : " WHERE product_type9_id " + finalIn));
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_resource_type_class" + (finalIn.isEmpty() ? "" : " WHERE resource_id " + finalIn));
            ps.execute();
            ps.close();

        });
    }

    //==========================================================================
    // ProductType

    private RSQLVisitor<Boolean, ProductType> ProductTypeVisitor = new RSQLVisitor<Boolean, ProductType>() {

        @Override
        public Boolean visit(AndNode and, ProductType record) {
            for(Node n : and.getChildren()) {
                if(n instanceof AndNode) {
                    if(!visit((AndNode)n, record))
                        return false;
                } else if(n instanceof OrNode) {
                    if(!visit((OrNode)n, record))
                        return false;
                } else if(n instanceof ComparisonNode) {
                    if(!visit((ComparisonNode)n, record))
                        return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visit(OrNode or, ProductType record) {
            for(Node n : or.getChildren()) {
                if(n instanceof AndNode) {
                    if(visit((AndNode)n, record))
                        return true;
                } else if(n instanceof OrNode) {
                    if(visit((OrNode)n, record))
                        return true;
                } else if(n instanceof ComparisonNode) {
                    if(visit((ComparisonNode)n, record))
                        return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(ComparisonNode cn, ProductType record) {

            Set<String> actual = record.getStringSet(cn.getSelector());
            if(actual == null) {
                return false;
            }
            Set<String> expected = new HashSet<>(cn.getArguments());

            if(cn.getOperator().getSymbol().equals("=lt=") || 
               cn.getOperator().getSymbol().equals("=gt=") ||
               cn.getOperator().getSymbol().equals("=le=") ||
               cn.getOperator().getSymbol().equals("=ge=")) {
            
                double actualDouble = Double.parseDouble(actual.iterator().next());
                double expectedDouble = Double.parseDouble(expected.iterator().next());
                
                if(cn.getOperator().getSymbol().equals("=lt=")) {
                    return actualDouble < expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=gt=")) {
                    return actualDouble > expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=le=")) {
                    return actualDouble <= expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=ge=")) {
                    return actualDouble >= expectedDouble;
                }
            } else if(cn.getOperator().getSymbol().equals("==")) {
                return actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("!=")) {
                return !actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("=in=")) {
                return actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=out=")) {
                return !actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=regex=")) {
                String patternStr = expected.iterator().next();
                Pattern pattern = Pattern.compile(patternStr);
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        Matcher matcher = pattern.matcher(act);
                        if(matcher.find())
                            return true;
                    }
                }
                return false;
            } else if(cn.getOperator().getSymbol().equals("=lang=")) {
                String lang = expected.iterator().next();
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        if(act.endsWith("@" + lang))
                            return true;
                    }
                }
                return false;
            }

            return false;
        }
    };

    public List<ProductType> selectProductType() {
        return selectProductType(null, null, null, null);
    }

    public List<ProductType> selectProductType(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM product_type\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        if(limit != null) {
            querySB.append("\nLIMIT " + limit);
        }
        if(offset != null) {
            querySB.append("\nOFFSET " + offset);
        }

        String query = querySB.toString();

        List<ProductType> result = SqlUtils.supply(connection, c -> {
            List<ProductType> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                ProductType record = new ProductType();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setLabel(SqlUtils.getString(rs, "label"));
                record.setComment(SqlUtils.getString(rs, "comment"));
                record.setPublisher(SqlUtils.getString(rs, "publisher"));
                record.setDate(SqlUtils.getLong(rs, "date"));
                record.setSubClassOf(SqlUtils.getLong(rs, "sub_class_of"));

                record.setType(selectWithNM(record.getId(), "resource_id", "mn_resource_type_class", Long.class));
                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(ProductTypeVisitor, record));
        }

        return result;
    }

    public List<ProductType> insertProductType(List<ProductType> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO product_type VALUES (?,?,?,?,?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(ProductType record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getLabel());
                ps.setObject(3, record.getComment());
                ps.setObject(4, record.getPublisher());
                ps.setObject(5, record.getDate());
                ps.setObject(6, record.getSubClassOf());
                ps.addBatch();
            }
            ps.executeBatch();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            for(int i = records.size()-1; i >= 0; i--) {
                //does only return the last generated id
                if(!generatedKeys.next())
                    break;

                long id = generatedKeys.getLong(1);
                records.get(i).setId(id);
            }
            c.commit();
            ps.close();

            boolean wasRecordsAdded = false;

            //n:m tables
            query = "INSERT INTO mn_resource_type_class VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType record : records) {
                if(!record.hasId() || !record.hasType() || record.getType().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getType()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;


            c.setAutoCommit(true);
        });

        return records;
    }

    public List<ProductType> updateProductType(List<ProductType> records, boolean put) {    
        for(ProductType record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasLabel()) {
                    paramSJ.add("label = ?");
                }
                if(put || record.hasComment()) {
                    paramSJ.add("comment = ?");
                }
                if(put || record.hasPublisher()) {
                    paramSJ.add("publisher = ?");
                }
                if(put || record.hasDate()) {
                    paramSJ.add("date = ?");
                }
                if(put || record.hasSubClassOf()) {
                    paramSJ.add("sub_class_of = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE product_type SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasLabel()) {
                    ps.setObject(index, record.getLabel());
                    index++;
                }
                if(put || record.hasComment()) {
                    ps.setObject(index, record.getComment());
                    index++;
                }
                if(put || record.hasPublisher()) {
                    ps.setObject(index, record.getPublisher());
                    index++;
                }
                if(put || record.hasDate()) {
                    ps.setObject(index, record.getDate());
                    index++;
                }
                if(put || record.hasSubClassOf()) {
                    ps.setObject(index, record.getSubClassOf());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
                if(put || (record.hasType() && !record.getType().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_resource_type_class WHERE resource_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasType() && !record.getType().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_resource_type_class VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getType()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
            });
        }

        return records;
    }

    public void deleteProductType() {
        deleteProductType(null);
    }    

    public void deleteProductType(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM product_type");
        String in = "";
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            in = "IN (" + sj.toString() + ")";
            querySB.append(" WHERE id " + in);
        }

        final String finalIn = in;
        final String query = querySB.toString();

        SqlUtils.run(connection, c -> {
            PreparedStatement ps = c.prepareStatement(query);
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_resource_type_class" + (finalIn.isEmpty() ? "" : " WHERE resource_id " + finalIn));
            ps.execute();
            ps.close();

        });
    }

    //==========================================================================
    // ProductFeature

    private RSQLVisitor<Boolean, ProductFeature> ProductFeatureVisitor = new RSQLVisitor<Boolean, ProductFeature>() {

        @Override
        public Boolean visit(AndNode and, ProductFeature record) {
            for(Node n : and.getChildren()) {
                if(n instanceof AndNode) {
                    if(!visit((AndNode)n, record))
                        return false;
                } else if(n instanceof OrNode) {
                    if(!visit((OrNode)n, record))
                        return false;
                } else if(n instanceof ComparisonNode) {
                    if(!visit((ComparisonNode)n, record))
                        return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visit(OrNode or, ProductFeature record) {
            for(Node n : or.getChildren()) {
                if(n instanceof AndNode) {
                    if(visit((AndNode)n, record))
                        return true;
                } else if(n instanceof OrNode) {
                    if(visit((OrNode)n, record))
                        return true;
                } else if(n instanceof ComparisonNode) {
                    if(visit((ComparisonNode)n, record))
                        return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(ComparisonNode cn, ProductFeature record) {

            Set<String> actual = record.getStringSet(cn.getSelector());
            if(actual == null) {
                return false;
            }
            Set<String> expected = new HashSet<>(cn.getArguments());

            if(cn.getOperator().getSymbol().equals("=lt=") || 
               cn.getOperator().getSymbol().equals("=gt=") ||
               cn.getOperator().getSymbol().equals("=le=") ||
               cn.getOperator().getSymbol().equals("=ge=")) {
            
                double actualDouble = Double.parseDouble(actual.iterator().next());
                double expectedDouble = Double.parseDouble(expected.iterator().next());
                
                if(cn.getOperator().getSymbol().equals("=lt=")) {
                    return actualDouble < expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=gt=")) {
                    return actualDouble > expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=le=")) {
                    return actualDouble <= expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=ge=")) {
                    return actualDouble >= expectedDouble;
                }
            } else if(cn.getOperator().getSymbol().equals("==")) {
                return actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("!=")) {
                return !actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("=in=")) {
                return actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=out=")) {
                return !actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=regex=")) {
                String patternStr = expected.iterator().next();
                Pattern pattern = Pattern.compile(patternStr);
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        Matcher matcher = pattern.matcher(act);
                        if(matcher.find())
                            return true;
                    }
                }
                return false;
            } else if(cn.getOperator().getSymbol().equals("=lang=")) {
                String lang = expected.iterator().next();
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        if(act.endsWith("@" + lang))
                            return true;
                    }
                }
                return false;
            }

            return false;
        }
    };

    public List<ProductFeature> selectProductFeature() {
        return selectProductFeature(null, null, null, null);
    }

    public List<ProductFeature> selectProductFeature(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM product_feature\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        if(limit != null) {
            querySB.append("\nLIMIT " + limit);
        }
        if(offset != null) {
            querySB.append("\nOFFSET " + offset);
        }

        String query = querySB.toString();

        List<ProductFeature> result = SqlUtils.supply(connection, c -> {
            List<ProductFeature> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                ProductFeature record = new ProductFeature();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setLabel(SqlUtils.getString(rs, "label"));
                record.setComment(SqlUtils.getString(rs, "comment"));
                record.setPublisher(SqlUtils.getString(rs, "publisher"));
                record.setDate(SqlUtils.getLong(rs, "date"));

                record.setType(selectWithNM(record.getId(), "resource_id", "mn_resource_type_class", Long.class));
                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(ProductFeatureVisitor, record));
        }

        return result;
    }

    public List<ProductFeature> insertProductFeature(List<ProductFeature> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO product_feature VALUES (?,?,?,?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(ProductFeature record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getLabel());
                ps.setObject(3, record.getComment());
                ps.setObject(4, record.getPublisher());
                ps.setObject(5, record.getDate());
                ps.addBatch();
            }
            ps.executeBatch();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            for(int i = records.size()-1; i >= 0; i--) {
                //does only return the last generated id
                if(!generatedKeys.next())
                    break;

                long id = generatedKeys.getLong(1);
                records.get(i).setId(id);
            }
            c.commit();
            ps.close();

            boolean wasRecordsAdded = false;

            //n:m tables
            query = "INSERT INTO mn_resource_type_class VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductFeature record : records) {
                if(!record.hasId() || !record.hasType() || record.getType().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getType()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;


            c.setAutoCommit(true);
        });

        return records;
    }

    public List<ProductFeature> updateProductFeature(List<ProductFeature> records, boolean put) {    
        for(ProductFeature record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasLabel()) {
                    paramSJ.add("label = ?");
                }
                if(put || record.hasComment()) {
                    paramSJ.add("comment = ?");
                }
                if(put || record.hasPublisher()) {
                    paramSJ.add("publisher = ?");
                }
                if(put || record.hasDate()) {
                    paramSJ.add("date = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE product_feature SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasLabel()) {
                    ps.setObject(index, record.getLabel());
                    index++;
                }
                if(put || record.hasComment()) {
                    ps.setObject(index, record.getComment());
                    index++;
                }
                if(put || record.hasPublisher()) {
                    ps.setObject(index, record.getPublisher());
                    index++;
                }
                if(put || record.hasDate()) {
                    ps.setObject(index, record.getDate());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
                if(put || (record.hasType() && !record.getType().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_resource_type_class WHERE resource_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasType() && !record.getType().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_resource_type_class VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getType()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
            });
        }

        return records;
    }

    public void deleteProductFeature() {
        deleteProductFeature(null);
    }    

    public void deleteProductFeature(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM product_feature");
        String in = "";
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            in = "IN (" + sj.toString() + ")";
            querySB.append(" WHERE id " + in);
        }

        final String finalIn = in;
        final String query = querySB.toString();

        SqlUtils.run(connection, c -> {
            PreparedStatement ps = c.prepareStatement(query);
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_resource_type_class" + (finalIn.isEmpty() ? "" : " WHERE resource_id " + finalIn));
            ps.execute();
            ps.close();

        });
    }

    //==========================================================================
    // Product

    private RSQLVisitor<Boolean, Product> ProductVisitor = new RSQLVisitor<Boolean, Product>() {

        @Override
        public Boolean visit(AndNode and, Product record) {
            for(Node n : and.getChildren()) {
                if(n instanceof AndNode) {
                    if(!visit((AndNode)n, record))
                        return false;
                } else if(n instanceof OrNode) {
                    if(!visit((OrNode)n, record))
                        return false;
                } else if(n instanceof ComparisonNode) {
                    if(!visit((ComparisonNode)n, record))
                        return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visit(OrNode or, Product record) {
            for(Node n : or.getChildren()) {
                if(n instanceof AndNode) {
                    if(visit((AndNode)n, record))
                        return true;
                } else if(n instanceof OrNode) {
                    if(visit((OrNode)n, record))
                        return true;
                } else if(n instanceof ComparisonNode) {
                    if(visit((ComparisonNode)n, record))
                        return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(ComparisonNode cn, Product record) {

            Set<String> actual = record.getStringSet(cn.getSelector());
            if(actual == null) {
                return false;
            }
            Set<String> expected = new HashSet<>(cn.getArguments());

            if(cn.getOperator().getSymbol().equals("=lt=") || 
               cn.getOperator().getSymbol().equals("=gt=") ||
               cn.getOperator().getSymbol().equals("=le=") ||
               cn.getOperator().getSymbol().equals("=ge=")) {
            
                double actualDouble = Double.parseDouble(actual.iterator().next());
                double expectedDouble = Double.parseDouble(expected.iterator().next());
                
                if(cn.getOperator().getSymbol().equals("=lt=")) {
                    return actualDouble < expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=gt=")) {
                    return actualDouble > expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=le=")) {
                    return actualDouble <= expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=ge=")) {
                    return actualDouble >= expectedDouble;
                }
            } else if(cn.getOperator().getSymbol().equals("==")) {
                return actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("!=")) {
                return !actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("=in=")) {
                return actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=out=")) {
                return !actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=regex=")) {
                String patternStr = expected.iterator().next();
                Pattern pattern = Pattern.compile(patternStr);
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        Matcher matcher = pattern.matcher(act);
                        if(matcher.find())
                            return true;
                    }
                }
                return false;
            } else if(cn.getOperator().getSymbol().equals("=lang=")) {
                String lang = expected.iterator().next();
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        if(act.endsWith("@" + lang))
                            return true;
                    }
                }
                return false;
            }

            return false;
        }
    };

    public List<Product> selectProduct() {
        return selectProduct(null, null, null, null);
    }

    public List<Product> selectProduct(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM product\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        if(limit != null) {
            querySB.append("\nLIMIT " + limit);
        }
        if(offset != null) {
            querySB.append("\nOFFSET " + offset);
        }

        String query = querySB.toString();

        List<Product> result = SqlUtils.supply(connection, c -> {
            List<Product> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                Product record = new Product();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setLabel(SqlUtils.getString(rs, "label"));
                record.setComment(SqlUtils.getString(rs, "comment"));
                record.setProductPropertyTextual1(SqlUtils.getString(rs, "product_property_textual1"));
                record.setProductPropertyTextual2(SqlUtils.getString(rs, "product_property_textual2"));
                record.setProductPropertyTextual3(SqlUtils.getString(rs, "product_property_textual3"));
                record.setProductPropertyTextual4(SqlUtils.getString(rs, "product_property_textual4"));
                record.setProductPropertyTextual5(SqlUtils.getString(rs, "product_property_textual5"));
                record.setProductPropertyTextual6(SqlUtils.getString(rs, "product_property_textual6"));
                record.setPublisher(SqlUtils.getString(rs, "publisher"));
                record.setDate(SqlUtils.getLong(rs, "date"));
                record.setProducer(SqlUtils.getLong(rs, "producer"));
                record.setProductPropertyNumeric1(SqlUtils.getLong(rs, "product_property_numeric1"));
                record.setProductPropertyNumeric2(SqlUtils.getLong(rs, "product_property_numeric2"));
                record.setProductPropertyNumeric3(SqlUtils.getLong(rs, "product_property_numeric3"));
                record.setProductPropertyNumeric4(SqlUtils.getLong(rs, "product_property_numeric4"));
                record.setProductPropertyNumeric5(SqlUtils.getLong(rs, "product_property_numeric5"));
                record.setProductPropertyNumeric6(SqlUtils.getLong(rs, "product_property_numeric6"));

                record.setProductFeatureProductFeature(selectWithNM(record.getId(), "product_id", "mn_product_product_feature_product_feature", Long.class));
                record.setType(selectWithNM(record.getId(), "resource_id", "mn_resource_type_class", Long.class));
                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(ProductVisitor, record));
        }

        return result;
    }

    public List<Product> insertProduct(List<Product> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO product VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(Product record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getLabel());
                ps.setObject(3, record.getComment());
                ps.setObject(4, record.getProductPropertyTextual1());
                ps.setObject(5, record.getProductPropertyTextual2());
                ps.setObject(6, record.getProductPropertyTextual3());
                ps.setObject(7, record.getProductPropertyTextual4());
                ps.setObject(8, record.getProductPropertyTextual5());
                ps.setObject(9, record.getProductPropertyTextual6());
                ps.setObject(10, record.getPublisher());
                ps.setObject(11, record.getDate());
                ps.setObject(12, record.getProducer());
                ps.setObject(13, record.getProductPropertyNumeric1());
                ps.setObject(14, record.getProductPropertyNumeric2());
                ps.setObject(15, record.getProductPropertyNumeric3());
                ps.setObject(16, record.getProductPropertyNumeric4());
                ps.setObject(17, record.getProductPropertyNumeric5());
                ps.setObject(18, record.getProductPropertyNumeric6());
                ps.addBatch();
            }
            ps.executeBatch();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            for(int i = records.size()-1; i >= 0; i--) {
                //does only return the last generated id
                if(!generatedKeys.next())
                    break;

                long id = generatedKeys.getLong(1);
                records.get(i).setId(id);
            }
            c.commit();
            ps.close();

            boolean wasRecordsAdded = false;

            //n:m tables
            query = "INSERT INTO mn_product_product_feature_product_feature VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(Product record : records) {
                if(!record.hasId() || !record.hasProductFeatureProductFeature() || record.getProductFeatureProductFeature().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getProductFeatureProductFeature()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;

            query = "INSERT INTO mn_resource_type_class VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(Product record : records) {
                if(!record.hasId() || !record.hasType() || record.getType().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getType()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;


            c.setAutoCommit(true);
        });

        return records;
    }

    public List<Product> updateProduct(List<Product> records, boolean put) {    
        for(Product record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasLabel()) {
                    paramSJ.add("label = ?");
                }
                if(put || record.hasComment()) {
                    paramSJ.add("comment = ?");
                }
                if(put || record.hasProductPropertyTextual1()) {
                    paramSJ.add("product_property_textual1 = ?");
                }
                if(put || record.hasProductPropertyTextual2()) {
                    paramSJ.add("product_property_textual2 = ?");
                }
                if(put || record.hasProductPropertyTextual3()) {
                    paramSJ.add("product_property_textual3 = ?");
                }
                if(put || record.hasProductPropertyTextual4()) {
                    paramSJ.add("product_property_textual4 = ?");
                }
                if(put || record.hasProductPropertyTextual5()) {
                    paramSJ.add("product_property_textual5 = ?");
                }
                if(put || record.hasProductPropertyTextual6()) {
                    paramSJ.add("product_property_textual6 = ?");
                }
                if(put || record.hasPublisher()) {
                    paramSJ.add("publisher = ?");
                }
                if(put || record.hasDate()) {
                    paramSJ.add("date = ?");
                }
                if(put || record.hasProducer()) {
                    paramSJ.add("producer = ?");
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    paramSJ.add("product_property_numeric1 = ?");
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    paramSJ.add("product_property_numeric2 = ?");
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    paramSJ.add("product_property_numeric3 = ?");
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    paramSJ.add("product_property_numeric4 = ?");
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    paramSJ.add("product_property_numeric5 = ?");
                }
                if(put || record.hasProductPropertyNumeric6()) {
                    paramSJ.add("product_property_numeric6 = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE product SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasLabel()) {
                    ps.setObject(index, record.getLabel());
                    index++;
                }
                if(put || record.hasComment()) {
                    ps.setObject(index, record.getComment());
                    index++;
                }
                if(put || record.hasProductPropertyTextual1()) {
                    ps.setObject(index, record.getProductPropertyTextual1());
                    index++;
                }
                if(put || record.hasProductPropertyTextual2()) {
                    ps.setObject(index, record.getProductPropertyTextual2());
                    index++;
                }
                if(put || record.hasProductPropertyTextual3()) {
                    ps.setObject(index, record.getProductPropertyTextual3());
                    index++;
                }
                if(put || record.hasProductPropertyTextual4()) {
                    ps.setObject(index, record.getProductPropertyTextual4());
                    index++;
                }
                if(put || record.hasProductPropertyTextual5()) {
                    ps.setObject(index, record.getProductPropertyTextual5());
                    index++;
                }
                if(put || record.hasProductPropertyTextual6()) {
                    ps.setObject(index, record.getProductPropertyTextual6());
                    index++;
                }
                if(put || record.hasPublisher()) {
                    ps.setObject(index, record.getPublisher());
                    index++;
                }
                if(put || record.hasDate()) {
                    ps.setObject(index, record.getDate());
                    index++;
                }
                if(put || record.hasProducer()) {
                    ps.setObject(index, record.getProducer());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    ps.setObject(index, record.getProductPropertyNumeric1());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    ps.setObject(index, record.getProductPropertyNumeric2());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    ps.setObject(index, record.getProductPropertyNumeric3());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    ps.setObject(index, record.getProductPropertyNumeric4());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    ps.setObject(index, record.getProductPropertyNumeric5());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric6()) {
                    ps.setObject(index, record.getProductPropertyNumeric6());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
                if(put || (record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_product_product_feature_product_feature WHERE product_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_product_product_feature_product_feature VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getProductFeatureProductFeature()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
                if(put || (record.hasType() && !record.getType().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_resource_type_class WHERE resource_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasType() && !record.getType().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_resource_type_class VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getType()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
            });
        }

        return records;
    }

    public void deleteProduct() {
        deleteProduct(null);
    }    

    public void deleteProduct(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM product");
        String in = "";
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            in = "IN (" + sj.toString() + ")";
            querySB.append(" WHERE id " + in);
        }

        final String finalIn = in;
        final String query = querySB.toString();

        SqlUtils.run(connection, c -> {
            PreparedStatement ps = c.prepareStatement(query);
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_product_product_feature_product_feature" + (finalIn.isEmpty() ? "" : " WHERE product_id " + finalIn));
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_resource_type_class" + (finalIn.isEmpty() ? "" : " WHERE resource_id " + finalIn));
            ps.execute();
            ps.close();

        });
    }

    //==========================================================================
    // ProductType17

    private RSQLVisitor<Boolean, ProductType17> ProductType17Visitor = new RSQLVisitor<Boolean, ProductType17>() {

        @Override
        public Boolean visit(AndNode and, ProductType17 record) {
            for(Node n : and.getChildren()) {
                if(n instanceof AndNode) {
                    if(!visit((AndNode)n, record))
                        return false;
                } else if(n instanceof OrNode) {
                    if(!visit((OrNode)n, record))
                        return false;
                } else if(n instanceof ComparisonNode) {
                    if(!visit((ComparisonNode)n, record))
                        return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visit(OrNode or, ProductType17 record) {
            for(Node n : or.getChildren()) {
                if(n instanceof AndNode) {
                    if(visit((AndNode)n, record))
                        return true;
                } else if(n instanceof OrNode) {
                    if(visit((OrNode)n, record))
                        return true;
                } else if(n instanceof ComparisonNode) {
                    if(visit((ComparisonNode)n, record))
                        return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(ComparisonNode cn, ProductType17 record) {

            Set<String> actual = record.getStringSet(cn.getSelector());
            if(actual == null) {
                return false;
            }
            Set<String> expected = new HashSet<>(cn.getArguments());

            if(cn.getOperator().getSymbol().equals("=lt=") || 
               cn.getOperator().getSymbol().equals("=gt=") ||
               cn.getOperator().getSymbol().equals("=le=") ||
               cn.getOperator().getSymbol().equals("=ge=")) {
            
                double actualDouble = Double.parseDouble(actual.iterator().next());
                double expectedDouble = Double.parseDouble(expected.iterator().next());
                
                if(cn.getOperator().getSymbol().equals("=lt=")) {
                    return actualDouble < expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=gt=")) {
                    return actualDouble > expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=le=")) {
                    return actualDouble <= expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=ge=")) {
                    return actualDouble >= expectedDouble;
                }
            } else if(cn.getOperator().getSymbol().equals("==")) {
                return actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("!=")) {
                return !actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("=in=")) {
                return actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=out=")) {
                return !actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=regex=")) {
                String patternStr = expected.iterator().next();
                Pattern pattern = Pattern.compile(patternStr);
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        Matcher matcher = pattern.matcher(act);
                        if(matcher.find())
                            return true;
                    }
                }
                return false;
            } else if(cn.getOperator().getSymbol().equals("=lang=")) {
                String lang = expected.iterator().next();
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        if(act.endsWith("@" + lang))
                            return true;
                    }
                }
                return false;
            }

            return false;
        }
    };

    public List<ProductType17> selectProductType17() {
        return selectProductType17(null, null, null, null);
    }

    public List<ProductType17> selectProductType17(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM product_type17\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        if(limit != null) {
            querySB.append("\nLIMIT " + limit);
        }
        if(offset != null) {
            querySB.append("\nOFFSET " + offset);
        }

        String query = querySB.toString();

        List<ProductType17> result = SqlUtils.supply(connection, c -> {
            List<ProductType17> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                ProductType17 record = new ProductType17();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setLabel(SqlUtils.getString(rs, "label"));
                record.setComment(SqlUtils.getString(rs, "comment"));
                record.setProductPropertyTextual1(SqlUtils.getString(rs, "product_property_textual1"));
                record.setProductPropertyTextual2(SqlUtils.getString(rs, "product_property_textual2"));
                record.setProductPropertyTextual3(SqlUtils.getString(rs, "product_property_textual3"));
                record.setProductPropertyTextual4(SqlUtils.getString(rs, "product_property_textual4"));
                record.setProductPropertyTextual5(SqlUtils.getString(rs, "product_property_textual5"));
                record.setProductPropertyTextual6(SqlUtils.getString(rs, "product_property_textual6"));
                record.setPublisher(SqlUtils.getString(rs, "publisher"));
                record.setDate(SqlUtils.getLong(rs, "date"));
                record.setProducer(SqlUtils.getLong(rs, "producer"));
                record.setProductPropertyNumeric1(SqlUtils.getLong(rs, "product_property_numeric1"));
                record.setProductPropertyNumeric2(SqlUtils.getLong(rs, "product_property_numeric2"));
                record.setProductPropertyNumeric3(SqlUtils.getLong(rs, "product_property_numeric3"));
                record.setProductPropertyNumeric4(SqlUtils.getLong(rs, "product_property_numeric4"));
                record.setProductPropertyNumeric5(SqlUtils.getLong(rs, "product_property_numeric5"));

                record.setProductFeatureProductFeature(selectWithNM(record.getId(), "product_type17_id", "mn_product_type17_product_feature_product_feature", Long.class));
                record.setType(selectWithNM(record.getId(), "resource_id", "mn_resource_type_class", Long.class));
                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(ProductType17Visitor, record));
        }

        return result;
    }

    public List<ProductType17> insertProductType17(List<ProductType17> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO product_type17 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(ProductType17 record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getLabel());
                ps.setObject(3, record.getComment());
                ps.setObject(4, record.getProductPropertyTextual1());
                ps.setObject(5, record.getProductPropertyTextual2());
                ps.setObject(6, record.getProductPropertyTextual3());
                ps.setObject(7, record.getProductPropertyTextual4());
                ps.setObject(8, record.getProductPropertyTextual5());
                ps.setObject(9, record.getProductPropertyTextual6());
                ps.setObject(10, record.getPublisher());
                ps.setObject(11, record.getDate());
                ps.setObject(12, record.getProducer());
                ps.setObject(13, record.getProductPropertyNumeric1());
                ps.setObject(14, record.getProductPropertyNumeric2());
                ps.setObject(15, record.getProductPropertyNumeric3());
                ps.setObject(16, record.getProductPropertyNumeric4());
                ps.setObject(17, record.getProductPropertyNumeric5());
                ps.addBatch();
            }
            ps.executeBatch();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            for(int i = records.size()-1; i >= 0; i--) {
                //does only return the last generated id
                if(!generatedKeys.next())
                    break;

                long id = generatedKeys.getLong(1);
                records.get(i).setId(id);
            }
            c.commit();
            ps.close();

            boolean wasRecordsAdded = false;

            //n:m tables
            query = "INSERT INTO mn_product_type17_product_feature_product_feature VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType17 record : records) {
                if(!record.hasId() || !record.hasProductFeatureProductFeature() || record.getProductFeatureProductFeature().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getProductFeatureProductFeature()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;

            query = "INSERT INTO mn_resource_type_class VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType17 record : records) {
                if(!record.hasId() || !record.hasType() || record.getType().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getType()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;


            c.setAutoCommit(true);
        });

        return records;
    }

    public List<ProductType17> updateProductType17(List<ProductType17> records, boolean put) {    
        for(ProductType17 record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasLabel()) {
                    paramSJ.add("label = ?");
                }
                if(put || record.hasComment()) {
                    paramSJ.add("comment = ?");
                }
                if(put || record.hasProductPropertyTextual1()) {
                    paramSJ.add("product_property_textual1 = ?");
                }
                if(put || record.hasProductPropertyTextual2()) {
                    paramSJ.add("product_property_textual2 = ?");
                }
                if(put || record.hasProductPropertyTextual3()) {
                    paramSJ.add("product_property_textual3 = ?");
                }
                if(put || record.hasProductPropertyTextual4()) {
                    paramSJ.add("product_property_textual4 = ?");
                }
                if(put || record.hasProductPropertyTextual5()) {
                    paramSJ.add("product_property_textual5 = ?");
                }
                if(put || record.hasProductPropertyTextual6()) {
                    paramSJ.add("product_property_textual6 = ?");
                }
                if(put || record.hasPublisher()) {
                    paramSJ.add("publisher = ?");
                }
                if(put || record.hasDate()) {
                    paramSJ.add("date = ?");
                }
                if(put || record.hasProducer()) {
                    paramSJ.add("producer = ?");
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    paramSJ.add("product_property_numeric1 = ?");
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    paramSJ.add("product_property_numeric2 = ?");
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    paramSJ.add("product_property_numeric3 = ?");
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    paramSJ.add("product_property_numeric4 = ?");
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    paramSJ.add("product_property_numeric5 = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE product_type17 SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasLabel()) {
                    ps.setObject(index, record.getLabel());
                    index++;
                }
                if(put || record.hasComment()) {
                    ps.setObject(index, record.getComment());
                    index++;
                }
                if(put || record.hasProductPropertyTextual1()) {
                    ps.setObject(index, record.getProductPropertyTextual1());
                    index++;
                }
                if(put || record.hasProductPropertyTextual2()) {
                    ps.setObject(index, record.getProductPropertyTextual2());
                    index++;
                }
                if(put || record.hasProductPropertyTextual3()) {
                    ps.setObject(index, record.getProductPropertyTextual3());
                    index++;
                }
                if(put || record.hasProductPropertyTextual4()) {
                    ps.setObject(index, record.getProductPropertyTextual4());
                    index++;
                }
                if(put || record.hasProductPropertyTextual5()) {
                    ps.setObject(index, record.getProductPropertyTextual5());
                    index++;
                }
                if(put || record.hasProductPropertyTextual6()) {
                    ps.setObject(index, record.getProductPropertyTextual6());
                    index++;
                }
                if(put || record.hasPublisher()) {
                    ps.setObject(index, record.getPublisher());
                    index++;
                }
                if(put || record.hasDate()) {
                    ps.setObject(index, record.getDate());
                    index++;
                }
                if(put || record.hasProducer()) {
                    ps.setObject(index, record.getProducer());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    ps.setObject(index, record.getProductPropertyNumeric1());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    ps.setObject(index, record.getProductPropertyNumeric2());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    ps.setObject(index, record.getProductPropertyNumeric3());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    ps.setObject(index, record.getProductPropertyNumeric4());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    ps.setObject(index, record.getProductPropertyNumeric5());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
                if(put || (record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_product_type17_product_feature_product_feature WHERE product_type17_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_product_type17_product_feature_product_feature VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getProductFeatureProductFeature()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
                if(put || (record.hasType() && !record.getType().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_resource_type_class WHERE resource_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasType() && !record.getType().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_resource_type_class VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getType()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
            });
        }

        return records;
    }

    public void deleteProductType17() {
        deleteProductType17(null);
    }    

    public void deleteProductType17(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM product_type17");
        String in = "";
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            in = "IN (" + sj.toString() + ")";
            querySB.append(" WHERE id " + in);
        }

        final String finalIn = in;
        final String query = querySB.toString();

        SqlUtils.run(connection, c -> {
            PreparedStatement ps = c.prepareStatement(query);
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_product_type17_product_feature_product_feature" + (finalIn.isEmpty() ? "" : " WHERE product_type17_id " + finalIn));
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_resource_type_class" + (finalIn.isEmpty() ? "" : " WHERE resource_id " + finalIn));
            ps.execute();
            ps.close();

        });
    }

    //==========================================================================
    // ProductType18

    private RSQLVisitor<Boolean, ProductType18> ProductType18Visitor = new RSQLVisitor<Boolean, ProductType18>() {

        @Override
        public Boolean visit(AndNode and, ProductType18 record) {
            for(Node n : and.getChildren()) {
                if(n instanceof AndNode) {
                    if(!visit((AndNode)n, record))
                        return false;
                } else if(n instanceof OrNode) {
                    if(!visit((OrNode)n, record))
                        return false;
                } else if(n instanceof ComparisonNode) {
                    if(!visit((ComparisonNode)n, record))
                        return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visit(OrNode or, ProductType18 record) {
            for(Node n : or.getChildren()) {
                if(n instanceof AndNode) {
                    if(visit((AndNode)n, record))
                        return true;
                } else if(n instanceof OrNode) {
                    if(visit((OrNode)n, record))
                        return true;
                } else if(n instanceof ComparisonNode) {
                    if(visit((ComparisonNode)n, record))
                        return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(ComparisonNode cn, ProductType18 record) {

            Set<String> actual = record.getStringSet(cn.getSelector());
            if(actual == null) {
                return false;
            }
            Set<String> expected = new HashSet<>(cn.getArguments());

            if(cn.getOperator().getSymbol().equals("=lt=") || 
               cn.getOperator().getSymbol().equals("=gt=") ||
               cn.getOperator().getSymbol().equals("=le=") ||
               cn.getOperator().getSymbol().equals("=ge=")) {
            
                double actualDouble = Double.parseDouble(actual.iterator().next());
                double expectedDouble = Double.parseDouble(expected.iterator().next());
                
                if(cn.getOperator().getSymbol().equals("=lt=")) {
                    return actualDouble < expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=gt=")) {
                    return actualDouble > expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=le=")) {
                    return actualDouble <= expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=ge=")) {
                    return actualDouble >= expectedDouble;
                }
            } else if(cn.getOperator().getSymbol().equals("==")) {
                return actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("!=")) {
                return !actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("=in=")) {
                return actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=out=")) {
                return !actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=regex=")) {
                String patternStr = expected.iterator().next();
                Pattern pattern = Pattern.compile(patternStr);
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        Matcher matcher = pattern.matcher(act);
                        if(matcher.find())
                            return true;
                    }
                }
                return false;
            } else if(cn.getOperator().getSymbol().equals("=lang=")) {
                String lang = expected.iterator().next();
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        if(act.endsWith("@" + lang))
                            return true;
                    }
                }
                return false;
            }

            return false;
        }
    };

    public List<ProductType18> selectProductType18() {
        return selectProductType18(null, null, null, null);
    }

    public List<ProductType18> selectProductType18(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM product_type18\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        if(limit != null) {
            querySB.append("\nLIMIT " + limit);
        }
        if(offset != null) {
            querySB.append("\nOFFSET " + offset);
        }

        String query = querySB.toString();

        List<ProductType18> result = SqlUtils.supply(connection, c -> {
            List<ProductType18> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                ProductType18 record = new ProductType18();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setLabel(SqlUtils.getString(rs, "label"));
                record.setComment(SqlUtils.getString(rs, "comment"));
                record.setProductPropertyTextual1(SqlUtils.getString(rs, "product_property_textual1"));
                record.setProductPropertyTextual2(SqlUtils.getString(rs, "product_property_textual2"));
                record.setProductPropertyTextual3(SqlUtils.getString(rs, "product_property_textual3"));
                record.setProductPropertyTextual4(SqlUtils.getString(rs, "product_property_textual4"));
                record.setProductPropertyTextual5(SqlUtils.getString(rs, "product_property_textual5"));
                record.setProductPropertyTextual6(SqlUtils.getString(rs, "product_property_textual6"));
                record.setPublisher(SqlUtils.getString(rs, "publisher"));
                record.setDate(SqlUtils.getLong(rs, "date"));
                record.setProducer(SqlUtils.getLong(rs, "producer"));
                record.setProductPropertyNumeric1(SqlUtils.getLong(rs, "product_property_numeric1"));
                record.setProductPropertyNumeric2(SqlUtils.getLong(rs, "product_property_numeric2"));
                record.setProductPropertyNumeric3(SqlUtils.getLong(rs, "product_property_numeric3"));
                record.setProductPropertyNumeric4(SqlUtils.getLong(rs, "product_property_numeric4"));
                record.setProductPropertyNumeric5(SqlUtils.getLong(rs, "product_property_numeric5"));
                record.setProductPropertyNumeric6(SqlUtils.getLong(rs, "product_property_numeric6"));

                record.setProductFeatureProductFeature(selectWithNM(record.getId(), "product_type18_id", "mn_product_type18_product_feature_product_feature", Long.class));
                record.setType(selectWithNM(record.getId(), "resource_id", "mn_resource_type_class", Long.class));
                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(ProductType18Visitor, record));
        }

        return result;
    }

    public List<ProductType18> insertProductType18(List<ProductType18> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO product_type18 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(ProductType18 record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getLabel());
                ps.setObject(3, record.getComment());
                ps.setObject(4, record.getProductPropertyTextual1());
                ps.setObject(5, record.getProductPropertyTextual2());
                ps.setObject(6, record.getProductPropertyTextual3());
                ps.setObject(7, record.getProductPropertyTextual4());
                ps.setObject(8, record.getProductPropertyTextual5());
                ps.setObject(9, record.getProductPropertyTextual6());
                ps.setObject(10, record.getPublisher());
                ps.setObject(11, record.getDate());
                ps.setObject(12, record.getProducer());
                ps.setObject(13, record.getProductPropertyNumeric1());
                ps.setObject(14, record.getProductPropertyNumeric2());
                ps.setObject(15, record.getProductPropertyNumeric3());
                ps.setObject(16, record.getProductPropertyNumeric4());
                ps.setObject(17, record.getProductPropertyNumeric5());
                ps.setObject(18, record.getProductPropertyNumeric6());
                ps.addBatch();
            }
            ps.executeBatch();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            for(int i = records.size()-1; i >= 0; i--) {
                //does only return the last generated id
                if(!generatedKeys.next())
                    break;

                long id = generatedKeys.getLong(1);
                records.get(i).setId(id);
            }
            c.commit();
            ps.close();

            boolean wasRecordsAdded = false;

            //n:m tables
            query = "INSERT INTO mn_product_type18_product_feature_product_feature VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType18 record : records) {
                if(!record.hasId() || !record.hasProductFeatureProductFeature() || record.getProductFeatureProductFeature().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getProductFeatureProductFeature()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;

            query = "INSERT INTO mn_resource_type_class VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType18 record : records) {
                if(!record.hasId() || !record.hasType() || record.getType().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getType()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;


            c.setAutoCommit(true);
        });

        return records;
    }

    public List<ProductType18> updateProductType18(List<ProductType18> records, boolean put) {    
        for(ProductType18 record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasLabel()) {
                    paramSJ.add("label = ?");
                }
                if(put || record.hasComment()) {
                    paramSJ.add("comment = ?");
                }
                if(put || record.hasProductPropertyTextual1()) {
                    paramSJ.add("product_property_textual1 = ?");
                }
                if(put || record.hasProductPropertyTextual2()) {
                    paramSJ.add("product_property_textual2 = ?");
                }
                if(put || record.hasProductPropertyTextual3()) {
                    paramSJ.add("product_property_textual3 = ?");
                }
                if(put || record.hasProductPropertyTextual4()) {
                    paramSJ.add("product_property_textual4 = ?");
                }
                if(put || record.hasProductPropertyTextual5()) {
                    paramSJ.add("product_property_textual5 = ?");
                }
                if(put || record.hasProductPropertyTextual6()) {
                    paramSJ.add("product_property_textual6 = ?");
                }
                if(put || record.hasPublisher()) {
                    paramSJ.add("publisher = ?");
                }
                if(put || record.hasDate()) {
                    paramSJ.add("date = ?");
                }
                if(put || record.hasProducer()) {
                    paramSJ.add("producer = ?");
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    paramSJ.add("product_property_numeric1 = ?");
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    paramSJ.add("product_property_numeric2 = ?");
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    paramSJ.add("product_property_numeric3 = ?");
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    paramSJ.add("product_property_numeric4 = ?");
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    paramSJ.add("product_property_numeric5 = ?");
                }
                if(put || record.hasProductPropertyNumeric6()) {
                    paramSJ.add("product_property_numeric6 = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE product_type18 SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasLabel()) {
                    ps.setObject(index, record.getLabel());
                    index++;
                }
                if(put || record.hasComment()) {
                    ps.setObject(index, record.getComment());
                    index++;
                }
                if(put || record.hasProductPropertyTextual1()) {
                    ps.setObject(index, record.getProductPropertyTextual1());
                    index++;
                }
                if(put || record.hasProductPropertyTextual2()) {
                    ps.setObject(index, record.getProductPropertyTextual2());
                    index++;
                }
                if(put || record.hasProductPropertyTextual3()) {
                    ps.setObject(index, record.getProductPropertyTextual3());
                    index++;
                }
                if(put || record.hasProductPropertyTextual4()) {
                    ps.setObject(index, record.getProductPropertyTextual4());
                    index++;
                }
                if(put || record.hasProductPropertyTextual5()) {
                    ps.setObject(index, record.getProductPropertyTextual5());
                    index++;
                }
                if(put || record.hasProductPropertyTextual6()) {
                    ps.setObject(index, record.getProductPropertyTextual6());
                    index++;
                }
                if(put || record.hasPublisher()) {
                    ps.setObject(index, record.getPublisher());
                    index++;
                }
                if(put || record.hasDate()) {
                    ps.setObject(index, record.getDate());
                    index++;
                }
                if(put || record.hasProducer()) {
                    ps.setObject(index, record.getProducer());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    ps.setObject(index, record.getProductPropertyNumeric1());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    ps.setObject(index, record.getProductPropertyNumeric2());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    ps.setObject(index, record.getProductPropertyNumeric3());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    ps.setObject(index, record.getProductPropertyNumeric4());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    ps.setObject(index, record.getProductPropertyNumeric5());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric6()) {
                    ps.setObject(index, record.getProductPropertyNumeric6());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
                if(put || (record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_product_type18_product_feature_product_feature WHERE product_type18_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_product_type18_product_feature_product_feature VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getProductFeatureProductFeature()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
                if(put || (record.hasType() && !record.getType().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_resource_type_class WHERE resource_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasType() && !record.getType().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_resource_type_class VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getType()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
            });
        }

        return records;
    }

    public void deleteProductType18() {
        deleteProductType18(null);
    }    

    public void deleteProductType18(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM product_type18");
        String in = "";
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            in = "IN (" + sj.toString() + ")";
            querySB.append(" WHERE id " + in);
        }

        final String finalIn = in;
        final String query = querySB.toString();

        SqlUtils.run(connection, c -> {
            PreparedStatement ps = c.prepareStatement(query);
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_product_type18_product_feature_product_feature" + (finalIn.isEmpty() ? "" : " WHERE product_type18_id " + finalIn));
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_resource_type_class" + (finalIn.isEmpty() ? "" : " WHERE resource_id " + finalIn));
            ps.execute();
            ps.close();

        });
    }

    //==========================================================================
    // ProductType15

    private RSQLVisitor<Boolean, ProductType15> ProductType15Visitor = new RSQLVisitor<Boolean, ProductType15>() {

        @Override
        public Boolean visit(AndNode and, ProductType15 record) {
            for(Node n : and.getChildren()) {
                if(n instanceof AndNode) {
                    if(!visit((AndNode)n, record))
                        return false;
                } else if(n instanceof OrNode) {
                    if(!visit((OrNode)n, record))
                        return false;
                } else if(n instanceof ComparisonNode) {
                    if(!visit((ComparisonNode)n, record))
                        return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visit(OrNode or, ProductType15 record) {
            for(Node n : or.getChildren()) {
                if(n instanceof AndNode) {
                    if(visit((AndNode)n, record))
                        return true;
                } else if(n instanceof OrNode) {
                    if(visit((OrNode)n, record))
                        return true;
                } else if(n instanceof ComparisonNode) {
                    if(visit((ComparisonNode)n, record))
                        return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(ComparisonNode cn, ProductType15 record) {

            Set<String> actual = record.getStringSet(cn.getSelector());
            if(actual == null) {
                return false;
            }
            Set<String> expected = new HashSet<>(cn.getArguments());

            if(cn.getOperator().getSymbol().equals("=lt=") || 
               cn.getOperator().getSymbol().equals("=gt=") ||
               cn.getOperator().getSymbol().equals("=le=") ||
               cn.getOperator().getSymbol().equals("=ge=")) {
            
                double actualDouble = Double.parseDouble(actual.iterator().next());
                double expectedDouble = Double.parseDouble(expected.iterator().next());
                
                if(cn.getOperator().getSymbol().equals("=lt=")) {
                    return actualDouble < expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=gt=")) {
                    return actualDouble > expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=le=")) {
                    return actualDouble <= expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=ge=")) {
                    return actualDouble >= expectedDouble;
                }
            } else if(cn.getOperator().getSymbol().equals("==")) {
                return actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("!=")) {
                return !actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("=in=")) {
                return actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=out=")) {
                return !actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=regex=")) {
                String patternStr = expected.iterator().next();
                Pattern pattern = Pattern.compile(patternStr);
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        Matcher matcher = pattern.matcher(act);
                        if(matcher.find())
                            return true;
                    }
                }
                return false;
            } else if(cn.getOperator().getSymbol().equals("=lang=")) {
                String lang = expected.iterator().next();
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        if(act.endsWith("@" + lang))
                            return true;
                    }
                }
                return false;
            }

            return false;
        }
    };

    public List<ProductType15> selectProductType15() {
        return selectProductType15(null, null, null, null);
    }

    public List<ProductType15> selectProductType15(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM product_type15\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        if(limit != null) {
            querySB.append("\nLIMIT " + limit);
        }
        if(offset != null) {
            querySB.append("\nOFFSET " + offset);
        }

        String query = querySB.toString();

        List<ProductType15> result = SqlUtils.supply(connection, c -> {
            List<ProductType15> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                ProductType15 record = new ProductType15();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setLabel(SqlUtils.getString(rs, "label"));
                record.setComment(SqlUtils.getString(rs, "comment"));
                record.setProductPropertyTextual1(SqlUtils.getString(rs, "product_property_textual1"));
                record.setProductPropertyTextual2(SqlUtils.getString(rs, "product_property_textual2"));
                record.setProductPropertyTextual3(SqlUtils.getString(rs, "product_property_textual3"));
                record.setProductPropertyTextual4(SqlUtils.getString(rs, "product_property_textual4"));
                record.setProductPropertyTextual5(SqlUtils.getString(rs, "product_property_textual5"));
                record.setProductPropertyTextual6(SqlUtils.getString(rs, "product_property_textual6"));
                record.setPublisher(SqlUtils.getString(rs, "publisher"));
                record.setDate(SqlUtils.getLong(rs, "date"));
                record.setProducer(SqlUtils.getLong(rs, "producer"));
                record.setProductPropertyNumeric1(SqlUtils.getLong(rs, "product_property_numeric1"));
                record.setProductPropertyNumeric2(SqlUtils.getLong(rs, "product_property_numeric2"));
                record.setProductPropertyNumeric3(SqlUtils.getLong(rs, "product_property_numeric3"));
                record.setProductPropertyNumeric4(SqlUtils.getLong(rs, "product_property_numeric4"));
                record.setProductPropertyNumeric5(SqlUtils.getLong(rs, "product_property_numeric5"));
                record.setProductPropertyNumeric6(SqlUtils.getLong(rs, "product_property_numeric6"));

                record.setProductFeatureProductFeature(selectWithNM(record.getId(), "product_type15_id", "mn_product_type15_product_feature_product_feature", Long.class));
                record.setType(selectWithNM(record.getId(), "resource_id", "mn_resource_type_class", Long.class));
                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(ProductType15Visitor, record));
        }

        return result;
    }

    public List<ProductType15> insertProductType15(List<ProductType15> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO product_type15 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(ProductType15 record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getLabel());
                ps.setObject(3, record.getComment());
                ps.setObject(4, record.getProductPropertyTextual1());
                ps.setObject(5, record.getProductPropertyTextual2());
                ps.setObject(6, record.getProductPropertyTextual3());
                ps.setObject(7, record.getProductPropertyTextual4());
                ps.setObject(8, record.getProductPropertyTextual5());
                ps.setObject(9, record.getProductPropertyTextual6());
                ps.setObject(10, record.getPublisher());
                ps.setObject(11, record.getDate());
                ps.setObject(12, record.getProducer());
                ps.setObject(13, record.getProductPropertyNumeric1());
                ps.setObject(14, record.getProductPropertyNumeric2());
                ps.setObject(15, record.getProductPropertyNumeric3());
                ps.setObject(16, record.getProductPropertyNumeric4());
                ps.setObject(17, record.getProductPropertyNumeric5());
                ps.setObject(18, record.getProductPropertyNumeric6());
                ps.addBatch();
            }
            ps.executeBatch();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            for(int i = records.size()-1; i >= 0; i--) {
                //does only return the last generated id
                if(!generatedKeys.next())
                    break;

                long id = generatedKeys.getLong(1);
                records.get(i).setId(id);
            }
            c.commit();
            ps.close();

            boolean wasRecordsAdded = false;

            //n:m tables
            query = "INSERT INTO mn_product_type15_product_feature_product_feature VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType15 record : records) {
                if(!record.hasId() || !record.hasProductFeatureProductFeature() || record.getProductFeatureProductFeature().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getProductFeatureProductFeature()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;

            query = "INSERT INTO mn_resource_type_class VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType15 record : records) {
                if(!record.hasId() || !record.hasType() || record.getType().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getType()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;


            c.setAutoCommit(true);
        });

        return records;
    }

    public List<ProductType15> updateProductType15(List<ProductType15> records, boolean put) {    
        for(ProductType15 record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasLabel()) {
                    paramSJ.add("label = ?");
                }
                if(put || record.hasComment()) {
                    paramSJ.add("comment = ?");
                }
                if(put || record.hasProductPropertyTextual1()) {
                    paramSJ.add("product_property_textual1 = ?");
                }
                if(put || record.hasProductPropertyTextual2()) {
                    paramSJ.add("product_property_textual2 = ?");
                }
                if(put || record.hasProductPropertyTextual3()) {
                    paramSJ.add("product_property_textual3 = ?");
                }
                if(put || record.hasProductPropertyTextual4()) {
                    paramSJ.add("product_property_textual4 = ?");
                }
                if(put || record.hasProductPropertyTextual5()) {
                    paramSJ.add("product_property_textual5 = ?");
                }
                if(put || record.hasProductPropertyTextual6()) {
                    paramSJ.add("product_property_textual6 = ?");
                }
                if(put || record.hasPublisher()) {
                    paramSJ.add("publisher = ?");
                }
                if(put || record.hasDate()) {
                    paramSJ.add("date = ?");
                }
                if(put || record.hasProducer()) {
                    paramSJ.add("producer = ?");
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    paramSJ.add("product_property_numeric1 = ?");
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    paramSJ.add("product_property_numeric2 = ?");
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    paramSJ.add("product_property_numeric3 = ?");
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    paramSJ.add("product_property_numeric4 = ?");
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    paramSJ.add("product_property_numeric5 = ?");
                }
                if(put || record.hasProductPropertyNumeric6()) {
                    paramSJ.add("product_property_numeric6 = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE product_type15 SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasLabel()) {
                    ps.setObject(index, record.getLabel());
                    index++;
                }
                if(put || record.hasComment()) {
                    ps.setObject(index, record.getComment());
                    index++;
                }
                if(put || record.hasProductPropertyTextual1()) {
                    ps.setObject(index, record.getProductPropertyTextual1());
                    index++;
                }
                if(put || record.hasProductPropertyTextual2()) {
                    ps.setObject(index, record.getProductPropertyTextual2());
                    index++;
                }
                if(put || record.hasProductPropertyTextual3()) {
                    ps.setObject(index, record.getProductPropertyTextual3());
                    index++;
                }
                if(put || record.hasProductPropertyTextual4()) {
                    ps.setObject(index, record.getProductPropertyTextual4());
                    index++;
                }
                if(put || record.hasProductPropertyTextual5()) {
                    ps.setObject(index, record.getProductPropertyTextual5());
                    index++;
                }
                if(put || record.hasProductPropertyTextual6()) {
                    ps.setObject(index, record.getProductPropertyTextual6());
                    index++;
                }
                if(put || record.hasPublisher()) {
                    ps.setObject(index, record.getPublisher());
                    index++;
                }
                if(put || record.hasDate()) {
                    ps.setObject(index, record.getDate());
                    index++;
                }
                if(put || record.hasProducer()) {
                    ps.setObject(index, record.getProducer());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    ps.setObject(index, record.getProductPropertyNumeric1());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    ps.setObject(index, record.getProductPropertyNumeric2());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    ps.setObject(index, record.getProductPropertyNumeric3());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    ps.setObject(index, record.getProductPropertyNumeric4());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    ps.setObject(index, record.getProductPropertyNumeric5());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric6()) {
                    ps.setObject(index, record.getProductPropertyNumeric6());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
                if(put || (record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_product_type15_product_feature_product_feature WHERE product_type15_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_product_type15_product_feature_product_feature VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getProductFeatureProductFeature()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
                if(put || (record.hasType() && !record.getType().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_resource_type_class WHERE resource_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasType() && !record.getType().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_resource_type_class VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getType()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
            });
        }

        return records;
    }

    public void deleteProductType15() {
        deleteProductType15(null);
    }    

    public void deleteProductType15(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM product_type15");
        String in = "";
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            in = "IN (" + sj.toString() + ")";
            querySB.append(" WHERE id " + in);
        }

        final String finalIn = in;
        final String query = querySB.toString();

        SqlUtils.run(connection, c -> {
            PreparedStatement ps = c.prepareStatement(query);
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_product_type15_product_feature_product_feature" + (finalIn.isEmpty() ? "" : " WHERE product_type15_id " + finalIn));
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_resource_type_class" + (finalIn.isEmpty() ? "" : " WHERE resource_id " + finalIn));
            ps.execute();
            ps.close();

        });
    }

    //==========================================================================
    // Vendor

    private RSQLVisitor<Boolean, Vendor> VendorVisitor = new RSQLVisitor<Boolean, Vendor>() {

        @Override
        public Boolean visit(AndNode and, Vendor record) {
            for(Node n : and.getChildren()) {
                if(n instanceof AndNode) {
                    if(!visit((AndNode)n, record))
                        return false;
                } else if(n instanceof OrNode) {
                    if(!visit((OrNode)n, record))
                        return false;
                } else if(n instanceof ComparisonNode) {
                    if(!visit((ComparisonNode)n, record))
                        return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visit(OrNode or, Vendor record) {
            for(Node n : or.getChildren()) {
                if(n instanceof AndNode) {
                    if(visit((AndNode)n, record))
                        return true;
                } else if(n instanceof OrNode) {
                    if(visit((OrNode)n, record))
                        return true;
                } else if(n instanceof ComparisonNode) {
                    if(visit((ComparisonNode)n, record))
                        return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(ComparisonNode cn, Vendor record) {

            Set<String> actual = record.getStringSet(cn.getSelector());
            if(actual == null) {
                return false;
            }
            Set<String> expected = new HashSet<>(cn.getArguments());

            if(cn.getOperator().getSymbol().equals("=lt=") || 
               cn.getOperator().getSymbol().equals("=gt=") ||
               cn.getOperator().getSymbol().equals("=le=") ||
               cn.getOperator().getSymbol().equals("=ge=")) {
            
                double actualDouble = Double.parseDouble(actual.iterator().next());
                double expectedDouble = Double.parseDouble(expected.iterator().next());
                
                if(cn.getOperator().getSymbol().equals("=lt=")) {
                    return actualDouble < expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=gt=")) {
                    return actualDouble > expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=le=")) {
                    return actualDouble <= expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=ge=")) {
                    return actualDouble >= expectedDouble;
                }
            } else if(cn.getOperator().getSymbol().equals("==")) {
                return actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("!=")) {
                return !actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("=in=")) {
                return actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=out=")) {
                return !actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=regex=")) {
                String patternStr = expected.iterator().next();
                Pattern pattern = Pattern.compile(patternStr);
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        Matcher matcher = pattern.matcher(act);
                        if(matcher.find())
                            return true;
                    }
                }
                return false;
            } else if(cn.getOperator().getSymbol().equals("=lang=")) {
                String lang = expected.iterator().next();
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        if(act.endsWith("@" + lang))
                            return true;
                    }
                }
                return false;
            }

            return false;
        }
    };

    public List<Vendor> selectVendor() {
        return selectVendor(null, null, null, null);
    }

    public List<Vendor> selectVendor(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM vendor\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        if(limit != null) {
            querySB.append("\nLIMIT " + limit);
        }
        if(offset != null) {
            querySB.append("\nOFFSET " + offset);
        }

        String query = querySB.toString();

        List<Vendor> result = SqlUtils.supply(connection, c -> {
            List<Vendor> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                Vendor record = new Vendor();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setLabel(SqlUtils.getString(rs, "label"));
                record.setComment(SqlUtils.getString(rs, "comment"));
                record.setCountry(SqlUtils.getString(rs, "country"));
                record.setHomepage(SqlUtils.getString(rs, "homepage"));
                record.setPublisher(SqlUtils.getString(rs, "publisher"));
                record.setDate(SqlUtils.getLong(rs, "date"));

                record.setType(selectWithNM(record.getId(), "resource_id", "mn_resource_type_class", Long.class));
                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(VendorVisitor, record));
        }

        return result;
    }

    public List<Vendor> insertVendor(List<Vendor> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO vendor VALUES (?,?,?,?,?,?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(Vendor record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getLabel());
                ps.setObject(3, record.getComment());
                ps.setObject(4, record.getCountry());
                ps.setObject(5, record.getHomepage());
                ps.setObject(6, record.getPublisher());
                ps.setObject(7, record.getDate());
                ps.addBatch();
            }
            ps.executeBatch();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            for(int i = records.size()-1; i >= 0; i--) {
                //does only return the last generated id
                if(!generatedKeys.next())
                    break;

                long id = generatedKeys.getLong(1);
                records.get(i).setId(id);
            }
            c.commit();
            ps.close();

            boolean wasRecordsAdded = false;

            //n:m tables
            query = "INSERT INTO mn_resource_type_class VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(Vendor record : records) {
                if(!record.hasId() || !record.hasType() || record.getType().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getType()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;


            c.setAutoCommit(true);
        });

        return records;
    }

    public List<Vendor> updateVendor(List<Vendor> records, boolean put) {    
        for(Vendor record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasLabel()) {
                    paramSJ.add("label = ?");
                }
                if(put || record.hasComment()) {
                    paramSJ.add("comment = ?");
                }
                if(put || record.hasCountry()) {
                    paramSJ.add("country = ?");
                }
                if(put || record.hasHomepage()) {
                    paramSJ.add("homepage = ?");
                }
                if(put || record.hasPublisher()) {
                    paramSJ.add("publisher = ?");
                }
                if(put || record.hasDate()) {
                    paramSJ.add("date = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE vendor SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasLabel()) {
                    ps.setObject(index, record.getLabel());
                    index++;
                }
                if(put || record.hasComment()) {
                    ps.setObject(index, record.getComment());
                    index++;
                }
                if(put || record.hasCountry()) {
                    ps.setObject(index, record.getCountry());
                    index++;
                }
                if(put || record.hasHomepage()) {
                    ps.setObject(index, record.getHomepage());
                    index++;
                }
                if(put || record.hasPublisher()) {
                    ps.setObject(index, record.getPublisher());
                    index++;
                }
                if(put || record.hasDate()) {
                    ps.setObject(index, record.getDate());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
                if(put || (record.hasType() && !record.getType().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_resource_type_class WHERE resource_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasType() && !record.getType().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_resource_type_class VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getType()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
            });
        }

        return records;
    }

    public void deleteVendor() {
        deleteVendor(null);
    }    

    public void deleteVendor(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM vendor");
        String in = "";
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            in = "IN (" + sj.toString() + ")";
            querySB.append(" WHERE id " + in);
        }

        final String finalIn = in;
        final String query = querySB.toString();

        SqlUtils.run(connection, c -> {
            PreparedStatement ps = c.prepareStatement(query);
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_resource_type_class" + (finalIn.isEmpty() ? "" : " WHERE resource_id " + finalIn));
            ps.execute();
            ps.close();

        });
    }

    //==========================================================================
    // ProductType16

    private RSQLVisitor<Boolean, ProductType16> ProductType16Visitor = new RSQLVisitor<Boolean, ProductType16>() {

        @Override
        public Boolean visit(AndNode and, ProductType16 record) {
            for(Node n : and.getChildren()) {
                if(n instanceof AndNode) {
                    if(!visit((AndNode)n, record))
                        return false;
                } else if(n instanceof OrNode) {
                    if(!visit((OrNode)n, record))
                        return false;
                } else if(n instanceof ComparisonNode) {
                    if(!visit((ComparisonNode)n, record))
                        return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visit(OrNode or, ProductType16 record) {
            for(Node n : or.getChildren()) {
                if(n instanceof AndNode) {
                    if(visit((AndNode)n, record))
                        return true;
                } else if(n instanceof OrNode) {
                    if(visit((OrNode)n, record))
                        return true;
                } else if(n instanceof ComparisonNode) {
                    if(visit((ComparisonNode)n, record))
                        return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(ComparisonNode cn, ProductType16 record) {

            Set<String> actual = record.getStringSet(cn.getSelector());
            if(actual == null) {
                return false;
            }
            Set<String> expected = new HashSet<>(cn.getArguments());

            if(cn.getOperator().getSymbol().equals("=lt=") || 
               cn.getOperator().getSymbol().equals("=gt=") ||
               cn.getOperator().getSymbol().equals("=le=") ||
               cn.getOperator().getSymbol().equals("=ge=")) {
            
                double actualDouble = Double.parseDouble(actual.iterator().next());
                double expectedDouble = Double.parseDouble(expected.iterator().next());
                
                if(cn.getOperator().getSymbol().equals("=lt=")) {
                    return actualDouble < expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=gt=")) {
                    return actualDouble > expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=le=")) {
                    return actualDouble <= expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=ge=")) {
                    return actualDouble >= expectedDouble;
                }
            } else if(cn.getOperator().getSymbol().equals("==")) {
                return actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("!=")) {
                return !actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("=in=")) {
                return actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=out=")) {
                return !actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=regex=")) {
                String patternStr = expected.iterator().next();
                Pattern pattern = Pattern.compile(patternStr);
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        Matcher matcher = pattern.matcher(act);
                        if(matcher.find())
                            return true;
                    }
                }
                return false;
            } else if(cn.getOperator().getSymbol().equals("=lang=")) {
                String lang = expected.iterator().next();
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        if(act.endsWith("@" + lang))
                            return true;
                    }
                }
                return false;
            }

            return false;
        }
    };

    public List<ProductType16> selectProductType16() {
        return selectProductType16(null, null, null, null);
    }

    public List<ProductType16> selectProductType16(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM product_type16\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        if(limit != null) {
            querySB.append("\nLIMIT " + limit);
        }
        if(offset != null) {
            querySB.append("\nOFFSET " + offset);
        }

        String query = querySB.toString();

        List<ProductType16> result = SqlUtils.supply(connection, c -> {
            List<ProductType16> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                ProductType16 record = new ProductType16();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setLabel(SqlUtils.getString(rs, "label"));
                record.setComment(SqlUtils.getString(rs, "comment"));
                record.setProductPropertyTextual1(SqlUtils.getString(rs, "product_property_textual1"));
                record.setProductPropertyTextual2(SqlUtils.getString(rs, "product_property_textual2"));
                record.setProductPropertyTextual3(SqlUtils.getString(rs, "product_property_textual3"));
                record.setProductPropertyTextual4(SqlUtils.getString(rs, "product_property_textual4"));
                record.setProductPropertyTextual5(SqlUtils.getString(rs, "product_property_textual5"));
                record.setPublisher(SqlUtils.getString(rs, "publisher"));
                record.setDate(SqlUtils.getLong(rs, "date"));
                record.setProducer(SqlUtils.getLong(rs, "producer"));
                record.setProductPropertyNumeric1(SqlUtils.getLong(rs, "product_property_numeric1"));
                record.setProductPropertyNumeric2(SqlUtils.getLong(rs, "product_property_numeric2"));
                record.setProductPropertyNumeric3(SqlUtils.getLong(rs, "product_property_numeric3"));
                record.setProductPropertyNumeric4(SqlUtils.getLong(rs, "product_property_numeric4"));
                record.setProductPropertyNumeric5(SqlUtils.getLong(rs, "product_property_numeric5"));

                record.setProductFeatureProductFeature(selectWithNM(record.getId(), "product_type16_id", "mn_product_type16_product_feature_product_feature", Long.class));
                record.setType(selectWithNM(record.getId(), "resource_id", "mn_resource_type_class", Long.class));
                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(ProductType16Visitor, record));
        }

        return result;
    }

    public List<ProductType16> insertProductType16(List<ProductType16> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO product_type16 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(ProductType16 record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getLabel());
                ps.setObject(3, record.getComment());
                ps.setObject(4, record.getProductPropertyTextual1());
                ps.setObject(5, record.getProductPropertyTextual2());
                ps.setObject(6, record.getProductPropertyTextual3());
                ps.setObject(7, record.getProductPropertyTextual4());
                ps.setObject(8, record.getProductPropertyTextual5());
                ps.setObject(9, record.getPublisher());
                ps.setObject(10, record.getDate());
                ps.setObject(11, record.getProducer());
                ps.setObject(12, record.getProductPropertyNumeric1());
                ps.setObject(13, record.getProductPropertyNumeric2());
                ps.setObject(14, record.getProductPropertyNumeric3());
                ps.setObject(15, record.getProductPropertyNumeric4());
                ps.setObject(16, record.getProductPropertyNumeric5());
                ps.addBatch();
            }
            ps.executeBatch();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            for(int i = records.size()-1; i >= 0; i--) {
                //does only return the last generated id
                if(!generatedKeys.next())
                    break;

                long id = generatedKeys.getLong(1);
                records.get(i).setId(id);
            }
            c.commit();
            ps.close();

            boolean wasRecordsAdded = false;

            //n:m tables
            query = "INSERT INTO mn_product_type16_product_feature_product_feature VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType16 record : records) {
                if(!record.hasId() || !record.hasProductFeatureProductFeature() || record.getProductFeatureProductFeature().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getProductFeatureProductFeature()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;

            query = "INSERT INTO mn_resource_type_class VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType16 record : records) {
                if(!record.hasId() || !record.hasType() || record.getType().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getType()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;


            c.setAutoCommit(true);
        });

        return records;
    }

    public List<ProductType16> updateProductType16(List<ProductType16> records, boolean put) {    
        for(ProductType16 record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasLabel()) {
                    paramSJ.add("label = ?");
                }
                if(put || record.hasComment()) {
                    paramSJ.add("comment = ?");
                }
                if(put || record.hasProductPropertyTextual1()) {
                    paramSJ.add("product_property_textual1 = ?");
                }
                if(put || record.hasProductPropertyTextual2()) {
                    paramSJ.add("product_property_textual2 = ?");
                }
                if(put || record.hasProductPropertyTextual3()) {
                    paramSJ.add("product_property_textual3 = ?");
                }
                if(put || record.hasProductPropertyTextual4()) {
                    paramSJ.add("product_property_textual4 = ?");
                }
                if(put || record.hasProductPropertyTextual5()) {
                    paramSJ.add("product_property_textual5 = ?");
                }
                if(put || record.hasPublisher()) {
                    paramSJ.add("publisher = ?");
                }
                if(put || record.hasDate()) {
                    paramSJ.add("date = ?");
                }
                if(put || record.hasProducer()) {
                    paramSJ.add("producer = ?");
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    paramSJ.add("product_property_numeric1 = ?");
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    paramSJ.add("product_property_numeric2 = ?");
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    paramSJ.add("product_property_numeric3 = ?");
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    paramSJ.add("product_property_numeric4 = ?");
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    paramSJ.add("product_property_numeric5 = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE product_type16 SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasLabel()) {
                    ps.setObject(index, record.getLabel());
                    index++;
                }
                if(put || record.hasComment()) {
                    ps.setObject(index, record.getComment());
                    index++;
                }
                if(put || record.hasProductPropertyTextual1()) {
                    ps.setObject(index, record.getProductPropertyTextual1());
                    index++;
                }
                if(put || record.hasProductPropertyTextual2()) {
                    ps.setObject(index, record.getProductPropertyTextual2());
                    index++;
                }
                if(put || record.hasProductPropertyTextual3()) {
                    ps.setObject(index, record.getProductPropertyTextual3());
                    index++;
                }
                if(put || record.hasProductPropertyTextual4()) {
                    ps.setObject(index, record.getProductPropertyTextual4());
                    index++;
                }
                if(put || record.hasProductPropertyTextual5()) {
                    ps.setObject(index, record.getProductPropertyTextual5());
                    index++;
                }
                if(put || record.hasPublisher()) {
                    ps.setObject(index, record.getPublisher());
                    index++;
                }
                if(put || record.hasDate()) {
                    ps.setObject(index, record.getDate());
                    index++;
                }
                if(put || record.hasProducer()) {
                    ps.setObject(index, record.getProducer());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    ps.setObject(index, record.getProductPropertyNumeric1());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    ps.setObject(index, record.getProductPropertyNumeric2());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    ps.setObject(index, record.getProductPropertyNumeric3());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    ps.setObject(index, record.getProductPropertyNumeric4());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    ps.setObject(index, record.getProductPropertyNumeric5());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
                if(put || (record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_product_type16_product_feature_product_feature WHERE product_type16_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_product_type16_product_feature_product_feature VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getProductFeatureProductFeature()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
                if(put || (record.hasType() && !record.getType().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_resource_type_class WHERE resource_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasType() && !record.getType().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_resource_type_class VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getType()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
            });
        }

        return records;
    }

    public void deleteProductType16() {
        deleteProductType16(null);
    }    

    public void deleteProductType16(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM product_type16");
        String in = "";
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            in = "IN (" + sj.toString() + ")";
            querySB.append(" WHERE id " + in);
        }

        final String finalIn = in;
        final String query = querySB.toString();

        SqlUtils.run(connection, c -> {
            PreparedStatement ps = c.prepareStatement(query);
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_product_type16_product_feature_product_feature" + (finalIn.isEmpty() ? "" : " WHERE product_type16_id " + finalIn));
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_resource_type_class" + (finalIn.isEmpty() ? "" : " WHERE resource_id " + finalIn));
            ps.execute();
            ps.close();

        });
    }

    //==========================================================================
    // ProductType13

    private RSQLVisitor<Boolean, ProductType13> ProductType13Visitor = new RSQLVisitor<Boolean, ProductType13>() {

        @Override
        public Boolean visit(AndNode and, ProductType13 record) {
            for(Node n : and.getChildren()) {
                if(n instanceof AndNode) {
                    if(!visit((AndNode)n, record))
                        return false;
                } else if(n instanceof OrNode) {
                    if(!visit((OrNode)n, record))
                        return false;
                } else if(n instanceof ComparisonNode) {
                    if(!visit((ComparisonNode)n, record))
                        return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visit(OrNode or, ProductType13 record) {
            for(Node n : or.getChildren()) {
                if(n instanceof AndNode) {
                    if(visit((AndNode)n, record))
                        return true;
                } else if(n instanceof OrNode) {
                    if(visit((OrNode)n, record))
                        return true;
                } else if(n instanceof ComparisonNode) {
                    if(visit((ComparisonNode)n, record))
                        return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(ComparisonNode cn, ProductType13 record) {

            Set<String> actual = record.getStringSet(cn.getSelector());
            if(actual == null) {
                return false;
            }
            Set<String> expected = new HashSet<>(cn.getArguments());

            if(cn.getOperator().getSymbol().equals("=lt=") || 
               cn.getOperator().getSymbol().equals("=gt=") ||
               cn.getOperator().getSymbol().equals("=le=") ||
               cn.getOperator().getSymbol().equals("=ge=")) {
            
                double actualDouble = Double.parseDouble(actual.iterator().next());
                double expectedDouble = Double.parseDouble(expected.iterator().next());
                
                if(cn.getOperator().getSymbol().equals("=lt=")) {
                    return actualDouble < expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=gt=")) {
                    return actualDouble > expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=le=")) {
                    return actualDouble <= expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=ge=")) {
                    return actualDouble >= expectedDouble;
                }
            } else if(cn.getOperator().getSymbol().equals("==")) {
                return actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("!=")) {
                return !actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("=in=")) {
                return actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=out=")) {
                return !actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=regex=")) {
                String patternStr = expected.iterator().next();
                Pattern pattern = Pattern.compile(patternStr);
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        Matcher matcher = pattern.matcher(act);
                        if(matcher.find())
                            return true;
                    }
                }
                return false;
            } else if(cn.getOperator().getSymbol().equals("=lang=")) {
                String lang = expected.iterator().next();
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        if(act.endsWith("@" + lang))
                            return true;
                    }
                }
                return false;
            }

            return false;
        }
    };

    public List<ProductType13> selectProductType13() {
        return selectProductType13(null, null, null, null);
    }

    public List<ProductType13> selectProductType13(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM product_type13\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        if(limit != null) {
            querySB.append("\nLIMIT " + limit);
        }
        if(offset != null) {
            querySB.append("\nOFFSET " + offset);
        }

        String query = querySB.toString();

        List<ProductType13> result = SqlUtils.supply(connection, c -> {
            List<ProductType13> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                ProductType13 record = new ProductType13();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setLabel(SqlUtils.getString(rs, "label"));
                record.setComment(SqlUtils.getString(rs, "comment"));
                record.setProductPropertyTextual1(SqlUtils.getString(rs, "product_property_textual1"));
                record.setProductPropertyTextual2(SqlUtils.getString(rs, "product_property_textual2"));
                record.setProductPropertyTextual3(SqlUtils.getString(rs, "product_property_textual3"));
                record.setProductPropertyTextual4(SqlUtils.getString(rs, "product_property_textual4"));
                record.setProductPropertyTextual5(SqlUtils.getString(rs, "product_property_textual5"));
                record.setProductPropertyTextual6(SqlUtils.getString(rs, "product_property_textual6"));
                record.setPublisher(SqlUtils.getString(rs, "publisher"));
                record.setDate(SqlUtils.getLong(rs, "date"));
                record.setProducer(SqlUtils.getLong(rs, "producer"));
                record.setProductPropertyNumeric1(SqlUtils.getLong(rs, "product_property_numeric1"));
                record.setProductPropertyNumeric2(SqlUtils.getLong(rs, "product_property_numeric2"));
                record.setProductPropertyNumeric3(SqlUtils.getLong(rs, "product_property_numeric3"));
                record.setProductPropertyNumeric4(SqlUtils.getLong(rs, "product_property_numeric4"));
                record.setProductPropertyNumeric5(SqlUtils.getLong(rs, "product_property_numeric5"));
                record.setProductPropertyNumeric6(SqlUtils.getLong(rs, "product_property_numeric6"));

                record.setProductFeatureProductFeature(selectWithNM(record.getId(), "product_type13_id", "mn_product_type13_product_feature_product_feature", Long.class));
                record.setType(selectWithNM(record.getId(), "resource_id", "mn_resource_type_class", Long.class));
                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(ProductType13Visitor, record));
        }

        return result;
    }

    public List<ProductType13> insertProductType13(List<ProductType13> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO product_type13 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(ProductType13 record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getLabel());
                ps.setObject(3, record.getComment());
                ps.setObject(4, record.getProductPropertyTextual1());
                ps.setObject(5, record.getProductPropertyTextual2());
                ps.setObject(6, record.getProductPropertyTextual3());
                ps.setObject(7, record.getProductPropertyTextual4());
                ps.setObject(8, record.getProductPropertyTextual5());
                ps.setObject(9, record.getProductPropertyTextual6());
                ps.setObject(10, record.getPublisher());
                ps.setObject(11, record.getDate());
                ps.setObject(12, record.getProducer());
                ps.setObject(13, record.getProductPropertyNumeric1());
                ps.setObject(14, record.getProductPropertyNumeric2());
                ps.setObject(15, record.getProductPropertyNumeric3());
                ps.setObject(16, record.getProductPropertyNumeric4());
                ps.setObject(17, record.getProductPropertyNumeric5());
                ps.setObject(18, record.getProductPropertyNumeric6());
                ps.addBatch();
            }
            ps.executeBatch();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            for(int i = records.size()-1; i >= 0; i--) {
                //does only return the last generated id
                if(!generatedKeys.next())
                    break;

                long id = generatedKeys.getLong(1);
                records.get(i).setId(id);
            }
            c.commit();
            ps.close();

            boolean wasRecordsAdded = false;

            //n:m tables
            query = "INSERT INTO mn_product_type13_product_feature_product_feature VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType13 record : records) {
                if(!record.hasId() || !record.hasProductFeatureProductFeature() || record.getProductFeatureProductFeature().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getProductFeatureProductFeature()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;

            query = "INSERT INTO mn_resource_type_class VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType13 record : records) {
                if(!record.hasId() || !record.hasType() || record.getType().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getType()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;


            c.setAutoCommit(true);
        });

        return records;
    }

    public List<ProductType13> updateProductType13(List<ProductType13> records, boolean put) {    
        for(ProductType13 record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasLabel()) {
                    paramSJ.add("label = ?");
                }
                if(put || record.hasComment()) {
                    paramSJ.add("comment = ?");
                }
                if(put || record.hasProductPropertyTextual1()) {
                    paramSJ.add("product_property_textual1 = ?");
                }
                if(put || record.hasProductPropertyTextual2()) {
                    paramSJ.add("product_property_textual2 = ?");
                }
                if(put || record.hasProductPropertyTextual3()) {
                    paramSJ.add("product_property_textual3 = ?");
                }
                if(put || record.hasProductPropertyTextual4()) {
                    paramSJ.add("product_property_textual4 = ?");
                }
                if(put || record.hasProductPropertyTextual5()) {
                    paramSJ.add("product_property_textual5 = ?");
                }
                if(put || record.hasProductPropertyTextual6()) {
                    paramSJ.add("product_property_textual6 = ?");
                }
                if(put || record.hasPublisher()) {
                    paramSJ.add("publisher = ?");
                }
                if(put || record.hasDate()) {
                    paramSJ.add("date = ?");
                }
                if(put || record.hasProducer()) {
                    paramSJ.add("producer = ?");
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    paramSJ.add("product_property_numeric1 = ?");
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    paramSJ.add("product_property_numeric2 = ?");
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    paramSJ.add("product_property_numeric3 = ?");
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    paramSJ.add("product_property_numeric4 = ?");
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    paramSJ.add("product_property_numeric5 = ?");
                }
                if(put || record.hasProductPropertyNumeric6()) {
                    paramSJ.add("product_property_numeric6 = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE product_type13 SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasLabel()) {
                    ps.setObject(index, record.getLabel());
                    index++;
                }
                if(put || record.hasComment()) {
                    ps.setObject(index, record.getComment());
                    index++;
                }
                if(put || record.hasProductPropertyTextual1()) {
                    ps.setObject(index, record.getProductPropertyTextual1());
                    index++;
                }
                if(put || record.hasProductPropertyTextual2()) {
                    ps.setObject(index, record.getProductPropertyTextual2());
                    index++;
                }
                if(put || record.hasProductPropertyTextual3()) {
                    ps.setObject(index, record.getProductPropertyTextual3());
                    index++;
                }
                if(put || record.hasProductPropertyTextual4()) {
                    ps.setObject(index, record.getProductPropertyTextual4());
                    index++;
                }
                if(put || record.hasProductPropertyTextual5()) {
                    ps.setObject(index, record.getProductPropertyTextual5());
                    index++;
                }
                if(put || record.hasProductPropertyTextual6()) {
                    ps.setObject(index, record.getProductPropertyTextual6());
                    index++;
                }
                if(put || record.hasPublisher()) {
                    ps.setObject(index, record.getPublisher());
                    index++;
                }
                if(put || record.hasDate()) {
                    ps.setObject(index, record.getDate());
                    index++;
                }
                if(put || record.hasProducer()) {
                    ps.setObject(index, record.getProducer());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    ps.setObject(index, record.getProductPropertyNumeric1());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    ps.setObject(index, record.getProductPropertyNumeric2());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    ps.setObject(index, record.getProductPropertyNumeric3());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    ps.setObject(index, record.getProductPropertyNumeric4());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    ps.setObject(index, record.getProductPropertyNumeric5());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric6()) {
                    ps.setObject(index, record.getProductPropertyNumeric6());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
                if(put || (record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_product_type13_product_feature_product_feature WHERE product_type13_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_product_type13_product_feature_product_feature VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getProductFeatureProductFeature()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
                if(put || (record.hasType() && !record.getType().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_resource_type_class WHERE resource_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasType() && !record.getType().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_resource_type_class VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getType()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
            });
        }

        return records;
    }

    public void deleteProductType13() {
        deleteProductType13(null);
    }    

    public void deleteProductType13(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM product_type13");
        String in = "";
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            in = "IN (" + sj.toString() + ")";
            querySB.append(" WHERE id " + in);
        }

        final String finalIn = in;
        final String query = querySB.toString();

        SqlUtils.run(connection, c -> {
            PreparedStatement ps = c.prepareStatement(query);
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_product_type13_product_feature_product_feature" + (finalIn.isEmpty() ? "" : " WHERE product_type13_id " + finalIn));
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_resource_type_class" + (finalIn.isEmpty() ? "" : " WHERE resource_id " + finalIn));
            ps.execute();
            ps.close();

        });
    }

    //==========================================================================
    // ProductType14

    private RSQLVisitor<Boolean, ProductType14> ProductType14Visitor = new RSQLVisitor<Boolean, ProductType14>() {

        @Override
        public Boolean visit(AndNode and, ProductType14 record) {
            for(Node n : and.getChildren()) {
                if(n instanceof AndNode) {
                    if(!visit((AndNode)n, record))
                        return false;
                } else if(n instanceof OrNode) {
                    if(!visit((OrNode)n, record))
                        return false;
                } else if(n instanceof ComparisonNode) {
                    if(!visit((ComparisonNode)n, record))
                        return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visit(OrNode or, ProductType14 record) {
            for(Node n : or.getChildren()) {
                if(n instanceof AndNode) {
                    if(visit((AndNode)n, record))
                        return true;
                } else if(n instanceof OrNode) {
                    if(visit((OrNode)n, record))
                        return true;
                } else if(n instanceof ComparisonNode) {
                    if(visit((ComparisonNode)n, record))
                        return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(ComparisonNode cn, ProductType14 record) {

            Set<String> actual = record.getStringSet(cn.getSelector());
            if(actual == null) {
                return false;
            }
            Set<String> expected = new HashSet<>(cn.getArguments());

            if(cn.getOperator().getSymbol().equals("=lt=") || 
               cn.getOperator().getSymbol().equals("=gt=") ||
               cn.getOperator().getSymbol().equals("=le=") ||
               cn.getOperator().getSymbol().equals("=ge=")) {
            
                double actualDouble = Double.parseDouble(actual.iterator().next());
                double expectedDouble = Double.parseDouble(expected.iterator().next());
                
                if(cn.getOperator().getSymbol().equals("=lt=")) {
                    return actualDouble < expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=gt=")) {
                    return actualDouble > expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=le=")) {
                    return actualDouble <= expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=ge=")) {
                    return actualDouble >= expectedDouble;
                }
            } else if(cn.getOperator().getSymbol().equals("==")) {
                return actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("!=")) {
                return !actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("=in=")) {
                return actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=out=")) {
                return !actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=regex=")) {
                String patternStr = expected.iterator().next();
                Pattern pattern = Pattern.compile(patternStr);
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        Matcher matcher = pattern.matcher(act);
                        if(matcher.find())
                            return true;
                    }
                }
                return false;
            } else if(cn.getOperator().getSymbol().equals("=lang=")) {
                String lang = expected.iterator().next();
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        if(act.endsWith("@" + lang))
                            return true;
                    }
                }
                return false;
            }

            return false;
        }
    };

    public List<ProductType14> selectProductType14() {
        return selectProductType14(null, null, null, null);
    }

    public List<ProductType14> selectProductType14(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM product_type14\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        if(limit != null) {
            querySB.append("\nLIMIT " + limit);
        }
        if(offset != null) {
            querySB.append("\nOFFSET " + offset);
        }

        String query = querySB.toString();

        List<ProductType14> result = SqlUtils.supply(connection, c -> {
            List<ProductType14> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                ProductType14 record = new ProductType14();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setLabel(SqlUtils.getString(rs, "label"));
                record.setComment(SqlUtils.getString(rs, "comment"));
                record.setProductPropertyTextual1(SqlUtils.getString(rs, "product_property_textual1"));
                record.setProductPropertyTextual2(SqlUtils.getString(rs, "product_property_textual2"));
                record.setProductPropertyTextual3(SqlUtils.getString(rs, "product_property_textual3"));
                record.setProductPropertyTextual4(SqlUtils.getString(rs, "product_property_textual4"));
                record.setProductPropertyTextual5(SqlUtils.getString(rs, "product_property_textual5"));
                record.setPublisher(SqlUtils.getString(rs, "publisher"));
                record.setDate(SqlUtils.getLong(rs, "date"));
                record.setProducer(SqlUtils.getLong(rs, "producer"));
                record.setProductPropertyNumeric1(SqlUtils.getLong(rs, "product_property_numeric1"));
                record.setProductPropertyNumeric2(SqlUtils.getLong(rs, "product_property_numeric2"));
                record.setProductPropertyNumeric3(SqlUtils.getLong(rs, "product_property_numeric3"));
                record.setProductPropertyNumeric4(SqlUtils.getLong(rs, "product_property_numeric4"));
                record.setProductPropertyNumeric5(SqlUtils.getLong(rs, "product_property_numeric5"));

                record.setProductFeatureProductFeature(selectWithNM(record.getId(), "product_type14_id", "mn_product_type14_product_feature_product_feature", Long.class));
                record.setType(selectWithNM(record.getId(), "resource_id", "mn_resource_type_class", Long.class));
                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(ProductType14Visitor, record));
        }

        return result;
    }

    public List<ProductType14> insertProductType14(List<ProductType14> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO product_type14 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(ProductType14 record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getLabel());
                ps.setObject(3, record.getComment());
                ps.setObject(4, record.getProductPropertyTextual1());
                ps.setObject(5, record.getProductPropertyTextual2());
                ps.setObject(6, record.getProductPropertyTextual3());
                ps.setObject(7, record.getProductPropertyTextual4());
                ps.setObject(8, record.getProductPropertyTextual5());
                ps.setObject(9, record.getPublisher());
                ps.setObject(10, record.getDate());
                ps.setObject(11, record.getProducer());
                ps.setObject(12, record.getProductPropertyNumeric1());
                ps.setObject(13, record.getProductPropertyNumeric2());
                ps.setObject(14, record.getProductPropertyNumeric3());
                ps.setObject(15, record.getProductPropertyNumeric4());
                ps.setObject(16, record.getProductPropertyNumeric5());
                ps.addBatch();
            }
            ps.executeBatch();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            for(int i = records.size()-1; i >= 0; i--) {
                //does only return the last generated id
                if(!generatedKeys.next())
                    break;

                long id = generatedKeys.getLong(1);
                records.get(i).setId(id);
            }
            c.commit();
            ps.close();

            boolean wasRecordsAdded = false;

            //n:m tables
            query = "INSERT INTO mn_product_type14_product_feature_product_feature VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType14 record : records) {
                if(!record.hasId() || !record.hasProductFeatureProductFeature() || record.getProductFeatureProductFeature().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getProductFeatureProductFeature()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;

            query = "INSERT INTO mn_resource_type_class VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType14 record : records) {
                if(!record.hasId() || !record.hasType() || record.getType().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getType()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;


            c.setAutoCommit(true);
        });

        return records;
    }

    public List<ProductType14> updateProductType14(List<ProductType14> records, boolean put) {    
        for(ProductType14 record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasLabel()) {
                    paramSJ.add("label = ?");
                }
                if(put || record.hasComment()) {
                    paramSJ.add("comment = ?");
                }
                if(put || record.hasProductPropertyTextual1()) {
                    paramSJ.add("product_property_textual1 = ?");
                }
                if(put || record.hasProductPropertyTextual2()) {
                    paramSJ.add("product_property_textual2 = ?");
                }
                if(put || record.hasProductPropertyTextual3()) {
                    paramSJ.add("product_property_textual3 = ?");
                }
                if(put || record.hasProductPropertyTextual4()) {
                    paramSJ.add("product_property_textual4 = ?");
                }
                if(put || record.hasProductPropertyTextual5()) {
                    paramSJ.add("product_property_textual5 = ?");
                }
                if(put || record.hasPublisher()) {
                    paramSJ.add("publisher = ?");
                }
                if(put || record.hasDate()) {
                    paramSJ.add("date = ?");
                }
                if(put || record.hasProducer()) {
                    paramSJ.add("producer = ?");
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    paramSJ.add("product_property_numeric1 = ?");
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    paramSJ.add("product_property_numeric2 = ?");
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    paramSJ.add("product_property_numeric3 = ?");
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    paramSJ.add("product_property_numeric4 = ?");
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    paramSJ.add("product_property_numeric5 = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE product_type14 SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasLabel()) {
                    ps.setObject(index, record.getLabel());
                    index++;
                }
                if(put || record.hasComment()) {
                    ps.setObject(index, record.getComment());
                    index++;
                }
                if(put || record.hasProductPropertyTextual1()) {
                    ps.setObject(index, record.getProductPropertyTextual1());
                    index++;
                }
                if(put || record.hasProductPropertyTextual2()) {
                    ps.setObject(index, record.getProductPropertyTextual2());
                    index++;
                }
                if(put || record.hasProductPropertyTextual3()) {
                    ps.setObject(index, record.getProductPropertyTextual3());
                    index++;
                }
                if(put || record.hasProductPropertyTextual4()) {
                    ps.setObject(index, record.getProductPropertyTextual4());
                    index++;
                }
                if(put || record.hasProductPropertyTextual5()) {
                    ps.setObject(index, record.getProductPropertyTextual5());
                    index++;
                }
                if(put || record.hasPublisher()) {
                    ps.setObject(index, record.getPublisher());
                    index++;
                }
                if(put || record.hasDate()) {
                    ps.setObject(index, record.getDate());
                    index++;
                }
                if(put || record.hasProducer()) {
                    ps.setObject(index, record.getProducer());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    ps.setObject(index, record.getProductPropertyNumeric1());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    ps.setObject(index, record.getProductPropertyNumeric2());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    ps.setObject(index, record.getProductPropertyNumeric3());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    ps.setObject(index, record.getProductPropertyNumeric4());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    ps.setObject(index, record.getProductPropertyNumeric5());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
                if(put || (record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_product_type14_product_feature_product_feature WHERE product_type14_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_product_type14_product_feature_product_feature VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getProductFeatureProductFeature()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
                if(put || (record.hasType() && !record.getType().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_resource_type_class WHERE resource_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasType() && !record.getType().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_resource_type_class VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getType()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
            });
        }

        return records;
    }

    public void deleteProductType14() {
        deleteProductType14(null);
    }    

    public void deleteProductType14(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM product_type14");
        String in = "";
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            in = "IN (" + sj.toString() + ")";
            querySB.append(" WHERE id " + in);
        }

        final String finalIn = in;
        final String query = querySB.toString();

        SqlUtils.run(connection, c -> {
            PreparedStatement ps = c.prepareStatement(query);
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_product_type14_product_feature_product_feature" + (finalIn.isEmpty() ? "" : " WHERE product_type14_id " + finalIn));
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_resource_type_class" + (finalIn.isEmpty() ? "" : " WHERE resource_id " + finalIn));
            ps.execute();
            ps.close();

        });
    }

    //==========================================================================
    // ProductType11

    private RSQLVisitor<Boolean, ProductType11> ProductType11Visitor = new RSQLVisitor<Boolean, ProductType11>() {

        @Override
        public Boolean visit(AndNode and, ProductType11 record) {
            for(Node n : and.getChildren()) {
                if(n instanceof AndNode) {
                    if(!visit((AndNode)n, record))
                        return false;
                } else if(n instanceof OrNode) {
                    if(!visit((OrNode)n, record))
                        return false;
                } else if(n instanceof ComparisonNode) {
                    if(!visit((ComparisonNode)n, record))
                        return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visit(OrNode or, ProductType11 record) {
            for(Node n : or.getChildren()) {
                if(n instanceof AndNode) {
                    if(visit((AndNode)n, record))
                        return true;
                } else if(n instanceof OrNode) {
                    if(visit((OrNode)n, record))
                        return true;
                } else if(n instanceof ComparisonNode) {
                    if(visit((ComparisonNode)n, record))
                        return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(ComparisonNode cn, ProductType11 record) {

            Set<String> actual = record.getStringSet(cn.getSelector());
            if(actual == null) {
                return false;
            }
            Set<String> expected = new HashSet<>(cn.getArguments());

            if(cn.getOperator().getSymbol().equals("=lt=") || 
               cn.getOperator().getSymbol().equals("=gt=") ||
               cn.getOperator().getSymbol().equals("=le=") ||
               cn.getOperator().getSymbol().equals("=ge=")) {
            
                double actualDouble = Double.parseDouble(actual.iterator().next());
                double expectedDouble = Double.parseDouble(expected.iterator().next());
                
                if(cn.getOperator().getSymbol().equals("=lt=")) {
                    return actualDouble < expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=gt=")) {
                    return actualDouble > expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=le=")) {
                    return actualDouble <= expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=ge=")) {
                    return actualDouble >= expectedDouble;
                }
            } else if(cn.getOperator().getSymbol().equals("==")) {
                return actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("!=")) {
                return !actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("=in=")) {
                return actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=out=")) {
                return !actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=regex=")) {
                String patternStr = expected.iterator().next();
                Pattern pattern = Pattern.compile(patternStr);
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        Matcher matcher = pattern.matcher(act);
                        if(matcher.find())
                            return true;
                    }
                }
                return false;
            } else if(cn.getOperator().getSymbol().equals("=lang=")) {
                String lang = expected.iterator().next();
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        if(act.endsWith("@" + lang))
                            return true;
                    }
                }
                return false;
            }

            return false;
        }
    };

    public List<ProductType11> selectProductType11() {
        return selectProductType11(null, null, null, null);
    }

    public List<ProductType11> selectProductType11(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM product_type11\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        if(limit != null) {
            querySB.append("\nLIMIT " + limit);
        }
        if(offset != null) {
            querySB.append("\nOFFSET " + offset);
        }

        String query = querySB.toString();

        List<ProductType11> result = SqlUtils.supply(connection, c -> {
            List<ProductType11> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                ProductType11 record = new ProductType11();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setLabel(SqlUtils.getString(rs, "label"));
                record.setComment(SqlUtils.getString(rs, "comment"));
                record.setProductPropertyTextual1(SqlUtils.getString(rs, "product_property_textual1"));
                record.setProductPropertyTextual2(SqlUtils.getString(rs, "product_property_textual2"));
                record.setProductPropertyTextual3(SqlUtils.getString(rs, "product_property_textual3"));
                record.setProductPropertyTextual4(SqlUtils.getString(rs, "product_property_textual4"));
                record.setProductPropertyTextual5(SqlUtils.getString(rs, "product_property_textual5"));
                record.setProductPropertyTextual6(SqlUtils.getString(rs, "product_property_textual6"));
                record.setPublisher(SqlUtils.getString(rs, "publisher"));
                record.setDate(SqlUtils.getLong(rs, "date"));
                record.setProducer(SqlUtils.getLong(rs, "producer"));
                record.setProductPropertyNumeric1(SqlUtils.getLong(rs, "product_property_numeric1"));
                record.setProductPropertyNumeric2(SqlUtils.getLong(rs, "product_property_numeric2"));
                record.setProductPropertyNumeric3(SqlUtils.getLong(rs, "product_property_numeric3"));
                record.setProductPropertyNumeric4(SqlUtils.getLong(rs, "product_property_numeric4"));
                record.setProductPropertyNumeric5(SqlUtils.getLong(rs, "product_property_numeric5"));
                record.setProductPropertyNumeric6(SqlUtils.getLong(rs, "product_property_numeric6"));

                record.setProductFeatureProductFeature(selectWithNM(record.getId(), "product_type11_id", "mn_product_type11_product_feature_product_feature", Long.class));
                record.setType(selectWithNM(record.getId(), "resource_id", "mn_resource_type_class", Long.class));
                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(ProductType11Visitor, record));
        }

        return result;
    }

    public List<ProductType11> insertProductType11(List<ProductType11> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO product_type11 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(ProductType11 record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getLabel());
                ps.setObject(3, record.getComment());
                ps.setObject(4, record.getProductPropertyTextual1());
                ps.setObject(5, record.getProductPropertyTextual2());
                ps.setObject(6, record.getProductPropertyTextual3());
                ps.setObject(7, record.getProductPropertyTextual4());
                ps.setObject(8, record.getProductPropertyTextual5());
                ps.setObject(9, record.getProductPropertyTextual6());
                ps.setObject(10, record.getPublisher());
                ps.setObject(11, record.getDate());
                ps.setObject(12, record.getProducer());
                ps.setObject(13, record.getProductPropertyNumeric1());
                ps.setObject(14, record.getProductPropertyNumeric2());
                ps.setObject(15, record.getProductPropertyNumeric3());
                ps.setObject(16, record.getProductPropertyNumeric4());
                ps.setObject(17, record.getProductPropertyNumeric5());
                ps.setObject(18, record.getProductPropertyNumeric6());
                ps.addBatch();
            }
            ps.executeBatch();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            for(int i = records.size()-1; i >= 0; i--) {
                //does only return the last generated id
                if(!generatedKeys.next())
                    break;

                long id = generatedKeys.getLong(1);
                records.get(i).setId(id);
            }
            c.commit();
            ps.close();

            boolean wasRecordsAdded = false;

            //n:m tables
            query = "INSERT INTO mn_product_type11_product_feature_product_feature VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType11 record : records) {
                if(!record.hasId() || !record.hasProductFeatureProductFeature() || record.getProductFeatureProductFeature().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getProductFeatureProductFeature()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;

            query = "INSERT INTO mn_resource_type_class VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType11 record : records) {
                if(!record.hasId() || !record.hasType() || record.getType().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getType()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;


            c.setAutoCommit(true);
        });

        return records;
    }

    public List<ProductType11> updateProductType11(List<ProductType11> records, boolean put) {    
        for(ProductType11 record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasLabel()) {
                    paramSJ.add("label = ?");
                }
                if(put || record.hasComment()) {
                    paramSJ.add("comment = ?");
                }
                if(put || record.hasProductPropertyTextual1()) {
                    paramSJ.add("product_property_textual1 = ?");
                }
                if(put || record.hasProductPropertyTextual2()) {
                    paramSJ.add("product_property_textual2 = ?");
                }
                if(put || record.hasProductPropertyTextual3()) {
                    paramSJ.add("product_property_textual3 = ?");
                }
                if(put || record.hasProductPropertyTextual4()) {
                    paramSJ.add("product_property_textual4 = ?");
                }
                if(put || record.hasProductPropertyTextual5()) {
                    paramSJ.add("product_property_textual5 = ?");
                }
                if(put || record.hasProductPropertyTextual6()) {
                    paramSJ.add("product_property_textual6 = ?");
                }
                if(put || record.hasPublisher()) {
                    paramSJ.add("publisher = ?");
                }
                if(put || record.hasDate()) {
                    paramSJ.add("date = ?");
                }
                if(put || record.hasProducer()) {
                    paramSJ.add("producer = ?");
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    paramSJ.add("product_property_numeric1 = ?");
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    paramSJ.add("product_property_numeric2 = ?");
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    paramSJ.add("product_property_numeric3 = ?");
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    paramSJ.add("product_property_numeric4 = ?");
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    paramSJ.add("product_property_numeric5 = ?");
                }
                if(put || record.hasProductPropertyNumeric6()) {
                    paramSJ.add("product_property_numeric6 = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE product_type11 SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasLabel()) {
                    ps.setObject(index, record.getLabel());
                    index++;
                }
                if(put || record.hasComment()) {
                    ps.setObject(index, record.getComment());
                    index++;
                }
                if(put || record.hasProductPropertyTextual1()) {
                    ps.setObject(index, record.getProductPropertyTextual1());
                    index++;
                }
                if(put || record.hasProductPropertyTextual2()) {
                    ps.setObject(index, record.getProductPropertyTextual2());
                    index++;
                }
                if(put || record.hasProductPropertyTextual3()) {
                    ps.setObject(index, record.getProductPropertyTextual3());
                    index++;
                }
                if(put || record.hasProductPropertyTextual4()) {
                    ps.setObject(index, record.getProductPropertyTextual4());
                    index++;
                }
                if(put || record.hasProductPropertyTextual5()) {
                    ps.setObject(index, record.getProductPropertyTextual5());
                    index++;
                }
                if(put || record.hasProductPropertyTextual6()) {
                    ps.setObject(index, record.getProductPropertyTextual6());
                    index++;
                }
                if(put || record.hasPublisher()) {
                    ps.setObject(index, record.getPublisher());
                    index++;
                }
                if(put || record.hasDate()) {
                    ps.setObject(index, record.getDate());
                    index++;
                }
                if(put || record.hasProducer()) {
                    ps.setObject(index, record.getProducer());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    ps.setObject(index, record.getProductPropertyNumeric1());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    ps.setObject(index, record.getProductPropertyNumeric2());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    ps.setObject(index, record.getProductPropertyNumeric3());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    ps.setObject(index, record.getProductPropertyNumeric4());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    ps.setObject(index, record.getProductPropertyNumeric5());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric6()) {
                    ps.setObject(index, record.getProductPropertyNumeric6());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
                if(put || (record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_product_type11_product_feature_product_feature WHERE product_type11_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_product_type11_product_feature_product_feature VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getProductFeatureProductFeature()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
                if(put || (record.hasType() && !record.getType().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_resource_type_class WHERE resource_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasType() && !record.getType().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_resource_type_class VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getType()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
            });
        }

        return records;
    }

    public void deleteProductType11() {
        deleteProductType11(null);
    }    

    public void deleteProductType11(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM product_type11");
        String in = "";
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            in = "IN (" + sj.toString() + ")";
            querySB.append(" WHERE id " + in);
        }

        final String finalIn = in;
        final String query = querySB.toString();

        SqlUtils.run(connection, c -> {
            PreparedStatement ps = c.prepareStatement(query);
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_product_type11_product_feature_product_feature" + (finalIn.isEmpty() ? "" : " WHERE product_type11_id " + finalIn));
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_resource_type_class" + (finalIn.isEmpty() ? "" : " WHERE resource_id " + finalIn));
            ps.execute();
            ps.close();

        });
    }

    //==========================================================================
    // Offer

    private RSQLVisitor<Boolean, Offer> OfferVisitor = new RSQLVisitor<Boolean, Offer>() {

        @Override
        public Boolean visit(AndNode and, Offer record) {
            for(Node n : and.getChildren()) {
                if(n instanceof AndNode) {
                    if(!visit((AndNode)n, record))
                        return false;
                } else if(n instanceof OrNode) {
                    if(!visit((OrNode)n, record))
                        return false;
                } else if(n instanceof ComparisonNode) {
                    if(!visit((ComparisonNode)n, record))
                        return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visit(OrNode or, Offer record) {
            for(Node n : or.getChildren()) {
                if(n instanceof AndNode) {
                    if(visit((AndNode)n, record))
                        return true;
                } else if(n instanceof OrNode) {
                    if(visit((OrNode)n, record))
                        return true;
                } else if(n instanceof ComparisonNode) {
                    if(visit((ComparisonNode)n, record))
                        return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(ComparisonNode cn, Offer record) {

            Set<String> actual = record.getStringSet(cn.getSelector());
            if(actual == null) {
                return false;
            }
            Set<String> expected = new HashSet<>(cn.getArguments());

            if(cn.getOperator().getSymbol().equals("=lt=") || 
               cn.getOperator().getSymbol().equals("=gt=") ||
               cn.getOperator().getSymbol().equals("=le=") ||
               cn.getOperator().getSymbol().equals("=ge=")) {
            
                double actualDouble = Double.parseDouble(actual.iterator().next());
                double expectedDouble = Double.parseDouble(expected.iterator().next());
                
                if(cn.getOperator().getSymbol().equals("=lt=")) {
                    return actualDouble < expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=gt=")) {
                    return actualDouble > expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=le=")) {
                    return actualDouble <= expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=ge=")) {
                    return actualDouble >= expectedDouble;
                }
            } else if(cn.getOperator().getSymbol().equals("==")) {
                return actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("!=")) {
                return !actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("=in=")) {
                return actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=out=")) {
                return !actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=regex=")) {
                String patternStr = expected.iterator().next();
                Pattern pattern = Pattern.compile(patternStr);
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        Matcher matcher = pattern.matcher(act);
                        if(matcher.find())
                            return true;
                    }
                }
                return false;
            } else if(cn.getOperator().getSymbol().equals("=lang=")) {
                String lang = expected.iterator().next();
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        if(act.endsWith("@" + lang))
                            return true;
                    }
                }
                return false;
            }

            return false;
        }
    };

    public List<Offer> selectOffer() {
        return selectOffer(null, null, null, null);
    }

    public List<Offer> selectOffer(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM offer\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        if(limit != null) {
            querySB.append("\nLIMIT " + limit);
        }
        if(offset != null) {
            querySB.append("\nOFFSET " + offset);
        }

        String query = querySB.toString();

        List<Offer> result = SqlUtils.supply(connection, c -> {
            List<Offer> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                Offer record = new Offer();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setOfferWebpage(SqlUtils.getString(rs, "offer_webpage"));
                record.setPrice(SqlUtils.getString(rs, "price"));
                record.setPublisher(SqlUtils.getString(rs, "publisher"));
                record.setDate(SqlUtils.getLong(rs, "date"));
                record.setDeliveryDays(SqlUtils.getLong(rs, "delivery_days"));
                record.setProduct(SqlUtils.getLong(rs, "product"));
                record.setValidFrom(SqlUtils.getLong(rs, "valid_from"));
                record.setValidTo(SqlUtils.getLong(rs, "valid_to"));
                record.setVendor(SqlUtils.getLong(rs, "vendor"));

                record.setType(selectWithNM(record.getId(), "resource_id", "mn_resource_type_class", Long.class));
                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(OfferVisitor, record));
        }

        return result;
    }

    public List<Offer> insertOffer(List<Offer> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO offer VALUES (?,?,?,?,?,?,?,?,?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(Offer record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getOfferWebpage());
                ps.setObject(3, record.getPrice());
                ps.setObject(4, record.getPublisher());
                ps.setObject(5, record.getDate());
                ps.setObject(6, record.getDeliveryDays());
                ps.setObject(7, record.getProduct());
                ps.setObject(8, record.getValidFrom());
                ps.setObject(9, record.getValidTo());
                ps.setObject(10, record.getVendor());
                ps.addBatch();
            }
            ps.executeBatch();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            for(int i = records.size()-1; i >= 0; i--) {
                //does only return the last generated id
                if(!generatedKeys.next())
                    break;

                long id = generatedKeys.getLong(1);
                records.get(i).setId(id);
            }
            c.commit();
            ps.close();

            boolean wasRecordsAdded = false;

            //n:m tables
            query = "INSERT INTO mn_resource_type_class VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(Offer record : records) {
                if(!record.hasId() || !record.hasType() || record.getType().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getType()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;


            c.setAutoCommit(true);
        });

        return records;
    }

    public List<Offer> updateOffer(List<Offer> records, boolean put) {    
        for(Offer record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasOfferWebpage()) {
                    paramSJ.add("offer_webpage = ?");
                }
                if(put || record.hasPrice()) {
                    paramSJ.add("price = ?");
                }
                if(put || record.hasPublisher()) {
                    paramSJ.add("publisher = ?");
                }
                if(put || record.hasDate()) {
                    paramSJ.add("date = ?");
                }
                if(put || record.hasDeliveryDays()) {
                    paramSJ.add("delivery_days = ?");
                }
                if(put || record.hasProduct()) {
                    paramSJ.add("product = ?");
                }
                if(put || record.hasValidFrom()) {
                    paramSJ.add("valid_from = ?");
                }
                if(put || record.hasValidTo()) {
                    paramSJ.add("valid_to = ?");
                }
                if(put || record.hasVendor()) {
                    paramSJ.add("vendor = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE offer SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasOfferWebpage()) {
                    ps.setObject(index, record.getOfferWebpage());
                    index++;
                }
                if(put || record.hasPrice()) {
                    ps.setObject(index, record.getPrice());
                    index++;
                }
                if(put || record.hasPublisher()) {
                    ps.setObject(index, record.getPublisher());
                    index++;
                }
                if(put || record.hasDate()) {
                    ps.setObject(index, record.getDate());
                    index++;
                }
                if(put || record.hasDeliveryDays()) {
                    ps.setObject(index, record.getDeliveryDays());
                    index++;
                }
                if(put || record.hasProduct()) {
                    ps.setObject(index, record.getProduct());
                    index++;
                }
                if(put || record.hasValidFrom()) {
                    ps.setObject(index, record.getValidFrom());
                    index++;
                }
                if(put || record.hasValidTo()) {
                    ps.setObject(index, record.getValidTo());
                    index++;
                }
                if(put || record.hasVendor()) {
                    ps.setObject(index, record.getVendor());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
                if(put || (record.hasType() && !record.getType().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_resource_type_class WHERE resource_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasType() && !record.getType().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_resource_type_class VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getType()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
            });
        }

        return records;
    }

    public void deleteOffer() {
        deleteOffer(null);
    }    

    public void deleteOffer(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM offer");
        String in = "";
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            in = "IN (" + sj.toString() + ")";
            querySB.append(" WHERE id " + in);
        }

        final String finalIn = in;
        final String query = querySB.toString();

        SqlUtils.run(connection, c -> {
            PreparedStatement ps = c.prepareStatement(query);
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_resource_type_class" + (finalIn.isEmpty() ? "" : " WHERE resource_id " + finalIn));
            ps.execute();
            ps.close();

        });
    }

    //==========================================================================
    // ProductType12

    private RSQLVisitor<Boolean, ProductType12> ProductType12Visitor = new RSQLVisitor<Boolean, ProductType12>() {

        @Override
        public Boolean visit(AndNode and, ProductType12 record) {
            for(Node n : and.getChildren()) {
                if(n instanceof AndNode) {
                    if(!visit((AndNode)n, record))
                        return false;
                } else if(n instanceof OrNode) {
                    if(!visit((OrNode)n, record))
                        return false;
                } else if(n instanceof ComparisonNode) {
                    if(!visit((ComparisonNode)n, record))
                        return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visit(OrNode or, ProductType12 record) {
            for(Node n : or.getChildren()) {
                if(n instanceof AndNode) {
                    if(visit((AndNode)n, record))
                        return true;
                } else if(n instanceof OrNode) {
                    if(visit((OrNode)n, record))
                        return true;
                } else if(n instanceof ComparisonNode) {
                    if(visit((ComparisonNode)n, record))
                        return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(ComparisonNode cn, ProductType12 record) {

            Set<String> actual = record.getStringSet(cn.getSelector());
            if(actual == null) {
                return false;
            }
            Set<String> expected = new HashSet<>(cn.getArguments());

            if(cn.getOperator().getSymbol().equals("=lt=") || 
               cn.getOperator().getSymbol().equals("=gt=") ||
               cn.getOperator().getSymbol().equals("=le=") ||
               cn.getOperator().getSymbol().equals("=ge=")) {
            
                double actualDouble = Double.parseDouble(actual.iterator().next());
                double expectedDouble = Double.parseDouble(expected.iterator().next());
                
                if(cn.getOperator().getSymbol().equals("=lt=")) {
                    return actualDouble < expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=gt=")) {
                    return actualDouble > expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=le=")) {
                    return actualDouble <= expectedDouble;
                } else if(cn.getOperator().getSymbol().equals("=ge=")) {
                    return actualDouble >= expectedDouble;
                }
            } else if(cn.getOperator().getSymbol().equals("==")) {
                return actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("!=")) {
                return !actual.equals(expected);
            } else if(cn.getOperator().getSymbol().equals("=in=")) {
                return actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=out=")) {
                return !actual.containsAll(expected);
            } else if(cn.getOperator().getSymbol().equals("=regex=")) {
                String patternStr = expected.iterator().next();
                Pattern pattern = Pattern.compile(patternStr);
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        Matcher matcher = pattern.matcher(act);
                        if(matcher.find())
                            return true;
                    }
                }
                return false;
            } else if(cn.getOperator().getSymbol().equals("=lang=")) {
                String lang = expected.iterator().next();
                if(!actual.isEmpty()) {
                    for(String act : actual) {
                        if(act.endsWith("@" + lang))
                            return true;
                    }
                }
                return false;
            }

            return false;
        }
    };

    public List<ProductType12> selectProductType12() {
        return selectProductType12(null, null, null, null);
    }

    public List<ProductType12> selectProductType12(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM product_type12\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        if(limit != null) {
            querySB.append("\nLIMIT " + limit);
        }
        if(offset != null) {
            querySB.append("\nOFFSET " + offset);
        }

        String query = querySB.toString();

        List<ProductType12> result = SqlUtils.supply(connection, c -> {
            List<ProductType12> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                ProductType12 record = new ProductType12();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setLabel(SqlUtils.getString(rs, "label"));
                record.setComment(SqlUtils.getString(rs, "comment"));
                record.setProductPropertyTextual1(SqlUtils.getString(rs, "product_property_textual1"));
                record.setProductPropertyTextual2(SqlUtils.getString(rs, "product_property_textual2"));
                record.setProductPropertyTextual3(SqlUtils.getString(rs, "product_property_textual3"));
                record.setProductPropertyTextual4(SqlUtils.getString(rs, "product_property_textual4"));
                record.setProductPropertyTextual5(SqlUtils.getString(rs, "product_property_textual5"));
                record.setPublisher(SqlUtils.getString(rs, "publisher"));
                record.setDate(SqlUtils.getLong(rs, "date"));
                record.setProducer(SqlUtils.getLong(rs, "producer"));
                record.setProductPropertyNumeric1(SqlUtils.getLong(rs, "product_property_numeric1"));
                record.setProductPropertyNumeric2(SqlUtils.getLong(rs, "product_property_numeric2"));
                record.setProductPropertyNumeric3(SqlUtils.getLong(rs, "product_property_numeric3"));
                record.setProductPropertyNumeric4(SqlUtils.getLong(rs, "product_property_numeric4"));
                record.setProductPropertyNumeric5(SqlUtils.getLong(rs, "product_property_numeric5"));

                record.setProductFeatureProductFeature(selectWithNM(record.getId(), "product_type12_id", "mn_product_type12_product_feature_product_feature", Long.class));
                record.setType(selectWithNM(record.getId(), "resource_id", "mn_resource_type_class", Long.class));
                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(ProductType12Visitor, record));
        }

        return result;
    }

    public List<ProductType12> insertProductType12(List<ProductType12> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO product_type12 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(ProductType12 record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getLabel());
                ps.setObject(3, record.getComment());
                ps.setObject(4, record.getProductPropertyTextual1());
                ps.setObject(5, record.getProductPropertyTextual2());
                ps.setObject(6, record.getProductPropertyTextual3());
                ps.setObject(7, record.getProductPropertyTextual4());
                ps.setObject(8, record.getProductPropertyTextual5());
                ps.setObject(9, record.getPublisher());
                ps.setObject(10, record.getDate());
                ps.setObject(11, record.getProducer());
                ps.setObject(12, record.getProductPropertyNumeric1());
                ps.setObject(13, record.getProductPropertyNumeric2());
                ps.setObject(14, record.getProductPropertyNumeric3());
                ps.setObject(15, record.getProductPropertyNumeric4());
                ps.setObject(16, record.getProductPropertyNumeric5());
                ps.addBatch();
            }
            ps.executeBatch();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            for(int i = records.size()-1; i >= 0; i--) {
                //does only return the last generated id
                if(!generatedKeys.next())
                    break;

                long id = generatedKeys.getLong(1);
                records.get(i).setId(id);
            }
            c.commit();
            ps.close();

            boolean wasRecordsAdded = false;

            //n:m tables
            query = "INSERT INTO mn_product_type12_product_feature_product_feature VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType12 record : records) {
                if(!record.hasId() || !record.hasProductFeatureProductFeature() || record.getProductFeatureProductFeature().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getProductFeatureProductFeature()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;

            query = "INSERT INTO mn_resource_type_class VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(ProductType12 record : records) {
                if(!record.hasId() || !record.hasType() || record.getType().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getType()) {
                    if(trg instanceof LangString) {
                        LangString langStr = (LangString) trg;
                        ps.setObject(2, langStr.getString());
                        ps.setObject(3, langStr.getLang());
                    } else {
                        ps.setObject(2, trg);
                    }
                    ps.addBatch();
                    wasRecordsAdded = true;
                }
            }
            if(wasRecordsAdded) {
                ps.executeBatch();
                c.commit();
            }
            ps.close();

            wasRecordsAdded = false;


            c.setAutoCommit(true);
        });

        return records;
    }

    public List<ProductType12> updateProductType12(List<ProductType12> records, boolean put) {    
        for(ProductType12 record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasLabel()) {
                    paramSJ.add("label = ?");
                }
                if(put || record.hasComment()) {
                    paramSJ.add("comment = ?");
                }
                if(put || record.hasProductPropertyTextual1()) {
                    paramSJ.add("product_property_textual1 = ?");
                }
                if(put || record.hasProductPropertyTextual2()) {
                    paramSJ.add("product_property_textual2 = ?");
                }
                if(put || record.hasProductPropertyTextual3()) {
                    paramSJ.add("product_property_textual3 = ?");
                }
                if(put || record.hasProductPropertyTextual4()) {
                    paramSJ.add("product_property_textual4 = ?");
                }
                if(put || record.hasProductPropertyTextual5()) {
                    paramSJ.add("product_property_textual5 = ?");
                }
                if(put || record.hasPublisher()) {
                    paramSJ.add("publisher = ?");
                }
                if(put || record.hasDate()) {
                    paramSJ.add("date = ?");
                }
                if(put || record.hasProducer()) {
                    paramSJ.add("producer = ?");
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    paramSJ.add("product_property_numeric1 = ?");
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    paramSJ.add("product_property_numeric2 = ?");
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    paramSJ.add("product_property_numeric3 = ?");
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    paramSJ.add("product_property_numeric4 = ?");
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    paramSJ.add("product_property_numeric5 = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE product_type12 SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasLabel()) {
                    ps.setObject(index, record.getLabel());
                    index++;
                }
                if(put || record.hasComment()) {
                    ps.setObject(index, record.getComment());
                    index++;
                }
                if(put || record.hasProductPropertyTextual1()) {
                    ps.setObject(index, record.getProductPropertyTextual1());
                    index++;
                }
                if(put || record.hasProductPropertyTextual2()) {
                    ps.setObject(index, record.getProductPropertyTextual2());
                    index++;
                }
                if(put || record.hasProductPropertyTextual3()) {
                    ps.setObject(index, record.getProductPropertyTextual3());
                    index++;
                }
                if(put || record.hasProductPropertyTextual4()) {
                    ps.setObject(index, record.getProductPropertyTextual4());
                    index++;
                }
                if(put || record.hasProductPropertyTextual5()) {
                    ps.setObject(index, record.getProductPropertyTextual5());
                    index++;
                }
                if(put || record.hasPublisher()) {
                    ps.setObject(index, record.getPublisher());
                    index++;
                }
                if(put || record.hasDate()) {
                    ps.setObject(index, record.getDate());
                    index++;
                }
                if(put || record.hasProducer()) {
                    ps.setObject(index, record.getProducer());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric1()) {
                    ps.setObject(index, record.getProductPropertyNumeric1());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric2()) {
                    ps.setObject(index, record.getProductPropertyNumeric2());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric3()) {
                    ps.setObject(index, record.getProductPropertyNumeric3());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric4()) {
                    ps.setObject(index, record.getProductPropertyNumeric4());
                    index++;
                }
                if(put || record.hasProductPropertyNumeric5()) {
                    ps.setObject(index, record.getProductPropertyNumeric5());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
                if(put || (record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_product_type12_product_feature_product_feature WHERE product_type12_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasProductFeatureProductFeature() && !record.getProductFeatureProductFeature().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_product_type12_product_feature_product_feature VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getProductFeatureProductFeature()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
                if(put || (record.hasType() && !record.getType().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_resource_type_class WHERE resource_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasType() && !record.getType().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_resource_type_class VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getType()) {
                            if(trg instanceof LangString) {
                                LangString langStr = (LangString) trg;
                                ps.setObject(2, langStr.getString());
                                ps.setObject(3, langStr.getLang());
                            } else {
                                ps.setObject(2, trg);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        c.commit();
                        ps.close();
                        c.setAutoCommit(true);
                    }
                }
            });
        }

        return records;
    }

    public void deleteProductType12() {
        deleteProductType12(null);
    }    

    public void deleteProductType12(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM product_type12");
        String in = "";
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            in = "IN (" + sj.toString() + ")";
            querySB.append(" WHERE id " + in);
        }

        final String finalIn = in;
        final String query = querySB.toString();

        SqlUtils.run(connection, c -> {
            PreparedStatement ps = c.prepareStatement(query);
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_product_type12_product_feature_product_feature" + (finalIn.isEmpty() ? "" : " WHERE product_type12_id " + finalIn));
            ps.execute();
            ps.close();

            ps = c.prepareStatement("DELETE FROM mn_resource_type_class" + (finalIn.isEmpty() ? "" : " WHERE resource_id " + finalIn));
            ps.execute();
            ps.close();

        });
    }


    //==========================================================================

    public void open() {
        if(isConnected()) {
            throw new RuntimeException("already connected");
        }

        try {
            connection = DriverManager.getConnection("jdbc:sqlite:" + databaseFile.getAbsolutePath());
        } catch(SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void close() {
        if(!isConnected()) {
            throw new RuntimeException("not connected");
        }

        try {
            connection.close();
            connection = null;
        } catch(SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public boolean isConnected() {
        return connection != null;
    }

}