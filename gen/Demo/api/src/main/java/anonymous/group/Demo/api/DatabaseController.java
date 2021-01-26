package anonymous.group.Demo.api;

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
            ps.close();
            
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
    // CapitalCity

    private RSQLVisitor<Boolean, CapitalCity> CapitalCityVisitor = new RSQLVisitor<Boolean, CapitalCity>() {

        @Override
        public Boolean visit(AndNode and, CapitalCity record) {
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
        public Boolean visit(OrNode or, CapitalCity record) {
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
        public Boolean visit(ComparisonNode cn, CapitalCity record) {

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

    public List<CapitalCity> selectCapitalCity() {
        return selectCapitalCity(null, null, null, null);
    }

    public List<CapitalCity> selectCapitalCity(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM capital_city\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        String query = querySB.toString();

        List<CapitalCity> result = SqlUtils.supply(connection, c -> {
            List<CapitalCity> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                CapitalCity record = new CapitalCity();

                record.setId(SqlUtils.getLong(rs, "id"));

                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(CapitalCityVisitor, record));
        }

        if(offset != null || limit != null) {
            if(limit == null) {
                limit = result.size();
            }
            if(limit < 0) {
                limit = 0;
            }
            if(offset == null || offset < 0) {
                offset = 0;
            }
            result = result.subList(Math.min(result.size(), offset), Math.min(result.size(), offset + limit));
        }

        return result;
    }

    public List<CapitalCity> insertCapitalCity(List<CapitalCity> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO capital_city VALUES (?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(CapitalCity record : records) {
                ps.setObject(1, record.getId());
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

            c.setAutoCommit(true);
        });

        return records;
    }

    public List<CapitalCity> updateCapitalCity(List<CapitalCity> records, boolean put) {    
        for(CapitalCity record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE capital_city SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
            });
        }

        return records;
    }

    public void deleteCapitalCity() {
        deleteCapitalCity(null);
    }    

    public void deleteCapitalCity(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM capital_city");
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

        });
    }

    //==========================================================================
    // Page

    private RSQLVisitor<Boolean, Page> PageVisitor = new RSQLVisitor<Boolean, Page>() {

        @Override
        public Boolean visit(AndNode and, Page record) {
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
        public Boolean visit(OrNode or, Page record) {
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
        public Boolean visit(ComparisonNode cn, Page record) {

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

    public List<Page> selectPage() {
        return selectPage(null, null, null, null);
    }

    public List<Page> selectPage(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM page\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        String query = querySB.toString();

        List<Page> result = SqlUtils.supply(connection, c -> {
            List<Page> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                Page record = new Page();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setBookHasPage(SqlUtils.getLong(rs, "book_has_page"));
                record.setPaperbackHasPage(SqlUtils.getLong(rs, "paperback_has_page"));

                record.setMentionsCapitalCity(selectWithNM(record.getId(), "page_id", "mn_page_mentions_capital_city", Long.class));
                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(PageVisitor, record));
        }

        if(offset != null || limit != null) {
            if(limit == null) {
                limit = result.size();
            }
            if(limit < 0) {
                limit = 0;
            }
            if(offset == null || offset < 0) {
                offset = 0;
            }
            result = result.subList(Math.min(result.size(), offset), Math.min(result.size(), offset + limit));
        }

        return result;
    }

    public List<Page> insertPage(List<Page> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO page VALUES (?,?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(Page record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getBookHasPage());
                ps.setObject(3, record.getPaperbackHasPage());
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
            query = "INSERT INTO mn_page_mentions_capital_city VALUES (?,?)";

            ps = c.prepareStatement(query);

            for(Page record : records) {
                if(!record.hasId() || !record.hasMentionsCapitalCity() || record.getMentionsCapitalCity().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getMentionsCapitalCity()) {
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

    public List<Page> updatePage(List<Page> records, boolean put) {    
        for(Page record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasBookHasPage()) {
                    paramSJ.add("book_has_page = ?");
                }
                if(put || record.hasPaperbackHasPage()) {
                    paramSJ.add("paperback_has_page = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE page SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasBookHasPage()) {
                    ps.setObject(index, record.getBookHasPage());
                    index++;
                }
                if(put || record.hasPaperbackHasPage()) {
                    ps.setObject(index, record.getPaperbackHasPage());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
                if(put || (record.hasMentionsCapitalCity() && !record.getMentionsCapitalCity().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_page_mentions_capital_city WHERE page_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasMentionsCapitalCity() && !record.getMentionsCapitalCity().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_page_mentions_capital_city VALUES (?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getMentionsCapitalCity()) {
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

    public void deletePage() {
        deletePage(null);
    }    

    public void deletePage(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM page");
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

            ps = c.prepareStatement("DELETE FROM mn_page_mentions_capital_city" + (finalIn.isEmpty() ? "" : " WHERE page_id " + finalIn));
            ps.execute();
            ps.close();

        });
    }

    //==========================================================================
    // Paperback

    private RSQLVisitor<Boolean, Paperback> PaperbackVisitor = new RSQLVisitor<Boolean, Paperback>() {

        @Override
        public Boolean visit(AndNode and, Paperback record) {
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
        public Boolean visit(OrNode or, Paperback record) {
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
        public Boolean visit(ComparisonNode cn, Paperback record) {

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

    public List<Paperback> selectPaperback() {
        return selectPaperback(null, null, null, null);
    }

    public List<Paperback> selectPaperback(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM paperback\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        String query = querySB.toString();

        List<Paperback> result = SqlUtils.supply(connection, c -> {
            List<Paperback> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                Paperback record = new Paperback();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setHasCover(SqlUtils.getString(rs, "has_cover"));
                record.setTakesPlaceIn(SqlUtils.getLong(rs, "takes_place_in"));

                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(PaperbackVisitor, record));
        }

        if(offset != null || limit != null) {
            if(limit == null) {
                limit = result.size();
            }
            if(limit < 0) {
                limit = 0;
            }
            if(offset == null || offset < 0) {
                offset = 0;
            }
            result = result.subList(Math.min(result.size(), offset), Math.min(result.size(), offset + limit));
        }

        return result;
    }

    public List<Paperback> insertPaperback(List<Paperback> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO paperback VALUES (?,?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(Paperback record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getHasCover());
                ps.setObject(3, record.getTakesPlaceIn());
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

            c.setAutoCommit(true);
        });

        return records;
    }

    public List<Paperback> updatePaperback(List<Paperback> records, boolean put) {    
        for(Paperback record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasHasCover()) {
                    paramSJ.add("has_cover = ?");
                }
                if(put || record.hasTakesPlaceIn()) {
                    paramSJ.add("takes_place_in = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE paperback SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasHasCover()) {
                    ps.setObject(index, record.getHasCover());
                    index++;
                }
                if(put || record.hasTakesPlaceIn()) {
                    ps.setObject(index, record.getTakesPlaceIn());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
            });
        }

        return records;
    }

    public void deletePaperback() {
        deletePaperback(null);
    }    

    public void deletePaperback(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM paperback");
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

        });
    }

    //==========================================================================
    // Book

    private RSQLVisitor<Boolean, Book> BookVisitor = new RSQLVisitor<Boolean, Book>() {

        @Override
        public Boolean visit(AndNode and, Book record) {
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
        public Boolean visit(OrNode or, Book record) {
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
        public Boolean visit(ComparisonNode cn, Book record) {

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

    public List<Book> selectBook() {
        return selectBook(null, null, null, null);
    }

    public List<Book> selectBook(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM book\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        String query = querySB.toString();

        List<Book> result = SqlUtils.supply(connection, c -> {
            List<Book> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                Book record = new Book();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setHasCover(SqlUtils.getString(rs, "has_cover"));
                record.setPublishedDate(SqlUtils.getLong(rs, "published_date"));
                record.setTakesPlaceIn(SqlUtils.getLong(rs, "takes_place_in"));

                record.setName(selectWithNM(record.getId(), "book_id", "mn_book_name", LangString.class));
                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(BookVisitor, record));
        }

        if(offset != null || limit != null) {
            if(limit == null) {
                limit = result.size();
            }
            if(limit < 0) {
                limit = 0;
            }
            if(offset == null || offset < 0) {
                offset = 0;
            }
            result = result.subList(Math.min(result.size(), offset), Math.min(result.size(), offset + limit));
        }

        return result;
    }

    public List<Book> insertBook(List<Book> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO book VALUES (?,?,?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(Book record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getHasCover());
                ps.setObject(3, record.getPublishedDate());
                ps.setObject(4, record.getTakesPlaceIn());
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
            query = "INSERT INTO mn_book_name VALUES (?,?,?)";

            ps = c.prepareStatement(query);

            for(Book record : records) {
                if(!record.hasId() || !record.hasName() || record.getName().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.getName()) {
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

    public List<Book> updateBook(List<Book> records, boolean put) {    
        for(Book record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasHasCover()) {
                    paramSJ.add("has_cover = ?");
                }
                if(put || record.hasPublishedDate()) {
                    paramSJ.add("published_date = ?");
                }
                if(put || record.hasTakesPlaceIn()) {
                    paramSJ.add("takes_place_in = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE book SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasHasCover()) {
                    ps.setObject(index, record.getHasCover());
                    index++;
                }
                if(put || record.hasPublishedDate()) {
                    ps.setObject(index, record.getPublishedDate());
                    index++;
                }
                if(put || record.hasTakesPlaceIn()) {
                    ps.setObject(index, record.getTakesPlaceIn());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
                if(put || (record.hasName() && !record.getName().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM mn_book_name WHERE book_id = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.hasName() && !record.getName().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO mn_book_name VALUES (?,?,?)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.getName()) {
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

    public void deleteBook() {
        deleteBook(null);
    }    

    public void deleteBook(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM book");
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

            ps = c.prepareStatement("DELETE FROM mn_book_name" + (finalIn.isEmpty() ? "" : " WHERE book_id " + finalIn));
            ps.execute();
            ps.close();

        });
    }

    //==========================================================================
    // Country

    private RSQLVisitor<Boolean, Country> CountryVisitor = new RSQLVisitor<Boolean, Country>() {

        @Override
        public Boolean visit(AndNode and, Country record) {
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
        public Boolean visit(OrNode or, Country record) {
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
        public Boolean visit(ComparisonNode cn, Country record) {

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

    public List<Country> selectCountry() {
        return selectCountry(null, null, null, null);
    }

    public List<Country> selectCountry(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM country\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        String query = querySB.toString();

        List<Country> result = SqlUtils.supply(connection, c -> {
            List<Country> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                Country record = new Country();

                record.setId(SqlUtils.getLong(rs, "id"));
                record.setHasCapitalCity(SqlUtils.getLong(rs, "has_capital_city"));

                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(CountryVisitor, record));
        }

        if(offset != null || limit != null) {
            if(limit == null) {
                limit = result.size();
            }
            if(limit < 0) {
                limit = 0;
            }
            if(offset == null || offset < 0) {
                offset = 0;
            }
            result = result.subList(Math.min(result.size(), offset), Math.min(result.size(), offset + limit));
        }

        return result;
    }

    public List<Country> insertCountry(List<Country> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO country VALUES (?,?)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(Country record : records) {
                ps.setObject(1, record.getId());
                ps.setObject(2, record.getHasCapitalCity());
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

            c.setAutoCommit(true);
        });

        return records;
    }

    public List<Country> updateCountry(List<Country> records, boolean put) {    
        for(Country record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                if(put || record.hasHasCapitalCity()) {
                    paramSJ.add("has_capital_city = ?");
                }
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE country SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                if(put || record.hasHasCapitalCity()) {
                    ps.setObject(index, record.getHasCapitalCity());
                    index++;
                }
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
            });
        }

        return records;
    }

    public void deleteCountry() {
        deleteCountry(null);
    }    

    public void deleteCountry(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM country");
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