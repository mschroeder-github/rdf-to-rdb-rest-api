package ${package};

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

    <#list javaClasses as javaClass>
    //==========================================================================
    // ${javaClass.getName()}

    private RSQLVisitor<Boolean, ${javaClass.getName()}> ${javaClass.getName()}Visitor = new RSQLVisitor<Boolean, ${javaClass.getName()}>() {

        @Override
        public Boolean visit(AndNode and, ${javaClass.getName()} record) {
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
        public Boolean visit(OrNode or, ${javaClass.getName()} record) {
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
        public Boolean visit(ComparisonNode cn, ${javaClass.getName()} record) {

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

    public List<${javaClass.getName()}> select${javaClass.getName()}() {
        return select${javaClass.getName()}(null, null, null, null);
    }

    public List<${javaClass.getName()}> select${javaClass.getName()}(List<Long> ids, Integer offset, Integer limit, Node rqlNode) {
        StringBuilder querySB = new StringBuilder("SELECT * FROM ${javaClass.getTable().getName()}\n");

        String idClause = null;
        if(ids != null) {
            StringJoiner sj = new StringJoiner(",");
            ids.forEach(id -> sj.add(String.valueOf(id)));
            idClause = "id IN (" + sj.toString() + ")";

            querySB.append("WHERE ");
            querySB.append(idClause);
        }

        String query = querySB.toString();

        List<${javaClass.getName()}> result = SqlUtils.supply(connection, c -> {
            List<${javaClass.getName()}> list = new ArrayList<>();

            PreparedStatement ps = c.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                ${javaClass.getName()} record = new ${javaClass.getName()}();

                <#list javaClass.getSingleAttributes() as attr>
                record.set${attr.getMethodName()}(SqlUtils.get${attr.getResultSetMethod()}(rs, "${attr.getColumn().getName()}"));
                </#list>

                <#list javaClass.getListAttributes() as attr>
                record.set${attr.getMethodName()}(selectWithNM(record.getId(), "${attr.getTable().getColumns()[0].getName()}", "${attr.getTable().getName()}", ${attr.getTypeSingle()}.class));
                </#list>
                
                list.add(record);
            }
            ps.close();

            return list;
        });

        if(rqlNode != null) {
            result.removeIf(record -> !rqlNode.accept(${javaClass.getName()}Visitor, record));
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

    public List<${javaClass.getName()}> insert${javaClass.getName()}(List<${javaClass.getName()}> records) {
        if(records.isEmpty())
            return records;

        SqlUtils.run(connection, c -> {
            String query = "INSERT INTO ${javaClass.getTable().getName()} (<#list javaClass.getSingleAttributes() as attr>\"${attr.getColumn().getName()}\"<#if !(attr?is_last)>,</#if></#list>) VALUES (<#list javaClass.getSingleAttributes() as attr>?<#if !(attr?is_last)>,</#if></#list>)";

            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            for(${javaClass.getName()} record : records) {
                <#list javaClass.getSingleAttributes() as attr>
                ps.setObject(${attr?index + 1}, record.get${attr.getMethodName()}());
                </#list>
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
            <#list javaClass.getListAttributes() as attr>
            query = "INSERT INTO ${attr.getTable().getName()} VALUES (<#list attr.getTable().getColumns() as col>?<#if !(col?is_last)>,</#if></#list>)";

            ps = c.prepareStatement(query);

            for(${javaClass.getName()} record : records) {
                if(!record.hasId() || !record.has${attr.getMethodName()}() || record.get${attr.getMethodName()}().isEmpty())
                    continue;

                ps.setObject(1, record.getId());
                for(Object trg : record.get${attr.getMethodName()}()) {
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

            </#list>

            c.setAutoCommit(true);
        });

        return records;
    }

    public List<${javaClass.getName()}> update${javaClass.getName()}(List<${javaClass.getName()}> records, boolean put) {    
        for(${javaClass.getName()} record : records) {
            if(!record.hasId())
                continue;
            
            SqlUtils.run(connection, c -> {
                StringJoiner paramSJ = new StringJoiner(", ");
                <#list javaClass.getSingleAttributesWithoutId() as attr>
                if(put || record.has${attr.getMethodName()}()) {
                    paramSJ.add("\"${attr.getColumn().getName()}\" = ?");
                }
                </#list>
                if(paramSJ.length() == 0) {
                    return;
                }
                String query = "UPDATE ${javaClass.getTable().getName()} SET "+ paramSJ.toString() +" WHERE id = ?";

                PreparedStatement ps = c.prepareStatement(query);
                int index = 1;
                <#list javaClass.getSingleAttributesWithoutId() as attr>
                if(put || record.has${attr.getMethodName()}()) {
                    ps.setObject(index, record.get${attr.getMethodName()}());
                    index++;
                }
                </#list>
                ps.setLong(index, record.getId());
                ps.execute();
                ps.close();

                //m:n relations
                <#list javaClass.getListAttributes() as attr>
                if(put || (record.has${attr.getMethodName()}() && !record.get${attr.getMethodName()}().isEmpty())) {
                    ps = c.prepareStatement("DELETE FROM ${attr.getTable().getName()} WHERE ${attr.getTable().getColumns()[0].getName()} = " + record.getId());
                    ps.execute();
                    ps.close();

                    if(record.has${attr.getMethodName()}() && !record.get${attr.getMethodName()}().isEmpty()) {
                        c.setAutoCommit(false);
                        ps = c.prepareStatement("INSERT INTO ${attr.getTable().getName()} VALUES (<#list attr.getTable().getColumns() as col>?<#if !(col?is_last)>,</#if></#list>)");
                        ps.setObject(1, record.getId());
                        for(Object trg : record.get${attr.getMethodName()}()) {
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
                </#list>
            });
        }

        return records;
    }

    public void delete${javaClass.getName()}() {
        delete${javaClass.getName()}(null);
    }    

    public void delete${javaClass.getName()}(List<Long> ids) {    
        StringBuilder querySB = new StringBuilder("DELETE FROM ${javaClass.getTable().getName()}");
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

            <#list javaClass.getListAttributes() as attr>
            ps = c.prepareStatement("DELETE FROM ${attr.getTable().getName()}" + (finalIn.isEmpty() ? "" : " WHERE ${attr.getTable().getColumns()[0].getName()} " + finalIn));
            ps.execute();
            ps.close();

            </#list>
        });
    }

    </#list>

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