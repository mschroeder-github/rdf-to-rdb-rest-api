package anonymous.group.Demo.api;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import cz.jirutka.rsql.parser.RSQLParser;
import cz.jirutka.rsql.parser.ast.*;
import java.util.Set;

/**
 * Utility to perform SQL on a connection.
 * It handles exceptions and synchronization.
 */
public class SqlUtils {
    
    public static Node toRQL(String rql) {
        Set<ComparisonOperator> operators = RSQLOperators.defaultOperators();
        operators.add(new ComparisonOperator("=regex=", false));
        operators.add(new ComparisonOperator("=lang=", false));
        return new RSQLParser(operators).parse(rql);
    }

    /**
     * Use this to just run sql statements with the connection.
     * @param connection
     * @param runnable 
     */
    public static void run(Connection connection, SQLRunnable runnable) {
        try {
            synchronized(connection) {
                runnable.run(connection);
            }
        } catch(SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Use this when you want to return something.
     * @param <T>
     * @param connection
     * @param runnable
     * @return 
     */
    public static <T> T supply(Connection connection, SQLSupplier<T> runnable) {
        try {
            synchronized(connection) {
                return runnable.run(connection);
            }
        } catch(SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Use this when you just want to execute a query.
     * @param connection
     * @param query 
     */
    public static void execute(Connection connection, String query) {
        run(connection, c -> {
            PreparedStatement stmt = c.prepareStatement(query);
            stmt.execute();
            stmt.close();
        });
    }
    
    /**
     * Like execute but you can prepare it beforehand.
     * @param connection
     * @param query
     * @param preparer 
     */
    public static void executePrepared(Connection connection, String query, Preparer preparer) {
        run(connection, c -> {
            PreparedStatement stmt = c.prepareStatement(query);
            preparer.prepare(stmt);
            stmt.execute();
            stmt.close();
        });
    }
    
    /**
     * Use this when you want to prepare the statement and return it.
     * You have to close it later.
     * @param connection
     * @param query
     * @param preparer
     * @return 
     */
    public static PreparedStatement prepare(Connection connection, String query, Preparer preparer) {
        return supply(connection, c -> {
            PreparedStatement stmt = c.prepareStatement(query);
            preparer.prepare(stmt);
            return stmt;
        });
    }
    
    @FunctionalInterface
    public interface SQLRunnable {
        
        public void run(Connection connection) throws SQLException;
        
    }
    
    @FunctionalInterface
    public interface SQLSupplier<T> {
        
        public T run(Connection connection) throws SQLException;
        
    }
    
    @FunctionalInterface
    public interface Preparer {
        
        public void prepare(PreparedStatement stmt) throws SQLException;
        
    }

    public static Integer getInt(ResultSet rs, String colName) throws SQLException {
        int nValue = rs.getInt(colName);
        return rs.wasNull() ? null : nValue;
    }

    public static Integer getInt(ResultSet rs, int colIndex) throws SQLException {
        int nValue = rs.getInt(colIndex);
        return rs.wasNull() ? null : nValue;
    }

    public static Long getLong(ResultSet rs, String colName) throws SQLException {
        long nValue = rs.getLong(colName);
        return rs.wasNull() ? null : nValue;
    }

    public static Long getLong(ResultSet rs, int colIndex) throws SQLException {
        long nValue = rs.getLong(colIndex);
        return rs.wasNull() ? null : nValue;
    }

    public static Double getDouble(ResultSet rs, String colName) throws SQLException {
        double nValue = rs.getInt(colName);
        return rs.wasNull() ? null : nValue;
    }

    public static Double getDouble(ResultSet rs, int colIndex) throws SQLException {
        double nValue = rs.getInt(colIndex);
        return rs.wasNull() ? null : nValue;
    }
    
    public static String getString(ResultSet rs, String colName) throws SQLException {
        String nValue = rs.getString(colName);
        return rs.wasNull() ? null : nValue;
    }

    public static String getString(ResultSet rs, int colIndex) throws SQLException {
        String nValue = rs.getString(colIndex);
        return rs.wasNull() ? null : nValue;
    }
     
}