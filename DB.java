import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

class DB {
    static void DBConnect() {
        try {
            Class.forName(getAuth("driver")).getConstructor().newInstance();
            //setup the connection by taking the credentials from the credentials file
            Run.conn = DriverManager.getConnection(getAuth("url") + "?characterEncoding=latin1", getAuth("username"), getAuth("password"));
            Run.conn.setCatalog(getAuth("DBName"));
        }
        catch (Exception e) {
            System.out.println("Unable to find and load driver: " + e.getMessage());
        }
    }

    static boolean checkIfTableExists(String tableName) {
        Statement stmt = null;
        try {
            stmt = Run.conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW TABLES LIKE '" + tableName + "'");

            //If table already exists
            if (rs.next()) return true;
        }
        catch(SQLException e) {
            System.out.println(e.getMessage());
        }
        finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            }
            catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
        return false;
    }
    //put the data in a queue to be added to the database
    static void addRecordToDBQueue(HashMap<String, String> data, String tablename) {
        try {
            String sql;
            StringJoiner sqlRowsWithValues = new StringJoiner(",");
            StringJoiner sqlValues = new StringJoiner(",");

            sql = "INSERT INTO `" + tablename + "` (";

            for (Map.Entry<String, String> entry : data.entrySet()) {
                sqlRowsWithValues.add(" `" + entry.getKey() + "`");
                sqlValues.add("'" + entry.getValue() + "'");
            }

            Run.statement.addBatch(sql + sqlRowsWithValues.toString() + ")" + " VALUES( " + sqlValues.toString() + ")");
        }
        catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }



    //retrieves data from the credentials file
    static String getAuth(String key) {
        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream(new File(Run.CREDENTIALS_FILE_NAME)));

            return properties.getProperty(key);
        }
        catch(IOException e) {
            System.out.println(e.getMessage());
        }
        return null;
    }
    static void createTable(String tableName) throws SQLException {
        Statement stmtEL;
        StringBuilder sql = new StringBuilder();

        //Get connection info
        stmtEL = Run.conn.createStatement();

        //Create DB table with specified table/column names
        sql.append("CREATE TABLE `" + tableName + "` (id INTEGER not NULL AUTO_INCREMENT,");
        if(tableName.equals(Run.STOCKS_DATA_TABLE)){
            for (Map.Entry<String, String> col : Scraper.stockColumnNames.entrySet())
            {
                //we want everything except the data and state columns to be integers
                if(col.getKey().equals("Date") || col.getKey().equals("Stock"))
                {
                    sql.append(" `" + col.getKey() + "` VARCHAR (20), ");
                }
                else
                {
                    sql.append(" `" + col.getKey() + "` DOUBLE, ");
                }
            }
            sql.append("PRIMARY KEY (id))");
        }
        else if(tableName.equals(Run.TOTAL_VALUE_TABLE)){
            for (Map.Entry<String, String> col : Scraper.valueColumnNames.entrySet())
            {
                //we want everything except the data and state columns to be integers
                if(col.getKey().equals("Date"))
                {
                    sql.append(" `" + col.getKey() + "` VARCHAR (20), ");
                }
                else
                {
                    sql.append(" `" + col.getKey() + "` DOUBLE, ");
                }
            }
            sql.append("PRIMARY KEY (id))");
        }
        //Write changes to DB
        stmtEL.executeUpdate(sql.toString());
        stmtEL.close();
    }
    static void writeQueuedRecordsToDB(Statement stmt) throws SQLException {
        try {
            stmt.executeBatch();
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }
}

