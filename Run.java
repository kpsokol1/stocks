
import java.io.File;
import java.io.IOException;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;


public class Run
{
    static String STOCKS_DATA_TABLE;
    static String TOTAL_VALUE_TABLE;
    static final String CREDENTIALS_FILE_NAME = "credentials.properties";
    static Connection conn;
    public static Statement statement;
    static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd");
    private static DecimalFormat df = new DecimalFormat("0.00");

    public static void main(String[]args) throws IOException, InterruptedException, SQLException {
        STOCKS_DATA_TABLE = args[1] + "_Stocks";
        TOTAL_VALUE_TABLE = args [1]+ "_Total Values";
        df.setRoundingMode(RoundingMode.UP);
        HashMap<String, String> stocks = new HashMap<>(); //key is the ticker, with number of shares as value
        Scanner reader = new Scanner(new File(args[0]));
        reader.useDelimiter(",");
        String ticker = "";
        String numberOfStocks = "";
        while (reader.hasNextLine()) {
            String line = reader.nextLine();
            stocks.put(parseTicker(line),parseNumberOfShares(line));
        }
        HashMap<String, String> stockData = new HashMap<>(); //holds the data returned by the api for each stock
        HashMap<String, String> valueData = new HashMap<>();
        ArrayList<Double> stockPrices = new ArrayList<>();

        try {
            //Initialize database connection
            DB.DBConnect();
            if (!DB.checkIfTableExists(STOCKS_DATA_TABLE)) DB.createTable(STOCKS_DATA_TABLE);
            if (!DB.checkIfTableExists(TOTAL_VALUE_TABLE)) DB.createTable(TOTAL_VALUE_TABLE);
            statement = conn.createStatement();
            collectStockData(stocks, stockData, valueData, stockPrices);
            DB.writeQueuedRecordsToDB(statement);
            statement = conn.createStatement();
            collectValueData(stockData, valueData, stockPrices);
            DB.writeQueuedRecordsToDB(statement);
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        finally {
            //Close open connection with api.DB
            if (conn != null) {
                conn.close();
            }
        }
    }

    private static String parseTicker(String str){
        String ticker = "";
        Scanner sc = new Scanner(str);
        sc.useDelimiter(",");

        // Check if there is another line of input
        ticker = sc.next();
        sc.close();
        return ticker;
    }
    private static String parseNumberOfShares(String str){
        String number = "";
        Scanner sc = new Scanner(str);
        sc.useDelimiter(",");

        // Check if there is another line of input
        while(sc.hasNext()){
            sc.next();
            number = sc.next();
        }
        sc.close();
        return number;
    }
    public static void collectValueData(HashMap<String,String> stockData, HashMap<String,String> valueData, ArrayList<Double> stockPrices) throws SQLException {
        String sql = "Select Total_Value from `"+TOTAL_VALUE_TABLE+"` order by id desc limit 1";
        String sql2 = "Select Total_Value from `"+TOTAL_VALUE_TABLE+"` order by id asc limit 1";
        for (Map.Entry<String, String> col : Scraper.valueColumnNames.entrySet()) {
            if(col.getKey().equals("Date")) {
                valueData.put(col.getKey(), dtf.format(LocalDate.now()));
            }
            else if (col.getKey().equals("Total_Value")) {
                double runningTotal = 0;
                for (Double element: stockPrices) {
                    runningTotal += element;
                }
                valueData.put(col.getKey(), String.valueOf(df.format(runningTotal)));
            }
            else if (col.getKey().equals("Daily_Gain")) {
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(sql);
                if (rs.next()) {
                    valueData.put(col.getKey(), String.valueOf(df.format(Double.parseDouble(valueData.get("Total_Value")) - (Double.parseDouble(rs.getString("Total_Value"))))));
                }
                else {
                    valueData.put(col.getKey(), "0");
                }
            }
            else if(col.getKey().equals("Total_Gain")){
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(sql2);
                if (rs.next()) {
                    valueData.put(col.getKey(), String.valueOf(df.format(Double.parseDouble(valueData.get("Total_Value")) - (Double.parseDouble(rs.getString("Total_Value"))))));
                }
                else {
                    valueData.put(col.getKey(), "0");
                }
            }
        }
        DB.addRecordToDBQueue(valueData, TOTAL_VALUE_TABLE);
    }
    public static void collectStockData(HashMap<String,String> stocks, HashMap<String,String> stockData, HashMap<String,String> valueData, ArrayList<Double> stockPrices ) throws IOException, SQLException {
        String price = "";

        for (Map.Entry<String, String> element : stocks.entrySet()) {
            //For each column that has a value for this state add it to the HashMap
            for (Map.Entry<String, String> col : Scraper.stockColumnNames.entrySet()) {
                if(col.getKey().equals("Date")) {
                    stockData.put(col.getKey(), dtf.format(LocalDate.now()));
                }
                else if (col.getKey().equals("Stock")) {
                    stockData.put(col.getKey(), element.getKey());
                }
                else if (col.getKey().equals("Price")) {
                    price  = Scraper.getStockPrice(element.getKey());
                    stockData.put(col.getKey(), price);
                    stockData.put("Total_Value", String.valueOf(df.format(Double.valueOf(price)*(Double.parseDouble(element.getValue())))));
                    stockPrices.add(Double.valueOf(price)*(Double.parseDouble(element.getValue())));
                }
            }
            //add the stock
            DB.addRecordToDBQueue(stockData, STOCKS_DATA_TABLE);
        }
    }
}
