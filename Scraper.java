
import yahoofinance.Stock;
import yahoofinance.YahooFinance;
import java.io.*;
import java.math.BigDecimal;
import java.util.LinkedHashMap;


public class Scraper
{
    public static String getStockPrice(String symbol) throws IOException {
        Stock stock = YahooFinance.get(symbol);
        BigDecimal price = stock.getQuote().getPrice();
        return price.toString();
    }


    //column names/keys for our data table
    static final LinkedHashMap<String, String> stockColumnNames = new LinkedHashMap<>();
    static
    {
        stockColumnNames.put("Date", "date");
        stockColumnNames.put("Stock", "stock");
        stockColumnNames.put("Price", "price");
        stockColumnNames.put("Total_Value", "total value");

    }
    static final LinkedHashMap<String, String> valueColumnNames = new LinkedHashMap<>();
    static
    {
        valueColumnNames.put("Date", "date");
        valueColumnNames.put("Total_Value", "total value");
        valueColumnNames.put("Daily_Gain", "gain");
        valueColumnNames.put("Total_Gain", "total_gain");
    }

}
