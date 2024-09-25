package appledog.stream.utils;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.apache.log4j.Level;

import java.util.ArrayList;
import java.util.List;

public class StreamingUtils {
    private StreamingUtils(){}

    private static final long KILO = 1024;
    private static final long MEGA = KILO * KILO;
    private static final long GIGA = MEGA * KILO;
    private static final long TERA = GIGA * KILO;

    public static JsonObject toJson(String data) {
        return new JsonParser().parse(data).getAsJsonObject();
    }

    public static List<JsonObject> toListJson(List<String> data) {
        List<JsonObject> jsonObjects = new ArrayList<>();
        data.forEach(d ->{
            JsonObject jsonObject = new JsonParser().parse(d).getAsJsonObject();
            jsonObjects.add(jsonObject);
        });
        return jsonObjects;
    }

    public static JsonArray toJsonArray(String data) {
        return new JsonParser().parse(data).getAsJsonArray();
    }

    public static boolean jsonValidate(String json) {
        try {
            new JsonParser().parse(json);
            return true;
        } catch (JsonSyntaxException jse) {
            return false;
        }
    }


    public static void offLog() {
        org.apache.log4j.Logger.getLogger("org").setLevel(Level.OFF);
        org.apache.log4j.Logger.getLogger("akka").setLevel(Level.OFF);
    }


    public static String getTableName(String select) throws JSQLParserException {
        Statement statement = CCJSqlParserUtil.parse(select);
        Select selectStatement = (Select) statement;
        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
        List<String> tableList = tablesNamesFinder.getTableList(selectStatement);
        if (tableList != null && !tableList.isEmpty()) {
            return tableList.get(0);
        }
        return null;
    }

    public static String getSize(long size) {
        String s = "";
        double kb = (double)size / KILO;
        double mb = kb / KILO;
        double gb = mb / KILO;
        double tb = gb / KILO;
        if(size < KILO) {
            s = size + " Bytes";
        } else if(size < MEGA) {
            s =  String.format("%.2f", kb) + " KB";
        } else if(size < GIGA) {
            s = String.format("%.2f", mb) + " MB";
        } else if(size < TERA) {
            s = String.format("%.2f", gb) + " GB";
        } else {
            s = String.format("%.2f", tb) + " TB";
        }
        return s;
    }
}
