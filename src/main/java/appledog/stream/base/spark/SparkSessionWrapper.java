package appledog.stream.base.spark;

import appledog.stream.utils.PropertiesFileReader;
import appledog.stream.utils.Security;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.crypto.NoSuchPaddingException;
import java.io.Serializable;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Properties;
import java.util.Set;

public abstract class SparkSessionWrapper implements Serializable {
    private SparkSession sparkSession;
    private SparkConf sparkConf;

    public void initSparkSession(String applicationName, String configPath) {
        Properties properties = PropertiesFileReader.readConfig(configPath);
        this.sparkConf = new SparkConf().setAppName(applicationName);
        Set<String> keyNames = properties.stringPropertyNames();
        keyNames.forEach(keyName -> sparkConf.set(keyName, properties.getProperty(keyName)));
        this.sparkSession = SparkSession.builder().config(sparkConf).master("local[*]").getOrCreate();
    }

    public SparkSession getSparkSession() {
        return this.sparkSession;
    }

    public SparkConf getSparkConf() {
        return sparkConf;
    }

    public Dataset<Row> readTable(String table) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException {
        return sparkSession
                .read()
                .format("jdbc")
                .option("url", "jdbc:oracle:thin:@10.14.222.208:1521:ORCL")
                .option("dbtable", "MSB_CDP.CDP_CUSTOMER_INFO")
                .option("user", "msb_cdp")
                .option("password", "fisdpa2024")
                .option("driver", "oracle.jdbc.driver.OracleDriver")
                .load();
    }

    public Dataset<Row> queryTable(String sql) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException {
        return sparkSession
                .read()
                .format("jdbc")
                .option("url", sparkConf.get("dataSource.uri"))
                .option("user", sparkConf.get("dataSource.user"))
                .option("password", sparkConf.get("dataSource.password"))
                .option("driver", sparkConf.get("dataSource.driver"))
                .option("query", sql)
                .load();
    }

    public int repartitionNumber() {
        int coresMax = Integer.parseInt(sparkSession.sparkContext().conf()
                .get("spark.cores.max"));
        return coresMax * 5;
    }

    public void close() {
        if (sparkSession != null) {
            sparkSession.close();
        }
    }
}
