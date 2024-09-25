package appledog.stream.base.spark;

import appledog.stream.utils.PropertiesFileReader;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
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

    public void close() {
        if (sparkSession != null) {
            sparkSession.close();
        }
    }
}
