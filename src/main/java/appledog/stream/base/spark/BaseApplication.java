package appledog.stream.base.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

public abstract class BaseApplication extends SparkSessionWrapper {
    protected final String configPath;

    protected BaseApplication(String configPath, String applicationName) {
        this.configPath = configPath;
        initSparkSession(applicationName, configPath);
    }

    public void start() {
        try (SparkSession sparkSession = getSparkSession()) {
            SparkContext sparkContext = sparkSession.sparkContext();
            init(sparkContext);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            close();
        }
    }

    public void init(SparkContext sparkContext) throws InterruptedException {
    }
}
