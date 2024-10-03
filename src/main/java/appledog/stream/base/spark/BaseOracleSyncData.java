package appledog.stream.base.spark;

import appledog.stream.connectors.DatabaseClientConnector;
import appledog.stream.connectors.RedisConnector;
import appledog.stream.utils.BroadcastTag;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseOracleSyncData extends BaseApplication{
    private static final Logger logger = LoggerFactory.getLogger(BaseOracleSyncData.class);
    protected Broadcast<DatabaseClientConnector> broadcastDatabaseClientConnector;
    protected Broadcast<RedisConnector> broadcastRedisConnector;
    protected BaseOracleSyncData(String configPath, String applicationName) {
        super(configPath, applicationName);
    }

    public abstract void execute(Dataset<Row> df);

    @Override
    public void init(SparkContext sparkContext) throws InterruptedException {
        try {
            sparkContext.setLogLevel("INFO");
            SparkConf sparkConf = sparkContext.getConf();

            broadcastRedisConnector = sparkContext.broadcast(
                    new RedisConnector(sparkConf),
                    BroadcastTag.classTag(RedisConnector.class));
            logger.info("created Broadcast RedisConnector");

            Dataset<Row> df = queryTable("select * from MSB_CDP.CDP_CUSTOMER_INFO where ETL_DATE = 20240101");
            if (!df.isEmpty()) {
                execute(df);
            }

        } catch (Exception e) {
            logger.error("run base oracle sync data error: {0}", e);
            throw new InterruptedException();
        }
    }
}
