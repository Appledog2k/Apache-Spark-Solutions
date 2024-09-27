package appledog.stream.application;

import appledog.stream.base.api.serializer.StringSerializer;
import appledog.stream.base.spark.BaseOracleSyncData;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkOracleSyncData extends BaseOracleSyncData {
    private static final Logger logger = LoggerFactory.getLogger(SparkStreamKafka.class);
    public SparkOracleSyncData(String configPath, String applicationName) {
        super(configPath, applicationName);
    }

    @Override
    public void execute(Dataset<Row> df) {
        // push data oracle to redis
        df.foreachPartition(row -> {
            while (row.hasNext()) {
                Row r = row.next();
                String customer_code = r.getAs("CUSTOMER_CODE").toString();
                String customer_type = r.getAs("CUSTOMER_TYPE").toString();
                broadcastRedisConnector.getValue().set(customer_code,customer_type, new StringSerializer(), new StringSerializer());
                logger.info("push data to redis: product_code: {}, customer_type: {}", customer_code, customer_type);
            }
        });
    }


    public static void main(String[] args) {
        String configPath = args.length == 0 ? System.getProperty("user.dir") + "/config/application.properties" : args[0];
        SparkOracleSyncData sparkOracleSyncData = new SparkOracleSyncData(configPath, SparkOracleSyncData.class.getName());
        sparkOracleSyncData.start();
    }
}
