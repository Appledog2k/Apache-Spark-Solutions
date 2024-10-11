package appledog.stream.base.spark;


import appledog.stream.base.kafka.services.SparkKafkaProducer;
import appledog.stream.connectors.DatabaseClientConnector;
import appledog.stream.connectors.RedisConnector;
import appledog.stream.spliting.MessageSplitSizeData;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseSparkRedisStream extends BaseApplication {
    private static final Logger logger = LoggerFactory.getLogger(BaseSparkRedisStream.class);

    protected Broadcast<SparkKafkaProducer> broadcastSparkKafkaProducer;
    protected Broadcast<RedisConnector> broadcastRedisConnector;
    protected Broadcast<MessageSplitSizeData> broadcastMessageSplitSizeData;
    protected Broadcast<DatabaseClientConnector> broadcastDatabaseClientConnector;

    protected BaseSparkRedisStream(String configPath, String applicationName) {
        super(configPath, applicationName);
    }
}
