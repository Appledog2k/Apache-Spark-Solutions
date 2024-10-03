package appledog.stream.base.spark;

import appledog.stream.connectors.KafkaStreamConnector;
import appledog.stream.connectors.RedisConnector;
import appledog.stream.pub.SparkKafkaProducer;
import appledog.stream.spliting.SplitOffsetSizeRecord;
import appledog.stream.utils.BroadcastTag;
import appledog.stream.utils.StringConstants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Milliseconds;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseSparkKafkaStream extends BaseApplication {
    private static final Logger logger = LoggerFactory.getLogger(BaseSparkKafkaStream.class);
    protected Broadcast<SparkKafkaProducer> broadcastSparkKafkaProducer;
    protected Broadcast<SplitOffsetSizeRecord> broadcastSplitOffsetSizeRecord;
    protected Broadcast<RedisConnector> broadcastRedisConnector;
    protected BaseSparkKafkaStream(String configPath, String applicationName) {
        super(configPath, applicationName);
    }

    public abstract void execute(JavaRDD<ConsumerRecord<String, String>> javaRDD);

    @Override
    public void init(SparkContext sparkContext) throws InterruptedException {
        try {
            sparkContext.setLogLevel("INFO");

            SparkConf sparkConf = sparkContext.getConf();
            StreamingContext streamingContext = new StreamingContext(sparkContext, Milliseconds.apply(100));
            JavaStreamingContext javaStreamingContext = new JavaStreamingContext(streamingContext);
            JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaStreamConnector.createStream(javaStreamingContext, sparkConf);

            int offsetBatchSize = Integer.parseInt(sparkConf.get(StringConstants.DATA_SOURCE_OFFSET_BATCH_SIZE));
            int offsetStreamSize = Integer.parseInt(sparkConf.get(StringConstants.DATA_SOURCE_OFFSET_STREAM_SIZE));

            broadcastRedisConnector = sparkContext.broadcast(
                    new RedisConnector(sparkConf),
                    BroadcastTag.classTag(RedisConnector.class));
            logger.info("created Broadcast RedisConnector");

            broadcastSparkKafkaProducer = sparkContext.broadcast(
                    new SparkKafkaProducer(sparkConf),
                    BroadcastTag.classTag(SparkKafkaProducer.class));
            logger.info("created Broadcast SparkKafkaProducer");

            broadcastSplitOffsetSizeRecord = sparkContext.broadcast(
                    new SplitOffsetSizeRecord(offsetBatchSize, offsetStreamSize),
                    BroadcastTag.classTag(SplitOffsetSizeRecord.class));
            logger.info("created Broadcast SplitOffsetSizeRecord");

            int repartitionNumber = repartitionNumber();
            logger.info("repartitionNumber size={}", repartitionNumber);
            stream.foreachRDD(rdd -> {
                if (!rdd.isEmpty()) {
                    execute(rdd.repartition(repartitionNumber));
                    OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                    ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
                }
            });

            javaStreamingContext.start();
            javaStreamingContext.awaitTermination();

        } catch (Exception e) {
            logger.error("run base spark kafka stream error: {}", e);
            throw new InterruptedException();
        }
    }
}
