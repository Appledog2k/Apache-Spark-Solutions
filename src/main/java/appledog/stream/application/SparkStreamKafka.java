package appledog.stream.application;

import appledog.stream.base.api.serializer.StringSerializer;
import appledog.stream.base.api.serializer.StringValueDeserializer;
import appledog.stream.base.spark.BaseSparkKafkaStream;
import appledog.stream.utils.StreamingUtils;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SparkStreamKafka extends BaseSparkKafkaStream {
    private static final Logger logger = LoggerFactory.getLogger(SparkStreamKafka.class);

    public SparkStreamKafka(String configPath, String applicationName) {
        super(configPath, applicationName);
    }

    @Override
    public void execute(JavaRDD<ConsumerRecord<String, String>> javaRDD) {
        JavaRDD<Row> rowRDD = javaRDD.mapPartitions(partitionOfRecords -> {
            List<Row> rows = new ArrayList<>();
            while (partitionOfRecords.hasNext()) {
                ConsumerRecord<String, String> consumerRecord = partitionOfRecords.next();
                boolean isJson = StreamingUtils.jsonValidate(consumerRecord.value());
                if (!isJson) continue;
                try {
                    JsonObject json = StreamingUtils.toJson(consumerRecord.value());
                    String cif = json.get("CIF").getAsString();
                    String product_code = json.get("Product code").getAsString();
                    if (broadcastRedisConnector.getValue().isExitsKey(cif, new StringSerializer())) {
                        String value_redis = broadcastRedisConnector.getValue().get(cif, new StringSerializer(), new StringValueDeserializer());
                        rows.add(RowFactory.create(cif, product_code, value_redis));
                    }
                } catch (Exception ex) {
                    logger.error("process json in error: {}", ex.getMessage());
                }
            }
            return rows.iterator();
        });

        // Convert RDD to DataFrame
        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();
        Dataset<Row> df = spark.createDataFrame(rowRDD,
                org.apache.spark.sql.types.DataTypes.createStructType(List.of(
                        org.apache.spark.sql.types.DataTypes.createStructField("CIF", org.apache.spark.sql.types.DataTypes.StringType, true),
                        org.apache.spark.sql.types.DataTypes.createStructField("Product code", org.apache.spark.sql.types.DataTypes.StringType, true),
                        org.apache.spark.sql.types.DataTypes.createStructField("Customer Type", org.apache.spark.sql.types.DataTypes.StringType, true)
                ))
        );

        // Show DataFrame content (this will print to console)
        df.show();
    }
    public static void main(String[] args) {
        StreamingUtils.offLog();
        String configPath = args.length == 0 ? System.getProperty("user.dir") + "/config/application.properties" : args[0];
        SparkStreamKafka sparkStreamKafka = new SparkStreamKafka(configPath, SparkStreamKafka.class.getName());
        sparkStreamKafka.start();
    }
}
