package appledog.stream.connectors;

import appledog.stream.utils.StringConstants;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public interface KafkaStreamConnector {
    static JavaInputDStream<ConsumerRecord<String, String>> createStream(JavaStreamingContext streamingContext, SparkConf sparkConf) {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sparkConf.get(StringConstants.KAFKA_SERVER));
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, sparkConf.get(StringConstants.KAFKA_GROUP_ID));
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, sparkConf.get(StringConstants.KAFKA_AUTO_OFFSET_RESET));
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        kafkaParams.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 5000);

        if ("true".equalsIgnoreCase(sparkConf.get(StringConstants.KAFKA_SECURE))) {
            String secureProtocol = sparkConf.get(StringConstants.KAFKA_SECURITY_PROTOCOL);
            String saslMechanism = sparkConf.get(StringConstants.KAFKA_SASL_MECHANISM);
            String kafkaServiceName = sparkConf.get(StringConstants.KAFKA_SERVICE_NAME);
            String user = sparkConf.get(StringConstants.KAFKA_USER);
            String pass = sparkConf.get(StringConstants.KAFKA_PASSWORD);
            kafkaParams.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, secureProtocol);
            kafkaParams.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
            kafkaParams.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule" + " required serviceName=\"" + kafkaServiceName + "\" username=\"" + user + "\" password=\"" + pass + "\";");
        }

        Pattern topicPattern = Pattern.compile(sparkConf.get(StringConstants.KAFKA_TOPICS_IN));
        return KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(), ConsumerStrategies.SubscribePattern(topicPattern, kafkaParams));

    }
}
