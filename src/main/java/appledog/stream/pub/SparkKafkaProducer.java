package appledog.stream.pub;

import appledog.stream.utils.StringConstants;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class SparkKafkaProducer implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(SparkKafkaProducer.class);
    private transient KafkaProducer<Long, byte[]> producer = null;
    private final SparkConf sparkConf;

    public SparkKafkaProducer(SparkConf sparkConf) {
        this.sparkConf = sparkConf;
    }

    public KafkaProducer<Long, byte[]> init() {
        Properties kafkaProperties = new Properties();
        logger.info("starting create kafka producer");
        String kafkaServer = sparkConf.get(StringConstants.KAFKA_SERVER);
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        logger.info("kafka bootstrap server {}", kafkaServer);
        kafkaProperties.put(ProducerConfig.CLIENT_ID_CONFIG, sparkConf.get(StringConstants.KAFKA_CLIENT_ID));
        kafkaProperties.put(ProducerConfig.ACKS_CONFIG, "0");
        kafkaProperties.put(ProducerConfig.RETRIES_CONFIG, "0");
        kafkaProperties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1048576");
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        kafkaProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        kafkaProperties.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        kafkaProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        if ("true".equalsIgnoreCase(sparkConf.get(StringConstants.KAFKA_SECURE))) {
            logger.info("kafka secure true");
            String secureProtocol = sparkConf.get(StringConstants.KAFKA_SECURITY_PROTOCOL);
            String saslMechanism = sparkConf.get(StringConstants.KAFKA_SASL_MECHANISM);
            String kafkaServiceName = sparkConf.get(StringConstants.KAFKA_SERVICE_NAME);
            String user = sparkConf.get(StringConstants.KAFKA_USER);
            String pass = sparkConf.get(StringConstants.KAFKA_PASSWORD);
            kafkaProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, secureProtocol);
            kafkaProperties.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
            kafkaProperties.put(SaslConfigs.SASL_JAAS_CONFIG,
                    "org.apache.kafka.common.security.scram.ScramLoginModule" +
                            " required serviceName=\"" + kafkaServiceName
                            + "\" username=\"" + user
                            + "\" password=\"" + pass + "\";");
        }
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
        return new KafkaProducer<>(kafkaProperties);
    }
    public void close() {
        producer.close();
    }

    public synchronized void send(String message, String topic) {
        send(null, message, topic);
    }

    public synchronized void send(String correlationIdByte, String message, String topic) {
        try {
            if (producer == null) {
                producer = init();
            }
            ProducerRecord<Long, byte[]> producerRecord;
            if (correlationIdByte != null) {
                logger.info("send record to zilla API with correlationId: {}", correlationIdByte);
                RecordHeaders recordHeaders = new RecordHeaders();
                recordHeaders.add(StringConstants.ZILLA_CORRELATION_ID,
                        correlationIdByte.getBytes(StandardCharsets.UTF_8));

                producerRecord = new ProducerRecord<>(topic, null, null,
                        System.currentTimeMillis(), getMessage(message), recordHeaders);

            } else {
                producerRecord = new ProducerRecord<>(topic, System.currentTimeMillis(), getMessage(message));
            }
            producer.send(producerRecord, new DummyCallback());
        } catch (Exception ex) {
            logger.error("Sending message to kafka error: {}", ex.getMessage());
        }
    }

    private static byte[] getMessage(String message) {
        return serialize(message);
    }

    public static byte[] serialize(final Object obj) {
        return org.apache.commons.lang3.SerializationUtils.serialize((Serializable) obj);
    }

    private static class DummyCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            String topic = recordMetadata.topic();
            if (e != null) {
                logger.error("Error while producing message to topic : {}", topic);
            } else {
                logger.info("sent message to topic: {}, partition: {}, offset: {} ", topic,
                        +recordMetadata.partition(), recordMetadata.offset());
            }
        }
    }
}
