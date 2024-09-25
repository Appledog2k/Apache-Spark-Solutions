package appledog.stream.connector;

import appledog.stream.base.api.iface.Deserializer;
import appledog.stream.base.api.iface.Serializer;
import appledog.stream.base.redis.iface.DistributedMapCacheClient;
import appledog.stream.base.redis.service.RedisDistributedMapCacheClientService;
import appledog.stream.utils.StringConstants;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisConnector implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(RedisConnector.class);
    private final SparkConf sparkConf;
    private DistributedMapCacheClient distributedMapCacheClient = null;

    public RedisConnector(SparkConf sparkConf) {
        this.sparkConf = sparkConf;
    }

    private DistributedMapCacheClient init() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
        return new RedisDistributedMapCacheClientService(sparkConf);
    }

    public void close() {
        distributedMapCacheClient.disable();
    }

    public synchronized void xAdd(String streamKey, String message, String kafkaTopicOut) {
        Map<byte[], byte[]> messageBody = new HashMap<>();
        logger.info("send stream key ={}", streamKey);
        logger.debug("send message ={}", message);
        messageBody.put(StringConstants.DATA.getBytes(StandardCharsets.UTF_8),
                message.getBytes(StandardCharsets.UTF_8));
        messageBody.put(StringConstants.KAFKA_TOPIC.getBytes(StandardCharsets.UTF_8),
                kafkaTopicOut.getBytes(StandardCharsets.UTF_8));
        messageBody.put(StringConstants.VALUE_TS.getBytes(StandardCharsets.UTF_8),
                String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));
        if (distributedMapCacheClient == null) {
            distributedMapCacheClient = init();
        }
        distributedMapCacheClient.xAdd(streamKey.getBytes(StandardCharsets.UTF_8), messageBody);
    }

    public synchronized void xAdd(String streamKey, String message, byte[] correlationIdByte, String kafkaTopicOut) {
        Map<byte[], byte[]> messageBody = new HashMap<>();
        logger.info("send stream key ={}", streamKey);
        logger.debug("send message ={}", message);
        messageBody.put(StringConstants.DATA.getBytes(StandardCharsets.UTF_8),
                message.getBytes(StandardCharsets.UTF_8));
        messageBody.put(StringConstants.KAFKA_TOPIC.getBytes(StandardCharsets.UTF_8),
                kafkaTopicOut.getBytes(StandardCharsets.UTF_8));
        messageBody.put(StringConstants.VALUE_TS.getBytes(StandardCharsets.UTF_8),
                String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));

        if (correlationIdByte != null) {
            messageBody.put(StringConstants.ZILLA_CORRELATION_ID.getBytes(StandardCharsets.UTF_8),
                    correlationIdByte);
        }
        if (distributedMapCacheClient == null) {
            distributedMapCacheClient = init();
        }
        distributedMapCacheClient.xAdd(streamKey.getBytes(StandardCharsets.UTF_8), messageBody);
    }

    public void put(String key, String value, Serializer<String> keySerializer,
                    Serializer<String> valueSerializer) throws IOException {
        if (distributedMapCacheClient == null) {
            distributedMapCacheClient = init();
        }
        distributedMapCacheClient.put(key, value, keySerializer, valueSerializer);
    }

    public void set(String key, String value, Serializer<String> keySerializer,
                    Serializer<String> valueSerializer) throws IOException {
        if (distributedMapCacheClient == null) {
            distributedMapCacheClient = init();
        }
        distributedMapCacheClient.set(key, value, -1L, keySerializer, valueSerializer);
    }

    public void publish(String channel, String value, Serializer<String> keySerializer,
                        Serializer<String> valueSerializer) throws IOException {
        if (distributedMapCacheClient == null) {
            distributedMapCacheClient = init();
        }
        distributedMapCacheClient.publish(channel, value, keySerializer, valueSerializer);
    }

    public List<String> getList(String key, Serializer<String> keySerializer,
                                Deserializer<String> valueStringDeserializer) throws IOException {
        if (distributedMapCacheClient == null) {
            distributedMapCacheClient = init();
        }
        return distributedMapCacheClient.getList(key, keySerializer, valueStringDeserializer);
    }

    public String get(String key, Serializer<String> keySerializer,
                      Deserializer<String> valueStringDeserializer) throws IOException {
        if (distributedMapCacheClient == null) {
            distributedMapCacheClient = init();
        }
        return distributedMapCacheClient.get(key, keySerializer, valueStringDeserializer);
    }

    public long removeByPattern(String keyPattern) throws IOException {
        if (distributedMapCacheClient == null) {
            distributedMapCacheClient = init();
        }
        return distributedMapCacheClient.removeByPattern(keyPattern);
    }

    public boolean isExitsKey(String key, Serializer<String> keySerializer) throws IOException {
        if (distributedMapCacheClient == null) {
            distributedMapCacheClient = init();
        }
        return distributedMapCacheClient.containsKey(key, keySerializer);
    }
}
