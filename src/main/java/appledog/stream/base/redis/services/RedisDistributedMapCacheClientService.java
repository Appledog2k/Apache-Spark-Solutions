package appledog.stream.base.redis.services;

import appledog.stream.base.api.interfaces.Deserializer;
import appledog.stream.base.api.interfaces.PropertyContext;
import appledog.stream.base.api.interfaces.Serializer;
import appledog.stream.base.api.standard.Tuple;
import appledog.stream.base.redis.interfaces.DistributedMapCacheClient;
import appledog.stream.base.redis.interfaces.RedisAction;
import appledog.stream.base.redis.interfaces.RedisConnectionPool;
import appledog.stream.base.redis.utils.RedisStandardProcessContext;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStreamCommands;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.Expiration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RedisDistributedMapCacheClientService implements DistributedMapCacheClient {
    private static final Logger logger = LoggerFactory.getLogger(RedisDistributedMapCacheClientService.class);
    private RedisConnectionPool redisConnectionPool;

    public RedisDistributedMapCacheClientService(SparkConf sparkConf) {
        initContext(sparkConf);
    }

    private void initContext(SparkConf sparkConf)  {
        PropertyContext propertyContext = new RedisStandardProcessContext(sparkConf);
        onEnableRedisPool(propertyContext);
    }

    private void onEnableRedisPool(PropertyContext propertyContext) {
        this.redisConnectionPool = new RedisConnectionPoolService(propertyContext);
        this.redisConnectionPool.onEnable();
    }
    public void disable() {
        redisConnectionPool.onDisable();
    }

    @Override
    public <K, V> boolean putIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {

        return false;
    }

    @Override
    public <K, V> V getAndPutIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer, Deserializer<V> valueDeserializer) throws IOException {
        return null;
    }

    @Override
    public <K> boolean containsKey(K key, Serializer<K> keySerializer) throws IOException {
        return withConnection(redisConnection -> {
            final byte[] k = serialize(key, keySerializer);
            return redisConnection.exists(k);
        });
    }

    @Override
    public <K, V> void put(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        withConnection(redisConnection -> {
            final Tuple<byte[], byte[]> kv = serialize(key, value, keySerializer, valueSerializer);
            redisConnection.rPush(kv.getKey(), kv.getValue());
            return null;
        });
    }

    @Override
    public <K, V> void set(K key, V value, Long exp, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        withConnection(redisConnection -> {
            final Tuple<byte[], byte[]> kv = serialize(key, value, keySerializer, valueSerializer);
            redisConnection.set(kv.getKey(), kv.getValue(), Expiration.seconds(exp), RedisStringCommands.SetOption.upsert());
            return null;
        });
    }

    @Override
    public <K, V> V get(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) {
        return withConnection(redisConnection -> {
            final byte[] k = serialize(key, keySerializer);
            final byte[] v = redisConnection.get(k);
            return valueDeserializer.deserialize(v);
        });
    }

    @Override
    public <K, V> List<V> getList(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) {
        return withConnection(redisConnection -> {
            final byte[] k = serialize(key, keySerializer);
            List<byte[]> vs = redisConnection.lRange(k, 0, -1);
            List<V> res = new ArrayList<>();
            assert vs != null;
            if (!vs.isEmpty()) {
                vs.forEach(v -> {
                    V v1 = valueDeserializer.deserialize(v);
                    res.add(v1);
                });
            }
            return res;
        });
    }

    @Override
    public <K> boolean remove(K key, Serializer<K> serializer) {
        return withConnection(redisConnection -> {
            final byte[] k = serialize(key, serializer);
            final long nRemoved = redisConnection.del(k);
            return nRemoved > 0;
        });
    }

    @Override
    public long removeByPattern(String regex) {
        return withConnection(redisConnection -> {
            long deletedCount = 0;
            final List<byte[]> batchKeys = new ArrayList<>();

            final Cursor<byte[]> cursor = redisConnection.scan(ScanOptions.scanOptions().count(100).match(regex).build());
            while (cursor.hasNext()) {
                batchKeys.add(cursor.next());

                if (batchKeys.size() == 1000) {
                    deletedCount += redisConnection.del(getKeys(batchKeys));
                    batchKeys.clear();
                }
            }

            if (!batchKeys.isEmpty()) {
                deletedCount += redisConnection.del(getKeys(batchKeys));
                batchKeys.clear();
            }

            return deletedCount;
        });
    }

    @Override
    public void xAdd(byte[] key, Map<byte[], byte[]> value) {
        withConnection(redisConnection -> {
            redisConnection.streamCommands().xAdd(StreamRecords.newRecord().in(key).ofMap(value),
                    RedisStreamCommands.XAddOptions.maxlen(1000));
            return null;
        });
    }

    @Override
    public <K, V> void publish(K channel, V message, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        withConnection(redisConnection -> {
            final Tuple<byte[], byte[]> kv = serialize(channel, message, keySerializer, valueSerializer);
            redisConnection.publish(kv.getKey(), kv.getValue());
            return null;
        });
    }


    private <K, V> Tuple<byte[], byte[]> serialize(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        keySerializer.serialize(key, out);
        final byte[] k = out.toByteArray();

        out.reset();

        valueSerializer.serialize(value, out);
        final byte[] v = out.toByteArray();

        return new Tuple<>(k, v);
    }

    private <K> byte[] serialize(final K key, final Serializer<K> keySerializer) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        keySerializer.serialize(key, out);
        return out.toByteArray();
    }

    private <T> T withConnection(final RedisAction<T> action) {
        RedisConnection redisConnection = null;
        try {
            redisConnection = redisConnectionPool.getConnection();
            return action.execute(redisConnection);
        } finally {
            if (redisConnection != null) {
                try {
                    redisConnection.close();
                } catch (Exception e) {
                    logger.warn("Error closing connection: " + e.getMessage(), e);
                }
            }
        }
    }

    private byte[][] getKeys(final List<byte[]> keys) {
        final byte[][] allKeysArray = new byte[keys.size()][];
        for (int i=0; i < keys.size(); i++) {
            allKeysArray[i] = keys.get(i);
        }
        return  allKeysArray;
    }
}
