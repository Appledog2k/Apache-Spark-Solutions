package appledog.stream.base.api.standard;

import appledog.stream.base.database.utils.DatabaseUtils;
import appledog.stream.base.redis.utils.RedisUtils;
import appledog.stream.utils.Security;
import appledog.stream.utils.StringConstants;
import org.apache.spark.SparkConf;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import redis.clients.jedis.Protocol;

import javax.crypto.NoSuchPaddingException;
import java.io.Serializable;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StandardParserConfig implements Serializable {
    public static Map<PropertyDescriptor, String> parserRedis(SparkConf conf) {

        Map<PropertyDescriptor, String> properties = new HashMap<>();
        String redisMode = conf.get("spark.redis.mode", "Standalone");
        String connectionString = conf.get("spark.redis.host", Protocol.DEFAULT_HOST);
        String communicationTimeOut = conf.get("spark.redis.communicationTimeOut", "10 seconds");
        String clusterMaxRedirects = conf.get("spark.redis.clusterMaxRedirects", "5");
        String sentinelMaster = conf.get("spark.redis.sentinel.master", "masters");
        String password = conf.get("spark.redis.auth", null);
        String poolMaxTotal = conf.get("spark.redis.poolMaxTotal", "30");
        String pollMaxIdle = conf.get("spark.redis.pollMaxIdle", "30");
        String pollMinIdle = conf.get("spark.redis.pollMinIdle", "0");
        String poolBlockWhenExhausted = conf.get("spark.redis.poolBlockWhenExhausted", "true");
        String poolMaxWaitTime = conf.get("spark.redis.poolMaxWaitTime", "10 seconds");
        String poolMinEvictableIdleTime = conf.get("spark.redis.poolMinEvictableIdleTime", "60 seconds");
        String poolTimeBetweenEvictionRuns = conf.get("spark.redis.poolTimeBetweenEvictionRuns", "30 seconds");
        String poolNumTestsPerEvictionRun = conf.get("spark.redis.poolNumTestsPerEvictionRun", "-1");
        String poolTestOnCreate = conf.get("spark.redis.poolTestOnCreate", StringConstants.FALSE);
        String poolTestOnBorrow = conf.get("spark.redis.poolTestOnBorrow", StringConstants.FALSE);
        String poolTestOnReturn = conf.get("spark.redis.poolTestOnReturn", StringConstants.FALSE);
        String poolTestWhileIdle = conf.get("spark.redis.poolTestWhileIdle", StringConstants.FALSE);

        properties.put(RedisUtils.REDIS_MODE, redisMode);
        properties.put(RedisUtils.CONNECTION_STRING, connectionString);
        properties.put(RedisUtils.COMMUNICATION_TIMEOUT, communicationTimeOut);
        properties.put(RedisUtils.CLUSTER_MAX_REDIRECTS, clusterMaxRedirects);
        properties.put(RedisUtils.SENTINEL_MASTER, sentinelMaster);
        properties.put(RedisUtils.PASSWORD, password);
        properties.put(RedisUtils.POOL_MAX_TOTAL, poolMaxTotal);
        properties.put(RedisUtils.POOL_MAX_IDLE, pollMaxIdle);
        properties.put(RedisUtils.POOL_MIN_IDLE, pollMinIdle);
        properties.put(RedisUtils.POOL_BLOCK_WHEN_EXHAUSTED, poolBlockWhenExhausted);
        properties.put(RedisUtils.POOL_MAX_WAIT_TIME, poolMaxWaitTime);
        properties.put(RedisUtils.POOL_MIN_EVICTABLE_IDLE_TIME, poolMinEvictableIdleTime);
        properties.put(RedisUtils.POOL_TIME_BETWEEN_EVICTION_RUNS, poolTimeBetweenEvictionRuns);
        properties.put(RedisUtils.POOL_NUM_TESTS_PER_EVICTION_RUN, poolNumTestsPerEvictionRun);
        properties.put(RedisUtils.POOL_TEST_ON_CREATE, poolTestOnCreate);
        properties.put(RedisUtils.POOL_TEST_ON_BORROW, poolTestOnBorrow);
        properties.put(RedisUtils.POOL_TEST_ON_RETURN, poolTestOnReturn);
        properties.put(RedisUtils.POOL_TEST_WHILE_IDLE, poolTestWhileIdle);

        return properties;
    }

    public static Map<PropertyDescriptor, String> parserDb(Properties properties) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException {
        Map<PropertyDescriptor, String> mapProps = new HashMap<>();

        String connectionString = properties.getProperty("dataSource.uri");
        String user = properties.getProperty("dataSource.user");
        String password = (Security.getInstance().decrypt(properties.getProperty("dataSource.password")));

        String minimumIdle = properties.getProperty("dataSource.minimum.idle");
        String poolSize = properties.getProperty("dataSource.pool.size");
        String cachePrepStmts = properties.getProperty("dataSource.cachePrepStmts");
        String prepStmtCacheSize = properties.getProperty("dataSource.prepStmtCacheSize");
        String prepStmtCacheSqlLimit = properties.getProperty("dataSource.prepStmtCacheSqlLimit");
        String[] splitConnectionString = connectionString.split(":");
        mapProps.put(DatabaseUtils.DATABASE_TYPE,splitConnectionString[1]);
        mapProps.put(DatabaseUtils.DRIVER, "oracle.jdbc.driver.OracleDriver");
        mapProps.put(DatabaseUtils.CONNECTION_STRING, connectionString);
        mapProps.put(DatabaseUtils.USER, user);
        mapProps.put(DatabaseUtils.PASSWORD, password);
        mapProps.put(DatabaseUtils.MINIMUM_IDLE, minimumIdle);
        mapProps.put(DatabaseUtils.POOL_SIZE, poolSize);
        mapProps.put(DatabaseUtils.CACHE_PREP_STMTS, cachePrepStmts);
        mapProps.put(DatabaseUtils.PREP_STMTS_CACHE_SIZE, prepStmtCacheSize);
        mapProps.put(DatabaseUtils.PREP_STMTS_CACHE_SQL_LIMIT, prepStmtCacheSqlLimit);
        return mapProps;
    }
}
