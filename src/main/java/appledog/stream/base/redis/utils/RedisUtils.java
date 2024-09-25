package appledog.stream.base.redis.utils;

import appledog.stream.base.api.iface.PropertyContext;
import appledog.stream.base.api.standard.AllowableValue;
import appledog.stream.base.api.standard.PropertyDescriptor;
import org.slf4j.Logger;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import redis.clients.jedis.JedisPoolConfig;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RedisUtils implements Serializable {
    private static final String TRUE = "true";
    private static final String FALSE = "false";
    public static final AllowableValue REDIS_MODE_STANDALONE = new AllowableValue(RedisType.STANDALONE.getDisplayName(), RedisType.STANDALONE.getDisplayName(), RedisType.STANDALONE.getDescription());
    public static final AllowableValue REDIS_MODE_SENTINEL = new AllowableValue(RedisType.SENTINEL.getDisplayName(), RedisType.SENTINEL.getDisplayName(), RedisType.SENTINEL.getDescription());
    public static final AllowableValue REDIS_MODE_CLUSTER = new AllowableValue(RedisType.CLUSTER.getDisplayName(), RedisType.CLUSTER.getDisplayName(), RedisType.CLUSTER.getDescription());

    public static final PropertyDescriptor REDIS_MODE = new PropertyDescriptor.Builder().name("Redis Mode").displayName("Redis Mode").description("The type of Redis being communicated with - standalone, sentinel, or clustered.").allowableValues(REDIS_MODE_STANDALONE, REDIS_MODE_SENTINEL, REDIS_MODE_CLUSTER).defaultValue(REDIS_MODE_STANDALONE.getValue()).required(true).build();

    public static final PropertyDescriptor CONNECTION_STRING = new PropertyDescriptor.Builder().name("Connection String").displayName("Connection String").description("The connection string for Redis. In a standalone instance this value will be of the form hostname:port. " + "In a sentinel instance this value will be the comma-separated list of sentinels, such as host1:port1,host2:port2,host3:port3. " + "In a clustered instance this value will be the comma-separated list of cluster masters, such as host1:port,host2:port,host3:port.").required(true).build();

    public static final PropertyDescriptor DATABASE = new PropertyDescriptor.Builder().name("Database Index").displayName("Database Index").description("The database index to be used by connections created from this connection pool. " + "See the databases property in redis.conf, by default databases 0-15 will be available.").defaultValue("0").required(true).build();

    public static final PropertyDescriptor COMMUNICATION_TIMEOUT = new PropertyDescriptor.Builder().name("Communication Timeout").displayName("Communication Timeout").description("The timeout to use when attempting to communicate with Redis.").defaultValue("10 seconds").required(true).build();

    public static final PropertyDescriptor CLUSTER_MAX_REDIRECTS = new PropertyDescriptor.Builder().name("Cluster Max Redirects").displayName("Cluster Max Redirects").description("The maximum number of redirects that can be performed when clustered.").defaultValue("5").required(true).build();

    public static final PropertyDescriptor SENTINEL_MASTER = new PropertyDescriptor.Builder().name("Sentinel Master").displayName("Sentinel Master").description("The name of the sentinel master, require when Mode is set to Sentinel").build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder().name("Password").displayName("Password").description("The password used to authenticate to the Redis server. See the requirepass property in redis.conf.").build();

    public static final PropertyDescriptor POOL_MAX_TOTAL = new PropertyDescriptor.Builder().name("Pool - Max Total").displayName("Pool - Max Total").description("The maximum number of connections that can be allocated by the pool (checked out to clients, or idle awaiting checkout). " + "A negative value indicates that there is no limit.").defaultValue("8").required(true).build();

    public static final PropertyDescriptor POOL_MAX_IDLE = new PropertyDescriptor.Builder().name("Pool - Max Idle").displayName("Pool - Max Idle").description("The maximum number of idle connections that can be held in the pool, or a negative value if there is no limit.").defaultValue("8").required(true).build();

    public static final PropertyDescriptor POOL_MIN_IDLE = new PropertyDescriptor.Builder().name("Pool - Min Idle").displayName("Pool - Min Idle").description("The target for the minimum number of idle connections to maintain in the pool. If the configured value of Min Idle is " + "greater than the configured value for Max Idle, then the value of Max Idle will be used instead.").defaultValue("0").required(true).build();

    public static final PropertyDescriptor POOL_BLOCK_WHEN_EXHAUSTED = new PropertyDescriptor.Builder().name("Pool - Block When Exhausted").displayName("Pool - Block When Exhausted").description("Whether or not clients should block and wait when trying to obtain a connection from the pool when the pool has no available connections. " + "Setting this to false means an error will occur immediately when a client requests a connection and none are available.").allowableValues(TRUE, FALSE).defaultValue(TRUE).required(true).build();

    public static final PropertyDescriptor POOL_MAX_WAIT_TIME = new PropertyDescriptor.Builder().name("Pool - Max Wait Time").displayName("Pool - Max Wait Time").description("The amount of time to wait for an available connection when Block When Exhausted is set to true.").defaultValue("10 seconds").required(true).build();

    public static final PropertyDescriptor POOL_MIN_EVICTABLE_IDLE_TIME = new PropertyDescriptor.Builder().name("Pool - Min Evictable Idle Time").displayName("Pool - Min Evictable Idle Time").description("The minimum amount of time an object may sit idle in the pool before it is eligible for eviction.").defaultValue("60 seconds").required(true).build();

    public static final PropertyDescriptor POOL_TIME_BETWEEN_EVICTION_RUNS = new PropertyDescriptor.Builder().name("Pool - Time Between Eviction Runs").displayName("Pool - Time Between Eviction Runs").description("The amount of time between attempting to evict idle connections from the pool.").defaultValue("30 seconds").required(true).build();

    public static final PropertyDescriptor POOL_NUM_TESTS_PER_EVICTION_RUN = new PropertyDescriptor.Builder().name("Pool - Num Tests Per Eviction Run").displayName("Pool - Num Tests Per Eviction Run").description("The number of connections to tests per eviction attempt. A negative value indicates to test all connections.").defaultValue("-1").required(true).build();

    public static final PropertyDescriptor POOL_TEST_ON_CREATE = new PropertyDescriptor.Builder().name("Pool - Test On Create").displayName("Pool - Test On Create").description("Whether or not connections should be tested upon creation.").allowableValues(TRUE, FALSE).defaultValue(FALSE).required(true).build();

    public static final PropertyDescriptor POOL_TEST_ON_BORROW = new PropertyDescriptor.Builder().name("Pool - Test On Borrow").displayName("Pool - Test On Borrow").description("Whether or not connections should be tested upon borrowing from the pool.").allowableValues(TRUE, FALSE).defaultValue(FALSE).required(true).build();

    public static final PropertyDescriptor POOL_TEST_ON_RETURN = new PropertyDescriptor.Builder().name("Pool - Test On Return").displayName("Pool - Test On Return").description("Whether or not connections should be tested upon returning to the pool.").allowableValues(TRUE, FALSE).defaultValue(FALSE).required(true).build();

    public static final PropertyDescriptor POOL_TEST_WHILE_IDLE = new PropertyDescriptor.Builder().name("Pool - Test While Idle").displayName("Pool - Test While Idle").description("Whether or not connections should be tested while idle.").allowableValues(TRUE, FALSE).defaultValue(TRUE).required(true).build();

    public static JedisConnectionFactory createConnectionFactory(PropertyContext context, Logger logger) {
        final String redisMode = context.getProperty(RedisUtils.REDIS_MODE).getValue();
        final String connectionString = context.getProperty(RedisUtils.CONNECTION_STRING).getValue();
        final Integer dbIndex = context.getProperty(RedisUtils.DATABASE).asInteger();
        final String password = context.getProperty(RedisUtils.PASSWORD).getValue();
        final Integer timeout = context.getProperty(RedisUtils.COMMUNICATION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        JedisPoolConfig jedisPoolConfig = createJedisPoolConfig(context);

        final JedisClientConfiguration jedisClientConfiguration = JedisClientConfiguration.builder().connectTimeout(Duration.ofMillis(timeout)).readTimeout(Duration.ofMillis(timeout)).usePooling().poolConfig(jedisPoolConfig).build();

        JedisConnectionFactory connectionFactory;
        if (RedisUtils.REDIS_MODE_STANDALONE.getValue().equals(redisMode)) {
            final String[] hostAndPortSplit = connectionString.split(":");
            final String host = hostAndPortSplit[0].trim();
            final int port = Integer.parseInt(hostAndPortSplit[1].trim());
            final RedisStandaloneConfiguration redisStandaloneConfiguration = new RedisStandaloneConfiguration(host, port);
            enrichRedisConfiguration(redisStandaloneConfiguration, dbIndex, password);

            connectionFactory = new JedisConnectionFactory(redisStandaloneConfiguration, jedisClientConfiguration);
        } else if (RedisUtils.REDIS_MODE_SENTINEL.getValue().equals(redisMode)) {
            final String[] sentinels = connectionString.split(",");
            final String sentinelMaster = context.getProperty(RedisUtils.SENTINEL_MASTER).getValue();
            final RedisSentinelConfiguration sentinelConfiguration = new RedisSentinelConfiguration(sentinelMaster, new HashSet<>(getTrimmedValues(sentinels)));
            enrichRedisConfiguration(sentinelConfiguration, dbIndex, password);

            logger.info("Connecting to Redis in sentinel mode...");
            logger.info("Redis master = {}",sentinelMaster);

            for (final String sentinel : sentinels) {
                logger.info("Redis sentinel at {}", sentinel);
            }

            connectionFactory = new JedisConnectionFactory(sentinelConfiguration, jedisClientConfiguration);

        } else {
            final String[] clusterNodes = connectionString.split(",");
            final Integer maxRedirects = context.getProperty(RedisUtils.CLUSTER_MAX_REDIRECTS).asInteger();

            final RedisClusterConfiguration clusterConfiguration = new RedisClusterConfiguration(getTrimmedValues(clusterNodes));
            enrichRedisConfiguration(clusterConfiguration, dbIndex, password);
            clusterConfiguration.setMaxRedirects(maxRedirects);

            logger.info("Connecting to Redis in clustered mode...");
            for (final String clusterNode : clusterNodes) {
                logger.info("Redis cluster node at {}", clusterNode);
            }

            connectionFactory = new JedisConnectionFactory(clusterConfiguration, jedisClientConfiguration);
        }
        connectionFactory.afterPropertiesSet();
        return connectionFactory;
    }

    private static List<String> getTrimmedValues(final String[] values) {
        final List<String> trimmedValues = new ArrayList<>();
        for (final String value : values) {
            trimmedValues.add(value.trim());
        }
        return trimmedValues;
    }

    private static void enrichRedisConfiguration(final RedisConfiguration redisConfiguration, final Integer dbIndex, final String password) {
        if (redisConfiguration instanceof RedisConfiguration.WithDatabaseIndex) {
            ((RedisConfiguration.WithDatabaseIndex) redisConfiguration).setDatabase(dbIndex);
        }
        if (redisConfiguration instanceof RedisConfiguration.WithPassword) {
            ((RedisConfiguration.WithPassword) redisConfiguration).setPassword(RedisPassword.of(password));
        }
    }

    public static JedisPoolConfig createJedisPoolConfig(final PropertyContext context) {
        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(context.getProperty(RedisUtils.POOL_MAX_TOTAL).asInteger());
        poolConfig.setMaxIdle(context.getProperty(RedisUtils.POOL_MAX_IDLE).asInteger());
        poolConfig.setMinIdle(context.getProperty(RedisUtils.POOL_MIN_IDLE).asInteger());
        poolConfig.setBlockWhenExhausted(context.getProperty(RedisUtils.POOL_BLOCK_WHEN_EXHAUSTED).asBoolean());
        poolConfig.setMaxWait(Duration.ofMillis(1000));
        poolConfig.setMinEvictableIdleTime(Duration.ofMillis(60000));
        poolConfig.setTimeBetweenEvictionRuns(Duration.ofMillis(30000));
        poolConfig.setNumTestsPerEvictionRun(context.getProperty(RedisUtils.POOL_NUM_TESTS_PER_EVICTION_RUN).asInteger());
        poolConfig.setTestOnCreate(context.getProperty(RedisUtils.POOL_TEST_ON_CREATE).asBoolean());
        poolConfig.setTestOnBorrow(context.getProperty(RedisUtils.POOL_TEST_ON_BORROW).asBoolean());
        poolConfig.setTestOnReturn(context.getProperty(RedisUtils.POOL_TEST_ON_RETURN).asBoolean());
        poolConfig.setTestWhileIdle(context.getProperty(RedisUtils.POOL_TEST_WHILE_IDLE).asBoolean());
        return poolConfig;
    }
}
