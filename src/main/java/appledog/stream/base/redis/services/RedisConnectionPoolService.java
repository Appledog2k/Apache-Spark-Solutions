package appledog.stream.base.redis.services;

import appledog.stream.base.api.interfaces.PropertyContext;
import appledog.stream.base.redis.interfaces.RedisConnectionPool;
import appledog.stream.base.redis.utils.RedisType;
import appledog.stream.base.redis.utils.RedisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;

public class RedisConnectionPoolService implements RedisConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(RedisConnectionPoolService.class);
    private PropertyContext propertyContext;
    private RedisType redisType;
    private transient JedisConnectionFactory connectionFactory;

    public RedisConnectionPoolService(PropertyContext propertyContext) {
        this.propertyContext = propertyContext;
    }

    @Override
    public void onEnable() {
        String redisMode = propertyContext.getProperty(RedisUtils.REDIS_MODE).getValue();
        this.redisType = RedisType.fromDisplayName(redisMode);
    }

    @Override
    public void onDisable() {
        if (connectionFactory != null) {
            connectionFactory.destroy();
            connectionFactory = null;
            redisType = null;
            propertyContext = null;
        }
    }

    @Override
    public RedisConnection getConnection() {
        if (connectionFactory == null) {
            synchronized (this) {
                connectionFactory = RedisUtils.createConnectionFactory(propertyContext, logger);
            }
        }
        return connectionFactory.getConnection();
    }
}
