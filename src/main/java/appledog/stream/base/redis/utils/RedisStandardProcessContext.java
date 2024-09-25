package appledog.stream.base.redis.utils;

import appledog.stream.base.api.standard.StandardParserConfig;
import appledog.stream.base.api.standard.StandardProcessContext;
import org.apache.spark.SparkConf;

public class RedisStandardProcessContext extends StandardProcessContext {

    public RedisStandardProcessContext(SparkConf sparkConf) {
        setAllProperties(StandardParserConfig.parserRedis(sparkConf));
    }
}
