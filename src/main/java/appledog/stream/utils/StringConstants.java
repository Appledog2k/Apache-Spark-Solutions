package appledog.stream.utils;

import java.io.Serializable;

public class StringConstants implements Serializable {
    public static final String KAFKA_SERVER = "kafka.server";
    public static final String KAFKA_GROUP_ID = "kafka.groupId";
    public static final String KAFKA_AUTO_OFFSET_RESET = "kafka.auto.offset.reset";
    public static final String KAFKA_TOPICS_IN = "kafka.topics.in";
    public static final String KAFKA_TOPIC_NOTIFY = "kafka.topic.notify";
    public static final String KAFKA_SECURE = "kafka.secure";
    public static final String KAFKA_SECURITY_PROTOCOL = "kafka.security.protocol";
    public static final String KAFKA_SASL_MECHANISM = "kafka.sasl.mechanism";
    public static final String KAFKA_SERVICE_NAME = "kafka.service.name";
    public static final String KAFKA_USER = "kafka.user";
    public static final String KAFKA_PASSWORD = "kafka.password";
    public static final String KAFKA_CLIENT_ID = "kafka.clientId";

    public static final String REDIS_TOPIC_STREAM = "redis.topic.stream";
    public static final String REDIS_TOPIC_BATCH = "redis.topic.batch";
    public static final String REDIS_STREAM_OD_INPUT = "redis.stream.od.input";
    public static final String REDIS_STREAM_KEY = "redis.stream.key";
    public static final String REDIS_ENGINE_CHANNEL = "redis.engine.channel";
    public static final String REDIS_TOPIC_OUT = "redis.topic.out";
    public static final String REDIS_CONSUMER_GROUP = "redis.consumer.group";
    public static final String REDIS_CONSUMER_NAME = "redis.consumer.name";
    public static final String REDIS_CONSUMER_SIZE = "redis.consumer.size";
    public static final String REDIS_CHANNEL_DATA = "redis.channel.data";
    public static final String REDIS_TOPIC_LOG = "redis.topic.log";

    public static final String SPARK_APP_NAME = "spark.app.name";
    public static final String PAGING ="Paging";
    public static final String FORMAT_PAGING =" OFFSET 0 ROWS FETCH NEXT %d ROWS ONLY ";
    public static final String FALSE ="false";
    public static final String DATA = "Data";
    public static final String USER_DATA = "UserData";
    public static final String KAFKA_TOPIC = "KafkaTopicOut";
    public static final String VALUE_TS = "value_ts";
    public static final String ZILLA_CORRELATION_ID = "zilla:correlation-id";

    public static final String SELECT="Select";
    public static final String WHERE="Where";
    public static final String GROUP_BY ="GroupBy";
    public static final String HAVING="Having";
    public static final String ORDER_BY ="OrderBy";

    public static final String ON_DEMAND_INPUT ="ON_DEMAND_INPUT";
    public static final String ON_DEMAND_STREAM ="ON_DEMAND_STREAM";
    public static final String ON_DEMAND_BATCH ="ON_DEMAND_BATCH";

    public static final String DATA_SOURCE_URI = "dataSource.uri";
    public static final String DATA_SOURCE_USER = "dataSource.user";
    public static final String DATA_SOURCE_PASSWORD = "dataSource.password";
    public static final String DATA_SOURCE_DRIVER = "dataSource.driver";
    public static final String DATA_SOURCE_OFFSET_BATCH_SIZE = "dataSource.offset.batch.size";
    public static final String DATA_SOURCE_OFFSET_STREAM_SIZE ="dataSource.offset.stream.size";
    public static final String DATA_SOURCE_SELECT_BATCH_SIZE = "dataSource.select.batch.size";

    public static final String PR_OUTPUT_SERVICES = "";
    public static final String TABLE_NAME = "TableName";
    public static final String SCHEMA_NAME = "SchemaName";
    public static final String FILE_ARRAY = "FileArray";
    public static final String FILE_NAME = "Filename";
    public static final String PATH_NAME = "Pathname";
    public static final String USER = "User";
    public static final String USER_HEADER = "UserHeader";
    public static final String FIELD_LIST = "FieldList";
    public static final String MY_ACTION = "MyAction";
    public static final String ENGINE = "Engine";
    public static final String SQL_TYPE ="SqlType";
}
