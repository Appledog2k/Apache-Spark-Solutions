package appledog.stream.base.redis.utils;

public enum RedisType {
    STANDALONE("Standalone", "A single standalone Redis instance."),

    SENTINEL("Sentinel", "Redis Sentinel which provides high-availability."),

    CLUSTER("Cluster", "Clustered Redis which provides sharding and replication.");

    private final String displayName;

    private final String description;

    RedisType(final String displayName, final String description) {
        this.displayName = displayName;
        this.description = description;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getDescription() {
        return description;
    }

    public static RedisType fromDisplayName(final String displayName) {
        for (RedisType redisType : values()) {
            if (redisType.getDisplayName().equals(displayName)) {
                return redisType;
            }
        }

        throw new IllegalArgumentException("Unknown RedisType: " + displayName);
    }
}
