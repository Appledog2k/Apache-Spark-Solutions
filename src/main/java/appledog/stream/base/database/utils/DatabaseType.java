package appledog.stream.base.database.utils;

public enum DatabaseType {

    ORACLE("oracle", "Oracle Database."),
    MYSQL("mysql", "MySQL Database."),
    POSTGRES("postgres", "Postgres Database."),
    SQLSERVER("sqlserver", "SQL Server Database.");

    private final String displayName;

    private final String description;

    DatabaseType(final String displayName, final String description) {
        this.displayName = displayName;
        this.description = description;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getDescription() {
        return description;
    }

    public static DatabaseType fromDisplayName(final String displayName) {
        for (DatabaseType databaseType : values()) {
            if (databaseType.getDisplayName().equals(displayName)) {
                return databaseType;
            }
        }

        throw new IllegalArgumentException("Unknown DatabaseType: " + displayName);
    }
}
