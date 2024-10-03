package appledog.stream.base.api.standard;

import java.io.Serializable;
import java.util.Objects;

public class AllowableValue implements Serializable {
    private final String value;
    private final String displayName;
    private final String description;

    public AllowableValue(final String value) {
        this(value, value);
    }

    public AllowableValue(final String value, final String displayName) {
        this(value, displayName, null);
    }

    public AllowableValue(final String value, final String displayName, final String description) {
        this.value = Objects.requireNonNull(value);
        this.displayName = Objects.requireNonNull(displayName);
        this.description = description;
    }

    public String getValue() {
        return value;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj instanceof AllowableValue) {
            final AllowableValue other = (AllowableValue) obj;
            return (this.value.equals(other.getValue()));
        } else if (obj instanceof String) {
            return this.value.equals(obj);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return 123456789 + 26 * value.hashCode();
    }

    @Override
    public String toString() {
        return value;
    }
}
