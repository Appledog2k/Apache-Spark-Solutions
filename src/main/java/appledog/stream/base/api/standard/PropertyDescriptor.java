package appledog.stream.base.api.standard;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class PropertyDescriptor implements Serializable {
    private final String name;

    private final String displayName;

    private final String description;

    private final String defaultValue;

    private final boolean required;

    private final List<AllowableValue> allowableValues;

    protected PropertyDescriptor(final Builder builder) {
        this.displayName = builder.displayName == null ? builder.name : builder.displayName;
        this.name = builder.name;
        this.description = builder.description;
        this.defaultValue = builder.defaultValue;
        this.required = builder.required;
        this.allowableValues = builder.allowableValues;
    }

    public static final class Builder {

        private String displayName = null;
        private String name = null;
        private String description = "";
        private String defaultValue = null;
        private boolean required = false;
        private List<AllowableValue> allowableValues = null;

        public Builder fromPropertyDescriptor(final PropertyDescriptor specDescriptor) {
            this.name = specDescriptor.name;
            this.displayName = specDescriptor.displayName;
            this.description = specDescriptor.description;
            this.defaultValue = specDescriptor.defaultValue;
            this.required = specDescriptor.required;
            this.allowableValues = specDescriptor.allowableValues == null ? null : new ArrayList<>(specDescriptor.allowableValues);
            return this;
        }

        public Builder displayName(final String displayName) {
            if (null != displayName) {
                this.displayName = displayName;
            }
            return this;
        }

        public Builder name(final String name) {
            if (null != name) {
                this.name = name;
            }
            return this;
        }

        public Builder description(final String description) {
            if (null != description) {
                this.description = description;
            }
            return this;
        }

        public Builder defaultValue(final String value) {
            if (null != value) {
                this.defaultValue = value;
            }
            return this;
        }

        public Builder required(final boolean required) {
            this.required = required;
            return this;
        }

        public Builder allowableValues(final Set<String> values) {
            if (null != values) {
                this.allowableValues = new ArrayList<>();

                for (final String value : values) {
                    this.allowableValues.add(new AllowableValue(value, value));
                }
            }
            return this;
        }

        public <E extends Enum<E>> Builder allowableValues(final E[] values) {
            if (null != values) {
                this.allowableValues = new ArrayList<>();
                for (final E value : values) {
                    allowableValues.add(new AllowableValue(value.name(), value.name()));
                }
            }
            return this;
        }


        public Builder allowableValues(final String... values) {
            if (null != values) {
                this.allowableValues = new ArrayList<>();
                for (final String value : values) {
                    allowableValues.add(new AllowableValue(value, value));
                }
            }
            return this;
        }

        public Builder allowableValues(final AllowableValue... values) {
            if (null != values) {
                this.allowableValues = Arrays.asList(values);
            }
            return this;
        }

        private boolean isValueAllowed(final String value) {
            if (allowableValues == null || value == null) {
                return true;
            }

            for (final AllowableValue allowableValue : allowableValues) {
                if (allowableValue.getValue().equals(value)) {
                    return true;
                }
            }

            return false;
        }

        public PropertyDescriptor build() {
            if (name == null) {
                throw new IllegalStateException("Must specify a name");
            }
            if (!isValueAllowed(defaultValue)) {
                throw new IllegalStateException("Default value [" + defaultValue + "] is not in the set of allowable values");
            }

            return new PropertyDescriptor(this);
        }
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + displayName + "]";
    }
}
