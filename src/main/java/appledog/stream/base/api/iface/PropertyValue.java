package appledog.stream.base.api.iface;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public interface PropertyValue extends Serializable {
    String getValue();

    Integer asInteger();

    Long asLong();

    Float asFloat();

    Double asDouble();

    boolean isSet();

    boolean asBoolean();

    Long asTimePeriod(TimeUnit timeUnit);
}
