package appledog.stream.base.api.iface;

import appledog.stream.base.api.standard.PropertyDescriptor;

import java.io.Serializable;
import java.util.Map;

public interface PropertyContext extends Serializable {
    PropertyValue getProperty(PropertyDescriptor descriptor);

    Map<PropertyDescriptor, String> getProperties();

    Map<String, String> getAllProperties();

    void setAllProperties(Map<PropertyDescriptor, String> properties);
}
