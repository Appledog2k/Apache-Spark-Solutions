package appledog.stream.base.api.standard;

import appledog.stream.base.api.interfaces.PropertyContext;
import appledog.stream.base.api.interfaces.PropertyValue;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class StandardProcessContext implements PropertyContext {
    private final Map<PropertyDescriptor, String> properties = new HashMap<>();

    @Override
    public PropertyValue getProperty(PropertyDescriptor descriptor) {
        final String setPropertyValue = properties.get(descriptor);
        if (setPropertyValue != null) {
            return new StandardPropertyValue(setPropertyValue);
        }
        final String defaultValue = descriptor.getDefaultValue();
        return new StandardPropertyValue(defaultValue);
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return properties;
    }

    @Override
    public Map<String, String> getAllProperties() {
        final Map<String, String> propValueMap = new LinkedHashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : getProperties().entrySet()) {
            propValueMap.put(entry.getKey().getName(), entry.getValue());
        }
        return propValueMap;
    }

    @Override
    public void setAllProperties(Map<PropertyDescriptor, String> properties) {
        this.properties.putAll(properties);
    }
}
