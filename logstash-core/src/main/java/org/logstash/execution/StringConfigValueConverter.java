package org.logstash.execution;

public class StringConfigValueConverter implements ConfigValueConverter<String> {
    @Override
    public String convertValue(String rawValue) {
        return rawValue;
    }
}
