package org.logstash.execution;

public interface ConfigValueConverter<T> {
    T convertValue(String rawValue);
}
