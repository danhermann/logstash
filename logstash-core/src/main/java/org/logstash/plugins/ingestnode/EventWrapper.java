package org.logstash.plugins.ingestnode;

import com.google.common.annotations.VisibleForTesting;
import org.logstash.Event;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class EventWrapper implements Map<String, Object> {

    private Event e;
    //private boolean deepCopy;

    EventWrapper(Event e) {
        this.e = e;
    }

    public Event getEvent() {
        return e;
    }

    @Override
    public int size() {
        return e.getData().size();
    }

    @Override
    public boolean isEmpty() {
        return e.getData().isEmpty();
    }

    @Override
    public boolean containsKey(final Object key) {
        return e.getUnconvertedField((String)key) != null;
    }

    @Override
    public boolean containsValue(final Object value) {
        return e.getData().containsValue(value);
    }

    @Override
    public Object get(final Object key) {
        return e.getField((String)key);
    }

    @Override
    public Object put(final String key, final Object value) {
        //if (deepCopy) {
        //    return setDeepCopy(key, value);
        //} else if (value instanceof Map) {
        //    deepCopy();
        //    return setDeepCopy(key, value);
        //} else {
            e.setField(key, value);
            return value;
        //}
    }

    /*
    private void deepCopy() {
        deepCopy = true;
    }

    @VisibleForTesting
    boolean isDeepCopy() {
        return deepCopy;
    }

    private Object setDeepCopy(final String key, final Object value) {
        return null;
    }
    */

    @Override
    public Object remove(final Object key) {
        return e.remove((String)key);
    }

    @Override
    public void putAll(final Map<? extends String, ?> m) {
        throw new NotImplementedException();
    }

    @Override
    public void clear() {
        throw new NotImplementedException();
    }

    @Override
    public Set<String> keySet() {
        return e.getData().keySet();
    }

    @Override
    public Collection<Object> values() {
        return e.getData().values();
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return e.getData().entrySet();
    }
}
