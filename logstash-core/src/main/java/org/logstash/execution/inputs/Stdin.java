package org.logstash.execution.inputs;

import org.logstash.execution.Input;
import org.logstash.execution.PluginConfigSpec;
import org.logstash.execution.QueueWriter;

import java.util.Collection;

public class Stdin implements Input{

    @Override
    public void start(QueueWriter writer) {

    }

    @Override
    public void stop() {

    }

    @Override
    public void awaitStop() throws InterruptedException {

    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        return null;
    }
}
