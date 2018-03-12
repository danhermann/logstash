package org.logstash.execution.inputs;

import org.logstash.execution.LsConfiguration;
import org.logstash.execution.PluginConfigSpec;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

// How do we want to implement core input plugin functionality? helper class, base class, etc?
public final class InputHelper {

    public static final PluginConfigSpec<Map<String, String>> ADD_FIELD_CONFIG =
            LsConfiguration.hashSetting("add_field");

    //public static final PluginConfigSpec<Codec> CODEC_CONFIG =
    //        LsConfiguration.codecSetting("codec");

    public static final PluginConfigSpec<Boolean> ENABLE_METRIC_CONFIG =
            LsConfiguration.booleanSetting("enable_metric");

    public static final PluginConfigSpec<String> ID_CONFIG =
            LsConfiguration.stringSetting("id");

    //public static final PluginConfigSpec<Array> TAGS_CONFIG =
    //        LsConfiguration.arraySetting("tags");

    public static final PluginConfigSpec<String> TYPE_CONFIG =
            LsConfiguration.stringSetting("type");

    public static Collection<PluginConfigSpec<?>> commonConfigSchema() {
        return Arrays.asList(ADD_FIELD_CONFIG, ENABLE_METRIC_CONFIG, /*CODEC_CONFIG,*/  ID_CONFIG,
                /*TAGS_CONFIG,*/ TYPE_CONFIG);
    }
}
