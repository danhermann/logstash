package org.logstash.execution;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

public final class PluginHelper {

    public static final PluginConfigSpec<Map<String, String>> ADD_FIELD_CONFIG =
            LsConfiguration.hashSetting("add_field");

    //public static final PluginConfigSpec<Array> ADD_TAG_CONFIG =
    //        LsConfiguration.arraySetting("add_tag");

    //public static final PluginConfigSpec<Codec> CODEC_CONFIG =
    //        LsConfiguration.codecSetting("codec");

    public static final PluginConfigSpec<Boolean> ENABLE_METRIC_CONFIG =
            LsConfiguration.booleanSetting("enable_metric");

    public static final PluginConfigSpec<String> ID_CONFIG =
            LsConfiguration.stringSetting("id");

    public static final PluginConfigSpec<Boolean> PERIODIC_FLUSH_CONFIG =
            LsConfiguration.booleanSetting("periodic_flush");

    //public static final PluginConfigSpec<Array> REMOVE_FIELD_CONFIG =
    //        LsConfiguration.arraySetting("remove_field");

    //public static final PluginConfigSpec<Array> REMOVE_TAG_CONFIG =
    //        LsConfiguration.arraySetting("remove_tag");

    //public static final PluginConfigSpec<Array> TAGS_CONFIG =
    //        LsConfiguration.arraySetting("tags");

    public static final PluginConfigSpec<String> TYPE_CONFIG =
            LsConfiguration.stringSetting("type");

    public static Collection<PluginConfigSpec<?>> commonInputOptions() {
        return Arrays.asList(ADD_FIELD_CONFIG, ENABLE_METRIC_CONFIG, /*CODEC_CONFIG,*/  ID_CONFIG,
                /*TAGS_CONFIG,*/ TYPE_CONFIG);
    }

    public static Collection<PluginConfigSpec<?>> commonOutputOptions() {
        return Arrays.asList(ENABLE_METRIC_CONFIG, /*CODEC_CONFIG,*/  ID_CONFIG);
    }

    public static Collection<PluginConfigSpec<?>> commonFilterOptions() {
        return Arrays.asList(ADD_FIELD_CONFIG, /*ADD_TAG_CONFIG,*/ ENABLE_METRIC_CONFIG, ID_CONFIG,
                PERIODIC_FLUSH_CONFIG /*, REMOVE_FIELD_CONFIG, REMOVE_TAG_CONFIG*/);
    }

}
