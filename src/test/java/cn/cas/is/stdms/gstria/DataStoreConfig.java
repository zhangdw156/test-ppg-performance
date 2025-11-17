package cn.cas.is.stdms.gstria;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public enum DataStoreConfig {

    PPG("geomesa.ppg"),
    HBASE("geomesa.hbase");

    private static class ConfigHolder {
        private static final Config config = ConfigFactory.load();
    }

    private final Map<String, Object> properties;

    DataStoreConfig(String configPath) {
        Config subConfig = ConfigHolder.config.getConfig(configPath);
        Map<String, Object> flattenedMap =
                subConfig.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().unwrapped()));
        this.properties = Collections.unmodifiableMap(flattenedMap);
    }

    public Map<String, Object> toMap() {
        return new HashMap<>(this.properties);
    }
}
