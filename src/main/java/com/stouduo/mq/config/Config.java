package com.stouduo.mq.config;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Config {
    public static final String APP_PROFILES_ACTIVE = "app.profiles.active";
    public static final String MQ_STORE_ROOT_PATH = "mq.store.root.path";
    public static final String MQ_STORE_PATH = "mq.store.path";
    public static final String CONSUMER_QUEUE_STORE_PATH = "consumer.queue.store.path";
    public static final String MQ_STORE_MAPPED_FILE_SIZE = "mq.store.mapped.file.size";
    public static final String CONSUMER_QUEUE_STORE_UNIT_COUNT = "consumer.queue.store.unit.count";
    public static final String MAPPED_FILE_FLUSH_DISK_INTERVAL = "mapped.file.flush.disk.interval";
    public static final String MAPPED_FILE_FLUSH_DISK_SIZE = "mapped.file.flush.disk.size";
    public static final String COUNT_PER_CONSUMER_QUEUE = "count.per.consumer.queue";
    public static final int CONSUMER_QUEUE_STORE_UNIT_SIZE = 12;
    public static int defaultStoreSize = 1024 * 1024 * 1024;
    public static int defaultFlushInterval = 1000;
    public static int defaultFlushSize = 1024 * 512;
    private static ConcurrentMap<String, String> configs = new ConcurrentHashMap<>();

    static {
        initConfig();
    }

    public static String rootPath = getOrDefaultValue(MQ_STORE_ROOT_PATH, System.getProperty("user.home") + File.separator + "alidata1" + File.separator + "race2018" + File.separator + "data");
    public static String consumerStorePath = getOrDefaultValue(CONSUMER_QUEUE_STORE_PATH, File.separator + "consumerqueue");
    public static String mqStorePath = getOrDefaultValue(MQ_STORE_PATH, File.separator + "mqstore");
    public static int mqStoreFileSize = Integer.parseInt(getOrDefaultValue(MQ_STORE_MAPPED_FILE_SIZE, defaultStoreSize + ""));
    public static int fileFlushInterval = Integer.parseInt(getOrDefaultValue(MAPPED_FILE_FLUSH_DISK_INTERVAL, defaultFlushInterval + ""));
    public static int fileFlushSize = Integer.parseInt(getOrDefaultValue(MAPPED_FILE_FLUSH_DISK_SIZE, defaultFlushSize + ""));
    public static int consumerStoreUnitCount = Integer.parseInt(getOrDefaultValue(CONSUMER_QUEUE_STORE_UNIT_COUNT, 300000 + ""));
    public static int countPerConsumerQueues = Integer.parseInt(getOrDefaultValue(COUNT_PER_CONSUMER_QUEUE, 20 + ""));

    public static void main(String[] args) {
        System.out.println(configs.toString());
    }

    private static void initConfig() {
        String profiles = System.getProperty(APP_PROFILES_ACTIVE, "");
        Properties properties = new Properties();
        try (InputStream inputStream = Config.class.getClassLoader().getResourceAsStream("app" + ("".equalsIgnoreCase(profiles) || profiles == null ? "" : ("_" + profiles)) + ".properties")) {
            properties.load(inputStream);
            configs.putAll(new HashMap(properties));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            configs.putAll(new HashMap(System.getProperties()));
        }
    }

    public static int getInt(String propKey) {
        return Integer.parseInt(get(propKey));
    }

    public static String get(String propKey) {
        return configs.get(propKey).toString();
    }

    public static boolean getBoolean(String propKey) {
        return Boolean.parseBoolean(get(propKey));
    }

    public static long getLong(String propKey) {
        return Long.parseLong(get(propKey));
    }

    public static float getFloat(String propKey) {
        return Float.parseFloat(get(propKey));
    }

    public static <T extends Enum<T>> T getEnum(Class<T> enumType, String propKey) {
        return Enum.valueOf(enumType, get(propKey));
    }

    public static <T> T getOrDefaultValue(String primaryKey, T defaultValue) {
        String val = configs.get(primaryKey);
        return val == null || "".equals(val) ? defaultValue : (T) val;
    }

}
