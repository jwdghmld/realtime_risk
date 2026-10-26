package com.ecommerce.realtime.config;

public final class RealtimeConfig {
    private static final String PLACEHOLDER = "此处自定义";

    public final String bootstrapServers;
    public final String groupId;
    public final String mysqlUrl;
    public final String mysqlUser;
    public final String mysqlPassword;
    public final String checkpointDir;

    private RealtimeConfig(String groupId, String checkpointDir) {
        this.bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS");
        this.groupId = groupId;
        this.mysqlUrl = "jdbc:mysql://" + env("MYSQL_HOST") + ":" + env("MYSQL_PORT")
                + "/" + env("REALTIME_MYSQL_DATABASE")
                + "?useUnicode=true&characterEncoding=" + env("MYSQL_CHARSET")
                + "&serverTimezone=" + env("MYSQL_TIMEZONE");
        this.mysqlUser = env("MYSQL_USER");
        this.mysqlPassword = env("MYSQL_PASSWORD");
        this.checkpointDir = checkpointDir;
    }

    public static RealtimeConfig orderRisk() {
        return new RealtimeConfig("order-risk", checkpointRoot() + "order-risk/");
    }

    public static RealtimeConfig userRisk() {
        return new RealtimeConfig("user-risk", checkpointRoot() + "user-risk/");
    }

    private static String checkpointRoot() {
        String root = env("FLINK_CHECKPOINT_ROOT");
        return root.endsWith("/") ? root : root + "/";
    }

    private static String env(String name) {
        return System.getenv().getOrDefault(name, PLACEHOLDER);
    }
}
