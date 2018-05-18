package com.github.zmbry.config;

/**
 * @author zifeng
 *
 */
public class ServerConfig {

    /**
     * The number of request handler threads used by the server to process requests
     */
    @Config("server.request.handler.num.of.threads")
    @Default("7")
    public final int serverRequestHandlerNumOfThreads;

    /**
     * The number of scheduler threads the server will use to perform background tasks (store, replication)
     */
    @Config("server.scheduler.num.of.threads")
    @Default("10")
    public final int serverSchedulerNumOfthreads;

    /**
     * The option to enable or disable publishing stats locally.
     */
    @Config("server.stats.publish.local.enabled")
    @Default("false")
    public final boolean serverStatsPublishLocalEnabled;

    /**
     * The option to enable or disable publishing stats via Health Reports
     */
    @Config("server.stats.publish.health.report.enabled")
    @Default("false")
    public final boolean serverStatsPublishHealthReportEnabled;

    /**
     * The frequency in mins at which cluster wide quota stats will be aggregated
     */
    @Config("server.quota.stats.aggregate.interval.in.minutes")
    @Default("60")
    public final long serverQuotaStatsAggregateIntervalInMinutes;

    public ServerConfig(VerifiableProperties verifiableProperties) {
        serverRequestHandlerNumOfThreads = verifiableProperties.getInt("server.request.handler.num.of.threads", 7);
        serverSchedulerNumOfthreads = verifiableProperties.getInt("server.scheduler.num.of.threads", 10);
        serverStatsPublishLocalEnabled = verifiableProperties.getBoolean("server.stats.publish.local.enabled", false);
        serverStatsPublishHealthReportEnabled =
                verifiableProperties.getBoolean("server.stats.publish.health.report.enabled", false);
        serverQuotaStatsAggregateIntervalInMinutes =
                verifiableProperties.getLong("server.quota.stats.aggregate.interval.in.minutes", 60);
    }

}
