package com.github.zmbry.config;

/**
 * @author zifeng
 *
 */
public class StatsManagerConfig {

    /**
     * The file path (including filename) to be used for publishing the stats.
     */
    @Config("stats.output.file.path")
    @Default("/tmp/stats_output.json")
    public final String outputFilePath;

    /**
     * The time period in seconds that configures how often stats are published.
     */
    @Config("stats.publish.period.in.secs")
    @Default("7200")
    public final long publishPeriodInSecs;

    /**
     * The upper bound for the initial delay in seconds before the first stats collection is triggered. The delay is a
     * random number b/w 0 (inclusive) and this number (exclusive). If no initial delay is desired, this can be set to 0.
     */
    @Config("stats.initial.delay.upper.bound.in.secs")
    @Default("600")
    public final int initialDelayUpperBoundInSecs;

    public StatsManagerConfig(VerifiableProperties verifiableProperties) {
        outputFilePath = verifiableProperties.getString("stats.output.file.path", "/tmp/stats_output.json");
        publishPeriodInSecs = verifiableProperties.getLongInRange("stats.publish.period.in.secs", 7200, 0,
                Long.MAX_VALUE);
        initialDelayUpperBoundInSecs =
                verifiableProperties.getIntInRange("stats.initial.delay.upper.bound.in.secs", 600, 0,
                        Integer.MAX_VALUE);
    }
}
