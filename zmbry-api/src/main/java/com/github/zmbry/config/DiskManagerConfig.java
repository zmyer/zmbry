package com.github.zmbry.config;

/**
 * @author zifeng
 *
 */
public class DiskManagerConfig {

    /**
     * The name of the folder where the reserve disk segment pool will be.
     * A directory with this name will be created at the root of each disk.
     */
    @Config("disk.manager.reserve.file.dir.name")
    @Default("reserve-pool")
    public final String diskManagerReserveFileDirName;

    @Config("disk.manager.required.swap.segments.per.size")
    @Default("1")
    public final int diskManagerRequiredSwapSegmentsPerSize;

    @Config("disk.manager.enable.segment.pooling")
    @Default("false")
    public final boolean diskManagerEnableSegmentPooling;

    public DiskManagerConfig(VerifiableProperties verifiableProperties) {
        diskManagerReserveFileDirName =
                verifiableProperties.getString("disk.manager.reserve.file.dir.name", "reserve-pool");
        diskManagerRequiredSwapSegmentsPerSize =
                verifiableProperties.getIntInRange("disk.manager.required.swap.segments.per.size", 1, 0, 1000);
        diskManagerEnableSegmentPooling = verifiableProperties.getBoolean("disk.manager.enable.segment.pooling", false);
    }

}
