package com.github.zmbry.store;

import com.github.zmbry.server.StatsSnapshot;
import javafx.util.Pair;

/**
 * @author zifeng
 *
 */
public interface StoreStats {
    Pair<Long, Long> getValue(TimeRange timeRange);

    StatsSnapshot getStatsSnapshot(long referenceTimeInMs);
}
