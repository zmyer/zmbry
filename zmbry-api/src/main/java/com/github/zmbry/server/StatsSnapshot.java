package com.github.zmbry.server;

import java.util.Map;

/**
 * @author zifeng
 *
 */
public class StatsSnapshot {
    private long value;
    private Map<String, StatsSnapshot> subMap;

    /**
     * Performs recursive aggregation of two {@link StatsSnapshot} and stores the result in the first one.
     * @param baseSnapshot one of the addends and where the result will be
     * @param newSnapshot the other addend to be added into the first {@link StatsSnapshot}
     */
    public static void aggregate(StatsSnapshot baseSnapshot, StatsSnapshot newSnapshot) {
        baseSnapshot.setValue(baseSnapshot.getValue() + newSnapshot.getValue());
        if (baseSnapshot.getSubMap() == null) {
            baseSnapshot.setSubMap(newSnapshot.getSubMap());
        } else if (newSnapshot.getSubMap() != null) {
            for (Map.Entry<String, StatsSnapshot> entry : newSnapshot.getSubMap().entrySet()) {
                if (!baseSnapshot.getSubMap().containsKey(entry.getKey())) {
                    baseSnapshot.getSubMap().put(entry.getKey(), new StatsSnapshot(0L, null));
                }
                aggregate(baseSnapshot.getSubMap().get(entry.getKey()), entry.getValue());
            }
        }
    }

    public StatsSnapshot(Long value, Map<String, StatsSnapshot> subMap) {
        this.value = value;
        this.subMap = subMap;
    }

    public StatsSnapshot() {
        // empty constructor for Jackson deserialization
    }

    public long getValue() {
        return value;
    }

    public Map<String, StatsSnapshot> getSubMap() {
        return subMap;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public void setSubMap(Map<String, StatsSnapshot> subMap) {
        this.subMap = subMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StatsSnapshot that = (StatsSnapshot) o;

        if (value != that.value) {
            return false;
        }
        return subMap != null ? subMap.equals(that.subMap) : that.subMap == null;
    }

    @Override
    public int hashCode() {
        int result = (int) (value ^ (value >>> 32));
        result = 31 * result + (subMap != null ? subMap.hashCode() : 0);
        return result;
    }
}
