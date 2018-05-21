package com.github.zmbry.store;

import java.util.List;

/**
 * @author zifeng
 *
 */
public interface CompactionPolicy {
    CompactionDetails getCompactionDetails(long totalCapacity, long usedCapacity, long segmentCapacity, long
            segemtntHeaderSize, List<String> logSegmentsNotInJournal, BlobStoreStats blobStoreStats);
}
