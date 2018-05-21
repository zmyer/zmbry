package com.github.zmbry.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * @author zifeng
 *
 */
public class DiskSpaceAllocator {
    private static final Logger logger = LoggerFactory.getLogger(DiskSpaceAllocator.class);
    private static final String RESERVE_FILE_PREFIX = "reserve_";
    private static final String FILE_SZIE_DIR_PREFIX = "reserve_size_";
    private static final FileFilter RESERVE_FILE_FILTER = pathname -> pathname.isFile() && pathname.getName()
            .startsWith(RESERVE_FILE_PREFIX);
    private static final FileFilter FILE_SIZE_DIR_FILTER = pathname -> pathname.isDirectory() &&
            getFileSizeForDirName(pathname.getName()) != null;
    private final boolean enablePooling;
    private final File reserveDir;
    private final long requiredSwapSegmentsPerSize;
    private final ReserveFileMap mReserveFileMap = new ReserveFileMap();
    private PoolState mPoolState = PoolState.NOT_INVENTORIED;
    private Exception inventoryException = null;

    public DiskSpaceAllocator(final boolean enablePooling, final File reserveDir, final long
            requiredSwapSegmentsPerSize) {
        this.enablePooling = enablePooling;
        this.reserveDir = reserveDir;
        this.requiredSwapSegmentsPerSize = requiredSwapSegmentsPerSize;
        try {
            if (enablePooling) {
                prepareDirectory(reserveDir);
                inventoryExistingReserveFiles();
                mPoolState = PoolState.INVENTORIED;
            }
        } catch (Exception e) {
            inventoryException = e;
            logger.error("Could not inventory preexisting reserve directory", e);
        }
    }

    private void inventoryExistingReserveFiles() throws IOException {
        File[] fileSizeDirs = reserveDir.listFiles(FILE_SIZE_DIR_FILTER);
        if (fileSizeDirs == null) {
            throw new IOException("Error while listing directories in " + reserveDir.getAbsolutePath());
        }
        for (File fileSizeDir : fileSizeDirs) {
            long sizeInBytes = getFileSizeForDirName(fileSizeDir.getName());
            File[] reserveFileForSize = fileSizeDir.listFiles(RESERVE_FILE_FILTER);
            if (reserveFileForSize == null) {
                throw new IOException("Error while listing files in " + fileSizeDir.getAbsolutePath());
            }
            for (File reserveFile : reserveFileForSize) {
                mReserveFileMap.add(sizeInBytes, reserveFile);
            }
        }
    }

    private static File prepareDirectory(final File reserveDir) throws IOException {
        if (!reserveDir.exists()) {
            reserveDir.mkdir();
        }
        if (!reserveDir.isDirectory()) {
            throw new IOException(reserveDir.getAbsolutePath() + " is not a directory or could not be created");
        }
        return reserveDir;
    }

    private static Long getFileSizeForDirName(final String name) {
        Long sizeInBytes = null;
        if (name.startsWith(FILE_SZIE_DIR_PREFIX)) {
            String sizeString = name.substring(FILE_SZIE_DIR_PREFIX.length());
            sizeInBytes = Long.parseLong(sizeString);
        }
        return sizeInBytes;
    }

    public void initializePool(final List<DiskSpaceRequirements> requirementsList) {

    }

    private static class ReserveFileMap {
        private final ConcurrentHashMap<Long, Queue<File>> internalMap = new ConcurrentHashMap<Long, Queue<File>>();

        void add(long sizeInBytes, File reserveFile) {
            internalMap.computeIfAbsent(sizeInBytes, key -> new ConcurrentLinkedDeque<>()).add(reserveFile);
        }

        File remove(long sizeInBytes) {
            File reserveFile = null;
            Queue<File> reserveFileForSize = internalMap.get(sizeInBytes);
            if (reserveFileForSize != null && reserveFileForSize.size() != 0) {
                reserveFile = reserveFileForSize.remove();
            }
            return reserveFile;
        }

        int getCount(long sizeInBytes) {
            Queue<File> reserveFilesForSize = internalMap.get(sizeInBytes);
            return reserveFilesForSize != null ? reserveFilesForSize.size() : 0;
        }

        Set<Long> getFileSizeSet() {
            return internalMap.keySet();
        }
    }

    private enum PoolState {
        /**
         * If the pool is not yet created/inventoried or failed initialization.
         */
        NOT_INVENTORIED,

        /**
         * If the pool has been inventoried but not initialized to match new {@link }.
         */
        INVENTORIED,

        /**
         * If the pool was successfully initialized.
         */
        INITIALIZED
    }
}
