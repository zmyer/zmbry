package com.github.zmbry.replication;

import com.github.zmbry.clustermap.ReplicaId;
import com.github.zmbry.network.Port;
import com.github.zmbry.store.FindToken;
import com.github.zmbry.store.Store;
import com.github.zmbry.utils.Time;

/**
 * @author zifeng
 *
 */
public class RemoteReplicaInfo {
    //远程副本id
    private final ReplicaId replicaId;
    //本地副本id
    private final ReplicaId localReplicaId;
    //锁对象
    private final Object lock = new Object();
    //本地存储对象
    private final Store localStore;
    private final Port port;
    private final Time time;
    // tracks the point up to which a node is in sync with a remote replica
    private final long tokenPersistIntervalInMs;

    // The latest known token
    private FindToken currentToken = null;
    // The token that will be safe to persist eventually
    private FindToken candidateTokenToPersist = null;
    // The time at which the candidate token is set
    private long timeCandidateSetInMs;
    // The token that is known to be safe to persist.
    private FindToken tokenSafeToPersist = null;
    private long totalBytesReadFromLocalStore;
    private long localLagFromRemoteStore = -1;

    RemoteReplicaInfo(ReplicaId replicaId, ReplicaId localReplicaId, Store localStore, FindToken token,
            long tokenPersistIntervalInMs, Time time, Port port) {
        this.replicaId = replicaId;
        this.localReplicaId = localReplicaId;
        this.totalBytesReadFromLocalStore = 0;
        this.localStore = localStore;
        this.time = time;
        this.port = port;
        this.tokenPersistIntervalInMs = tokenPersistIntervalInMs;
        initializeTokens(token);
    }

    ReplicaId getReplicaId() {
        return replicaId;
    }

    ReplicaId getLocalReplicaId() {
        return localReplicaId;
    }

    Store getLocalStore() {
        return localStore;
    }

    Port getPort() {
        return this.port;
    }

    long getRemoteLagFromLocalInBytes() {
        if (localStore != null) {
            return this.localStore.getSizeInBytes() - this.totalBytesReadFromLocalStore;
        } else {
            return 0;
        }
    }

    long getLocalLagFromRemoteInBytes() {
        return localLagFromRemoteStore;
    }

    FindToken getToken() {
        synchronized (lock) {
            return currentToken;
        }
    }

    void setTotalBytesReadFromLocalStore(long totalBytesReadFromLocalStore) {
        this.totalBytesReadFromLocalStore = totalBytesReadFromLocalStore;
    }

    void setLocalLagFromRemoteInBytes(long localLagFromRemoteStore) {
        this.localLagFromRemoteStore = localLagFromRemoteStore;
    }

    long getTotalBytesReadFromLocalStore() {
        return this.totalBytesReadFromLocalStore;
    }

    void setToken(FindToken token) {
        // reference assignment is atomic in java but we want to be completely safe. performance is
        // not important here
        synchronized (lock) {
            this.currentToken = token;
        }
    }

    void initializeTokens(FindToken token) {
        synchronized (lock) {
            this.currentToken = token;
            this.candidateTokenToPersist = token;
            this.tokenSafeToPersist = token;
            this.timeCandidateSetInMs = time.milliseconds();
        }
    }

    /**
     * get the token to persist. Returns either the candidate token if enough time has passed since it was
     * set, or the last token again.
     */
    FindToken getTokenToPersist() {
        synchronized (lock) {
            if (time.milliseconds() - timeCandidateSetInMs > tokenPersistIntervalInMs) {
                // candidateTokenToPersist is now safe to be persisted.
                tokenSafeToPersist = candidateTokenToPersist;
            }
            return tokenSafeToPersist;
        }
    }

    void onTokenPersisted() {
        synchronized (lock) {
      /* Only update the candidate token if it qualified as the token safe to be persisted in the previous get call.
       * If not, keep it as it is.
       */
            if (tokenSafeToPersist == candidateTokenToPersist) {
                candidateTokenToPersist = currentToken;
                timeCandidateSetInMs = time.milliseconds();
            }
        }
    }

    @Override
    public String toString() {
        return replicaId.toString();
    }
}
