package com.minmq.common.protocol;

import java.util.List;

public class PartitionInfo {
    private int partitionId;
    private String leaderBrokerId;
    private List<String> replicaBrokerIds;

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public String getLeaderBrokerId() {
        return leaderBrokerId;
    }

    public void setLeaderBrokerId(String leaderBrokerId) {
        this.leaderBrokerId = leaderBrokerId;
    }

    public List<String> getReplicaBrokerIds() {
        return replicaBrokerIds;
    }

    public void setReplicaBrokerIds(List<String> replicaBrokerIds) {
        this.replicaBrokerIds = replicaBrokerIds;
    }
}
