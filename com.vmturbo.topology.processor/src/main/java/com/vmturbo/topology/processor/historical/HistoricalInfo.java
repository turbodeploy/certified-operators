package com.vmturbo.topology.processor.historical;

import java.util.Map;
import java.util.Set;
import com.google.common.collect.Maps;

/**
 * This class keeps information for the historical used and peak values of all the service
 * entities in the system.
 */
public class HistoricalInfo {

    private Map<Long, HistoricalServiceEntityInfo> oidToHistoricalSEInfo = null;

    public HistoricalInfo() {
        oidToHistoricalSEInfo = Maps.newHashMap();
    }

    // Exposes the overall object. The following methods are convenience methods for easier use
    public Map<Long, HistoricalServiceEntityInfo> getOidToHistoricalSEInfo() {
        return oidToHistoricalSEInfo;
    }

    public void setOidToHistoricalSEInfo(Map<Long, HistoricalServiceEntityInfo> oidToHistoricalSEInfo) {
        this.oidToHistoricalSEInfo = oidToHistoricalSEInfo;
    }

    public void clear() {
        oidToHistoricalSEInfo.clear();
    }

    public void put(long oid, HistoricalServiceEntityInfo histSeInfo) {
        oidToHistoricalSEInfo.put(oid, histSeInfo);
    }

    public HistoricalServiceEntityInfo get(long key) {
        return oidToHistoricalSEInfo.get(key);
    }

    public HistoricalServiceEntityInfo remove(Long key) {
        return oidToHistoricalSEInfo.remove(key);
    }

    public boolean containsKey(long key) {
        return oidToHistoricalSEInfo.containsKey(key);
    }

    public Set<Long> keySet() {
        return oidToHistoricalSEInfo.keySet();
    }

    public HistoricalServiceEntityInfo replace(Long key, HistoricalServiceEntityInfo newValue) {
        return oidToHistoricalSEInfo.replace(key, newValue);
    }
}
