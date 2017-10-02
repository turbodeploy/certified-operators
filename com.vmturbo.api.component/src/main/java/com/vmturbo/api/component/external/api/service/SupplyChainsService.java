package com.vmturbo.api.component.external.api.service;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcher;
import com.vmturbo.api.dto.SupplychainApiDTO;
import com.vmturbo.api.dto.input.SupplyChainStatsApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.SupplyChainDetailType;
import com.vmturbo.api.serviceinterfaces.ISupplyChainsService;

public class SupplyChainsService implements ISupplyChainsService {

    private final SupplyChainFetcher supplyChainFetcher;
    private final long liveTopologyContextId;
    private final GroupExpander groupExpander;

    SupplyChainsService(@Nonnull final SupplyChainFetcher supplyChainFetcher,
                        final long liveTopologyContextId, GroupExpander groupExpander) {
        this.liveTopologyContextId = liveTopologyContextId;
        this.supplyChainFetcher = supplyChainFetcher;
        this.groupExpander = groupExpander;
    }

    @Override
    public SupplychainApiDTO getSupplyChainByUuids(List<String> uuids,
                                                   List<String> entityTypes,
                                                   EnvironmentType environmentType,
                                                   SupplyChainDetailType supplyChainDetailType,
                                                   Boolean includeHealthSummary) throws Exception {
        if (uuids.isEmpty()) {
            throw new RuntimeException("UUIDs list is empty");
        }

        // expand groups and clusters in UUIDs
        List<String> expandedUuids = groupExpander.expandUuidList(uuids).stream()
                .map(l -> Long.toString(l))
                .collect(Collectors.toList());

        // request the supply chain for the expanded list
        return supplyChainFetcher.newBuilder()
                .topologyContextId(liveTopologyContextId)
                .seedUuid(expandedUuids)
                .entityTypes(entityTypes)
                .environmentType(environmentType)
                .supplyChainDetailType(supplyChainDetailType)
                .includeHealthSummary(includeHealthSummary)
                .fetch();
    }

    /**
     * Return the stats for a supplychain; expand the supplychain to SE's and use those for
     * the Stats query.
     *
     * TODO: Not yet implemented in XL, but we need to return and empty list so the UX functions
     *
     * @param supplyChainStatsApiInputDTO a description of the supplychain seed uuids
     *                                    and the stats query to execute over that supplychain
     * @return the list of stats request for each snapshot time in the result
     * @throws Exception
     */
    @Override
    public List<StatSnapshotApiDTO> getSupplyChainStats(
            final SupplyChainStatsApiInputDTO supplyChainStatsApiInputDTO) throws Exception {

        // return an empty list instead of an exception or else the UX will get stuck!
        List<StatSnapshotApiDTO> answer = Lists.newArrayList();
        return answer;
    }
}
