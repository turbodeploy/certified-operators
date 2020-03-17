package com.vmturbo.api.component.external.api.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.repository.api.RepositoryListener;

/**
 * A gateway for magic scopes hard-coded in the UI.
 *
 * Magic scopes enter, real scopes leave. Any such scopes that can be converted to real IDs will
 * be handled here. Services that want to handle those scopes should feed them to the
 * {@link MagicScopeGateway#enter(String)} or {@link MagicScopeGateway#enter(List)} methods,
 * and work with the output of those methods instead of the original scope strings.
 */
public class MagicScopeGateway implements RepositoryListener  {

    private static Logger logger = LogManager.getLogger();

    /**
     * TODO (roman, Nov 26 2018): Once we have more scopes here, we should have
     * each scope UUID and the special handling for the scope in a separate class - a mini-framework!
     */
    @VisibleForTesting
    static final String ALL_ON_PREM_HOSTS = "_PE0v-YEUEee_hYfzgV9uYg";

    private final GroupServiceBlockingStub groupRpcService;

    private final GroupMapper groupMapper;

    private final long realtimeTopologyContextId;

    @GuardedBy("cacheLock")
    private final BiMap<String, MagicScopeMapping> magicToRealMap = HashBiMap.create();

    private final ReadWriteLock cacheLock = new ReentrantReadWriteLock();

    public MagicScopeGateway(@Nonnull final GroupMapper groupMapper,
                             @Nonnull final GroupServiceBlockingStub groupRpcService,
                             final long realtimeTopologyContextId) {
        this.groupMapper = Objects.requireNonNull(groupMapper);
        this.groupRpcService = Objects.requireNonNull(groupRpcService);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    /**
     * De-mystify an original scope passed in from the UI, in the case that it's a magic UUID
     * hard-coded on the UI side.
     *
     * @param originalScope A UUID passed in from the UI via the API.
     * @return If the original scope is a "magic" UUID, returns a real UUID that will work in the
     *         rest of the system. If not, returns the original scope.
     * @throws OperationFailedException If there is any error mapping the scope.
     */
    @Nonnull
    public String enter(@Nonnull final String originalScope) throws OperationFailedException {
        logger.trace("Scope {} entered the magic gateway.", originalScope);
        final MagicScopeMapping cachedMapping;
        cacheLock.readLock().lock();
        try {
            cachedMapping = magicToRealMap.computeIfPresent(originalScope, (k, existingMapping) -> {
                try {
                    final boolean stillValid = existingMapping.validityCheck().get();
                    if (stillValid) {
                        return existingMapping;
                    } else {
                        logger.info("Magic mapping for {} no longer valid.",
                            existingMapping.debugName());
                        return null;
                    }
                } catch (RuntimeException e) {
                    return null;
                }
            });
        } finally {
            cacheLock.readLock().unlock();
        }

        if (cachedMapping != null) {
            logger.debug("Found cached mapping {} for magic scope UUID {}",
                cachedMapping.realId(), originalScope);
            Metrics.MAPPINGS_COUNT.increment();
            return cachedMapping.realId();
        }

        cacheLock.writeLock().lock();
        try {
            // If multiple threads waited for the write-lock, the first one to enter will have
            // already computed the mapping and put it in the map. Check to avoid computing again.
            // Note: this is a super-corner case, since the most likely case is that other threads
            // will be waiting on the read-lock.
            final MagicScopeMapping alreadyComputedMapping = magicToRealMap.get(originalScope);
            if (alreadyComputedMapping != null) {
                Metrics.MAPPINGS_COUNT.increment();
                return alreadyComputedMapping.realId();
            }

            final Optional<MagicScopeMapping> computedMappingOpt = computeMapping(originalScope);
            computedMappingOpt.ifPresent(computedMapping ->
                magicToRealMap.put(originalScope, computedMapping));
            return computedMappingOpt
                .map(mapping -> {
                    Metrics.MAPPINGS_COUNT.increment();
                    return mapping.realId();
                })
                .orElse(originalScope);
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    /**
     * Bulk version of {@link MagicScopeGateway#enter(String)}.
     *
     * @param originalScopes A list of UUIDs, possibly containing magic UUIDs.
     * @return A list where magic UUIDs in the original scopes are replaced by IDs understandable
     *         by the rest of the system.
     * @throws OperationFailedException If there is any error mapping the scopes.
     */
    @Nonnull
    public List<String> enter(@Nonnull final List<String> originalScopes) throws OperationFailedException {
        final List<String> mappedScopes = new ArrayList<>(originalScopes.size());
        for (String originalScope : originalScopes) {
            mappedScopes.add(enter(originalScope));
        }
        return mappedScopes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onSourceTopologyAvailable(long topologyId, long topologyContextId) {
        if (topologyContextId != realtimeTopologyContextId) {
            return;
        }

        cacheLock.writeLock().lock();
        try {
            // Now that a new topology is available, the previous group is out of date.
            logger.debug("Received repository notification about topology available. Id: {}",
                topologyId);
            final MagicScopeMapping existingMapping = magicToRealMap.remove(ALL_ON_PREM_HOSTS);
            if (existingMapping != null) {
                logger.info("Removed existing mapping (id: {}) for magic \"all on-prem hosts\" UUID",
                    existingMapping.realId());
            }
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    @Nonnull
    private Optional<MagicScopeMapping> computeMapping(@Nonnull final String originalScope)
            throws OperationFailedException {
        if (originalScope.equals(ALL_ON_PREM_HOSTS)) {
            // For all on-prem hosts, we create a temporary group and pass back the ID of
            // that group.
            final GroupApiDTO allOnPremHostGroup = new GroupApiDTO();
            allOnPremHostGroup.setScope(Collections.singletonList(UuidMapper.UI_REAL_TIME_MARKET_STR));
            allOnPremHostGroup.setEnvironmentType(EnvironmentType.ONPREM);
            allOnPremHostGroup.setGroupType(UIEntityType.PHYSICAL_MACHINE.apiStr());
            allOnPremHostGroup.setTemporary(true);
            allOnPremHostGroup.setDisplayName("Magic Group: " + ALL_ON_PREM_HOSTS);
            try {
                logger.debug("Creating temp group for magic \"all on-prem hosts\" scope");
                final GroupDefinition tempGroup = groupMapper.toGroupDefinition(allOnPremHostGroup);

                final CreateGroupResponse resp = groupRpcService.createGroup(
                    CreateGroupRequest.newBuilder()
                        .setGroupDefinition(tempGroup)
                        .setOrigin(Origin.newBuilder()
                            .setSystem(Origin
                                    .System
                                    .newBuilder()
                                    .setDescription("Magic Group: " + ALL_ON_PREM_HOSTS)
                                    )
                            )
                        .build());
                final long groupOid = resp.getGroup().getId();
                logger.info("Created temp group {} for magic \"all on-prem hosts\" UUID", groupOid);
                return Optional.of(ImmutableMagicScopeMapping.builder()
                        .realId(Long.toString(groupOid))
                        // Note - right now this will make an RPC to the group component every time,
                        // even if there are concurrent validity checks. We don't want to cache
                        // the validity results because a temporary group can expire at any time,
                        // and the performance benefit of avoiding this call is not worth the
                        // extra complexity of tracking the lifetime of this group here.
                        .validityCheck(() -> groupRpcService.getGroup(GroupID.newBuilder()
                            .setId(groupOid)
                            .build()).hasGroup())
                        .debugName("all on-prem hosts")
                        .build());
            } catch (Exception e) {
                throw new OperationFailedException(e);
            }
        } else {
            return Optional.empty();
        }
    }

    @Value.Immutable
    interface MagicScopeMapping {
        String realId();
        String debugName();
        Supplier<Boolean> validityCheck();
    }

    private static class Metrics {

        private static final DataMetricCounter MAPPINGS_COUNT = DataMetricCounter.builder()
                .withName("api_magic_scope_gateway_mappings_count")
                .withHelp("Number of scopes translated to 'real' IDs by the magic scope gateway.")
                .build()
                .register();

    }
}
