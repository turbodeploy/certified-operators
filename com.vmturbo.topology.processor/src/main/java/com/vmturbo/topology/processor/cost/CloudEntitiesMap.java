package com.vmturbo.topology.processor.cost;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.stitching.StitchingContext;

/**
 *  Utility class for mapping cloud entity local id's to oid's assigned from the topology processor.
 */
public class CloudEntitiesMap implements Map<String, Long> {
    private static final Logger logger = LogManager.getLogger();

    private static final Set<EntityType> CLOUD_ENTITY_TYPES = ImmutableSet.of(
            EntityType.CLOUD_SERVICE,
            EntityType.COMPUTE_TIER,
            EntityType.STORAGE_TIER,
            EntityType.DATABASE_TIER,
            EntityType.DATABASE_SERVER_TIER,
            EntityType.REGION,
            EntityType.AVAILABILITY_ZONE,
            EntityType.RESERVED_INSTANCE,
            EntityType.BUSINESS_ACCOUNT,
            EntityType.SERVICE_PROVIDER
    );

    /**
     * String prefix for identifying fallback accounts.
     */
    private static final String FALLBACK_ACCOUNT_PREFIX = "Default account for target ";
    private static final Long FALLBACK_ACCOUNT_DEFAULT_OID = 0L; // used when no fallback account is found

    private final Map<String, Long> cloudEntityOidByLocalId = new HashMap<>();

    /**
     * Map to collect the IDs which are 'extra' in cloudEntityOidByLocalId.
     * We need this map as we format the key in cloudEntityOidByLocalId to match other naming convention
     * from various probes. like matching DB to DB costs in between Azure to Azure cost probes.
     * Negative side effect of formatting is we may loose some OIDs.
     * we want to keep them in this secondary map so it can used for lookup.
     */
    private final Map<String, Collection<String>> extraneousIdLookUps = new HashMap<>();

    /**
     * Create a new CloudEntitiesMap populated from the provided {@link StitchingContext}
     *
     * @param stitchingContext
     */
    public CloudEntitiesMap(StitchingContext stitchingContext,
                            Map<Long, SDKProbeType> probeTypesForTargetIds) {
        populateFrom(stitchingContext, probeTypesForTargetIds);
    }

    @Override
    public int hashCode() {
        return cloudEntityOidByLocalId.hashCode();
    }

    /**
     * Create a mapping of cloud entity local id's to oids, to facilitate later lookups by local id.
     * @param stitchingContext containing the cloud entities to include in the map.
     * @param probeTypesForTargetIds list of probeTypes indexed by their OIDs.
     * @return A mapping from cloud discovered entity local id -> stitching entity oid
     */
    private Map<String, Long> populateFrom(StitchingContext stitchingContext,
                                         Map<Long, SDKProbeType> probeTypesForTargetIds) {
        // build a map allowing easy translation from cloud service local id's to TP oids
        cloudEntityOidByLocalId.clear();
        CLOUD_ENTITY_TYPES.forEach(entityType -> {
            AtomicInteger counter = new AtomicInteger(0);
            stitchingContext.getEntitiesOfType(entityType).forEach(entity -> {
                cloudEntityOidByLocalId.put(entity.getLocalId(), entity.getOid());
                if (entityType == EntityType.DATABASE_TIER || entityType == EntityType.DATABASE_SERVER_TIER) {
                    // TODO: we are going to put data base tiers in here by display name too
                    // this is a workaround for a mapping issue, where the price tables refer to
                    // db tiers by "model" or "display name", while the discovery and billing probes
                    // use the UUID syntax. I don't know why we use the UUID syntax
                    // for DB profiles but not VM profiles (they seem like they should be more
                    // consistent with each other), which is something to be followed up on.
                    String databaseTierName = CloudCostUtils.databaseTierLocalNameToId(entity.getDisplayName(),
                            probeTypesForTargetIds.get(entity.getTargetId()));
                    if (cloudEntityOidByLocalId.containsKey(databaseTierName)) {
                        String databaseTierFullName = CloudCostUtils.databaseTierNameToFullId(
                                entity.getDisplayName(),
                                probeTypesForTargetIds.get(entity.getTargetId()));
                        extraneousIdLookUps.compute(databaseTierName, (key, currentList) -> {
                            currentList = currentList == null ? new ArrayList<>() : currentList;
                            currentList.add(databaseTierFullName);
                            return currentList;
                        });
                    } else {
                        cloudEntityOidByLocalId.put(databaseTierName, entity.getOid());
                    }
                }
                counter.getAndIncrement();
            });
            logger.info("Added {} entities of type {}", counter.get(), entityType);
        });

        // also add the fallback accounts
        addFallbackAccounts(stitchingContext, cloudEntityOidByLocalId);

        return cloudEntityOidByLocalId;
    }

    /**
     * Find the fallback accounts for each target id that has one.
     *
     * The "fallback account" is the account to assign ownership to, in the event that a proper
     * owner cannot be found. We are using this because the ownership is "most likely" the fallback
     * account anyways, and it's presumably better to default to this relationship than to have no
     * owner relationship at all. Note that if a target doesn't discover business accounts, it will
     * not have a fallback account, and any entities needing ownership assignments will probably
     * remain with an account oid of 0.
     *
     * the ideal fallback account:
     *  - is not owned by any other account
     *  - owns other accounts
     *
     * In the event of multiple accounts for a given target satisfying both criteria, the
     * account that owns the most other accounts will be the winner. if there is still a
     * tie, then one will be chosen from amongst the tied.
     *
     * @param stitchingContext
     * @param cloudEntityOidByLocalId
     */
    private void addFallbackAccounts(StitchingContext stitchingContext, Map<String, Long> cloudEntityOidByLocalId) {
        // we will be iterating the list of accounts more than once, which is not the most
        // efficient way to do this. Let's replace this with a faster algorithm later, if fallback
        // accounts remain necessary.
        //
        Set<Long> ownedAccounts = new HashSet<>(); // keep a set of the owned accounts
        // # of accounts owned per account oid
        Map<Long, AtomicInteger> entityAccountsOwned = new HashMap<>();
        // set the default account to 0 owned and into the "owned" set, so it is easily surpassed in
        // tests of domination.
        entityAccountsOwned.put(FALLBACK_ACCOUNT_DEFAULT_OID, new AtomicInteger(0));
        ownedAccounts.add(FALLBACK_ACCOUNT_DEFAULT_OID);
        stitchingContext.getEntitiesOfType(EntityType.BUSINESS_ACCOUNT).forEach(entity -> {
            AtomicInteger numOwned = entityAccountsOwned.getOrDefault(entity.getOid(), new AtomicInteger(0));
            if (entity.getConnectedToByType().containsKey(ConnectionType.OWNS_CONNECTION)) {
                // add any owned entities to the "owned accounts" set
                entity.getConnectedToByType().get(ConnectionType.OWNS_CONNECTION).forEach(owned -> {
                    if (owned.getEntityType() == EntityType.BUSINESS_ACCOUNT) {
                        logger.debug("Account {} owned by {}", owned.getOid(), entity.getOid());
                        ownedAccounts.add(owned.getOid());
                        numOwned.getAndIncrement();
                    }
                });
            }
            entityAccountsOwned.put(entity.getOid(), numOwned); // save # accounts owned
        });
        // now, iterate the accounts again and pick the fallback accounts
        stitchingContext.getEntitiesOfType(EntityType.BUSINESS_ACCOUNT).forEach(entity -> {
            // for all targets discovering this account, see if this account can claim dominion over
            // the target.
            entity.getDiscoveringTargetIds().forEach(targetId -> {
                // If our current account owns more or equal accounts than the current fallback
                // account for this target does, or the current fallback account itself is known
                // to be owned, then take over the fallback position.
                Long currentFallbackAccount = getFallbackAccountOid(targetId);
                if (entityAccountsOwned.get(entity.getOid()).intValue()
                        >= entityAccountsOwned.get(currentFallbackAccount).intValue()
                        || ownedAccounts.contains(currentFallbackAccount)) {
                    logger.debug("Setting fallback account for target {} to {} (owns {})",
                            targetId, entity.getOid(), entityAccountsOwned.get(entity.getOid()));
                    setFallbackAccountOid(targetId, entity.getOid());
                }
            });
        });
    }

    /**
     * Get the fallback account oid for a given target. The "fallback account" is going to be the
     * first master account discovered by a target, or 0 if no fallback account was found.
     *
     * This is used when we need to determine the owner account for something, but may have
     * incomplete data. An example is an RI that is bought on the current day -- normally the
     * account ownership comes from the billing data, but that would not be available until the next
     * day. So between now and the next billing data, we won't have an explicit account owner
     * identified -- so we will use the default owner.
     *
     * Hopefully we can replace this with something more explicit in the future.
     *
     * @param targetId
     * @return
     */
    public long getFallbackAccountOid(long targetId) {
        // default owners are stored in the cloud entity map using a special key of "Default Owner
        // for xxx", where xxx = the target id.
        String fallbackAccountKey = FALLBACK_ACCOUNT_PREFIX + targetId;
        return cloudEntityOidByLocalId.getOrDefault(fallbackAccountKey, FALLBACK_ACCOUNT_DEFAULT_OID);
    }

    /**
     * Sets the fallback account oid to use for the specified target id.
     *
     * @param targetId
     * @param fallbackAccountOid
     */
    private void setFallbackAccountOid(long targetId, Long fallbackAccountOid) {
        String fallbackAccountKey = FALLBACK_ACCOUNT_PREFIX + targetId;
        logger.debug("Setting fallback account for target {} to {}", targetId, fallbackAccountOid);
        cloudEntityOidByLocalId.put(fallbackAccountKey, fallbackAccountOid);
    }

    @Override
    public int size() {
        return cloudEntityOidByLocalId.size();
    }

    @Override
    public boolean isEmpty() {
        return cloudEntityOidByLocalId.isEmpty();
    }

    @Override
    public boolean containsKey(final Object key) {
        return cloudEntityOidByLocalId.containsKey(key);
    }

    @Override
    public boolean containsValue(final Object value) {
        return cloudEntityOidByLocalId.containsValue(value);
    }

    @Override
    public Long get(final Object key) {
        return cloudEntityOidByLocalId.get(key);
    }

    @Override
    public Long put(final String key, final Long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long remove(final Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(final Map<? extends String, ? extends Long> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> keySet() {
        return cloudEntityOidByLocalId.keySet();
    }

    @Override
    public Collection<Long> values() {
        return cloudEntityOidByLocalId.values();
    }

    @Override
    public Set<Entry<String, Long>> entrySet() {
        return cloudEntityOidByLocalId.entrySet();
    }

    public Map<String, Collection<String>> getExtraneousIdLookUps() {
        return extraneousIdLookUps;
    }
}
