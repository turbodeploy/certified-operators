package com.vmturbo.cost.component.cca;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.commitment.analysis.spec.catalog.ReservedInstanceCatalog;
import com.vmturbo.cloud.commitment.analysis.spec.catalog.SpecCatalogKey;
import com.vmturbo.cloud.commitment.analysis.spec.catalog.SpecCatalogKey.OrganizationType;
import com.vmturbo.cloud.common.immutable.HiddenImmutableTupleImplementation;
import com.vmturbo.cloud.common.topology.BillingFamilyRetriever;
import com.vmturbo.cloud.common.topology.BillingFamilyRetrieverFactory;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.components.common.utils.MultimapUtils;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;
import com.vmturbo.group.api.GroupAndMembers;

/**
 * A local instance of {@link ReservedInstanceCatalog}.
 */
public class LocalReservedInstanceCatalog implements ReservedInstanceCatalog {

    private final Logger logger = LogManager.getLogger();

    private final BillingFamilyRetriever billingFamilyRetriever;

    private final BusinessAccountPriceTableKeyStore businessAccountPriceTableKeyStore;

    private final PriceTableStore priceTableStore;

    private final ReservedInstanceSpecStore reservedInstanceSpecStore;

    private final Set<Long> scopedAccountOids;

    private final Object catalogLock = new Object();

    @GuardedBy("catalogLock")
    private final SetMultimap<SpecCatalogKey, CatalogEntry> catalogsByOrganizationMap = HashMultimap.create();

    @GuardedBy("catalogLock")
    private final Map<Long, Long> accountCatalogMap = new HashMap<>();

    @GuardedBy("catalogLock")
    private final SetMultimap<Long, Long> riSpecsByCatalog = HashMultimap.create();

    @GuardedBy("catalogLock")
    private final SetMultimap<Long, ReservedInstanceSpec> riSpecsByRegion = HashMultimap.create();

    private LocalReservedInstanceCatalog(@Nonnull BillingFamilyRetriever billingFamilyRetriever,
                                         @Nonnull BusinessAccountPriceTableKeyStore businessAccountPriceTableKeyStore,
                                         @Nonnull PriceTableStore priceTableStore,
                                         @Nonnull ReservedInstanceSpecStore reservedInstanceSpecStore,
                                         @Nonnull Collection<Long> scopedAccountOids) {

        this.billingFamilyRetriever = Objects.requireNonNull(billingFamilyRetriever);
        this.businessAccountPriceTableKeyStore = Objects.requireNonNull(businessAccountPriceTableKeyStore);
        this.priceTableStore = Objects.requireNonNull(priceTableStore);
        this.reservedInstanceSpecStore = Objects.requireNonNull(reservedInstanceSpecStore);
        this.scopedAccountOids = ImmutableSet.copyOf(scopedAccountOids);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public Set<SpecAccountGrouping<ReservedInstanceSpec>> getRegionalSpecs(@Nonnull SpecCatalogKey catalogKey,
                                                                           long regionOid) {

        synchronized (catalogLock) {
            final Set<CatalogEntry> catalogEntries = (catalogKey.organizationType() == OrganizationType.BILLING_FAMILY)
                    ? getOrLoadCatalogEntriesForBillingFamily(catalogKey)
                    : getOrLoadCatalogEntryForAccount(catalogKey);
            return resolveSpecsForCatalogs(catalogEntries, regionOid);
        }
    }

    private Set<SpecAccountGrouping<ReservedInstanceSpec>> resolveSpecsForCatalogs(@Nonnull Set<CatalogEntry> catalogEntries,
                                                                                   long regionOid) {

        final Map<Long, Set<Long>> riSpecIdsByCatalogMap = catalogEntries.stream()
                .collect(ImmutableMap.toImmutableMap(
                        CatalogEntry::catalogId,
                        catalogEntry -> getOrLoadCatalogRISpecs(catalogEntry.catalogId())));

        final Map<Long, ReservedInstanceSpec> riSpecsInRegionMap = getOrLoadRISpecsForRegion(regionOid)
                .stream()
                .collect(ImmutableMap.toImmutableMap(
                        ReservedInstanceSpec::getId,
                        Function.identity()));

        return catalogEntries.stream()
                .map(catalogEntry -> SpecAccountGrouping.of(
                        riSpecIdsByCatalogMap.getOrDefault(catalogEntry.catalogId(), Collections.emptySet())
                                .stream()
                                .map(riSpecsInRegionMap::get)
                                .filter(Objects::nonNull)
                                .collect(ImmutableSet.<ReservedInstanceSpec>toImmutableSet()),
                        catalogEntry.availableAccountOids()))
                .collect(ImmutableSet.toImmutableSet());
    }

    private Set<CatalogEntry> getOrLoadCatalogEntriesForBillingFamily(SpecCatalogKey billingFamilyKey) {

        return MultimapUtils.computeIfAbsent(
                catalogsByOrganizationMap,
                billingFamilyKey,
                (__) -> billingFamilyRetriever.getBillingFamilyById(billingFamilyKey.organizationId())
                        .map(this::loadCatalogEntriesForBillingFamily)
                        .orElseGet(() -> {
                            logger.error("Unable to find catalogs for billing family '{}'", billingFamilyKey.organizationId());
                            return Collections.emptySet();
                        }));
    }

    private Set<CatalogEntry> getOrLoadCatalogEntryForAccount(SpecCatalogKey accountKey) {
        return MultimapUtils.computeIfAbsent(
                catalogsByOrganizationMap,
                accountKey,
                (__) -> businessAccountPriceTableKeyStore.fetchPriceTableKeyOidForAccount(accountKey.organizationId())
                        .map(catalogId -> (Set<CatalogEntry>)ImmutableSet.of(CatalogEntry.of(catalogId, accountKey.organizationId())))
                        .orElseGet(() -> {
                            logger.error("Unable to find catalog for standalone account '{}'", accountKey.organizationId());
                            return Collections.emptySet();
                        }));
    }

    private Set<CatalogEntry> loadCatalogEntriesForBillingFamily(@Nonnull GroupAndMembers billingFamily) {

        final Set<Long> accountSet = ImmutableSet.copyOf(billingFamily.entities());
        final Set<Long> accountsInScope = scopedAccountOids.isEmpty()
                ? accountSet
                : Sets.intersection(scopedAccountOids, accountSet);

        final Map<Long, Long> accountToDirectCatalogMap =
                businessAccountPriceTableKeyStore.fetchPriceTableKeyOidsByBusinessAccount(accountsInScope);

        return accountsInScope.stream()
                .collect(Collectors.groupingBy(accountToDirectCatalogMap::get, ImmutableSet.toImmutableSet()))
                .entrySet()
                .stream()
                .map(catalogSet -> CatalogEntry.of(catalogSet.getKey(), catalogSet.getValue()))
                .collect(ImmutableSet.toImmutableSet());
    }

    @Nullable
    private Set<Long> getOrLoadCatalogRISpecs(long catalogId) {

        return MultimapUtils.computeIfAbsent(
                riSpecsByCatalog,
                catalogId,
                (__) -> priceTableStore.getRiPriceTable(catalogId)
                        .map(riPriceTable -> (Set<Long>)ImmutableSet.copyOf(
                                riPriceTable.getRiPricesBySpecIdMap().keySet()))
                        .orElseGet(() -> {
                            logger.warn("Unable to load RI price table with key '{}'", catalogId);
                            return Collections.emptySet();
                        }));
    }

    private Set<ReservedInstanceSpec> getOrLoadRISpecsForRegion(long regionOid) {
        return MultimapUtils.computeIfAbsent(
                riSpecsByRegion,
                regionOid,
                (__) -> ImmutableSet.copyOf(reservedInstanceSpecStore.getAllRISpecsForRegion(regionOid)));
    }

    /**
     * A factory class for constructing {@link LocalReservedInstanceCatalog} instances.
     */
    public static class LocalReservedInstanceCatalogFactory implements ReservedInstanceCatalogFactory {

        private final BillingFamilyRetrieverFactory billingFamilyRetrieverFactory;

        private final BusinessAccountPriceTableKeyStore businessAccountPriceTableKeyStore;

        private final PriceTableStore priceTableStore;

        private final ReservedInstanceSpecStore reservedInstanceSpecStore;

        /**
         * Constructs a new {@link LocalReservedInstanceCatalogFactory} instance.
         * @param billingFamilyRetrieverFactory The {@link BillingFamilyRetrieverFactory}.
         * @param businessAccountPriceTableKeyStore The {@link BusinessAccountPriceTableKeyStore}.
         * @param priceTableStore The {@link PriceTableStore}.
         * @param reservedInstanceSpecStore The {@link ReservedInstanceSpecStore}.
         */
        public LocalReservedInstanceCatalogFactory(@Nonnull BillingFamilyRetrieverFactory billingFamilyRetrieverFactory,
                                                   @Nonnull BusinessAccountPriceTableKeyStore businessAccountPriceTableKeyStore,
                                                   @Nonnull PriceTableStore priceTableStore,
                                                   @Nonnull ReservedInstanceSpecStore reservedInstanceSpecStore) {

            this.billingFamilyRetrieverFactory = Objects.requireNonNull(billingFamilyRetrieverFactory);
            this.businessAccountPriceTableKeyStore = Objects.requireNonNull(businessAccountPriceTableKeyStore);
            this.priceTableStore = Objects.requireNonNull(priceTableStore);
            this.reservedInstanceSpecStore = Objects.requireNonNull(reservedInstanceSpecStore);
        }

        /**
         * {@inheritDoc}.
         */
        @Override
        public ReservedInstanceCatalog createAccountRestrictedCatalog(@Nonnull final Collection<Long> accountOids) {
            return new LocalReservedInstanceCatalog(
                    billingFamilyRetrieverFactory.newInstance(),
                    businessAccountPriceTableKeyStore,
                    priceTableStore,
                    reservedInstanceSpecStore,
                    accountOids);
        }
    }

    /**
     * Represents a catalog entry (price table) and the accounts associated with the catalog.
     */
    @HiddenImmutableTupleImplementation
    @Immutable(builder = false)
    interface CatalogEntry {

        long catalogId();

        Set<Long> availableAccountOids();

        static CatalogEntry of(long catalogId, Set<Long> availableAccountOids) {
            return CatalogEntryTuple.of(catalogId, availableAccountOids);
        }

        static CatalogEntry of(long catalogId, long availableAccountOid) {
            return CatalogEntryTuple.of(catalogId, ImmutableSet.of(availableAccountOid));
        }
    }
}
