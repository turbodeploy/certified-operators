package com.vmturbo.cost.component.reserved.instance;

import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_SPEC;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceSpecRecord;
import com.vmturbo.cost.component.identity.IdentityProvider;

/**
 * This class is used to update reserved_instance_specs table based on latest reserved instance spec
 * data which comes from Topology Processor. There are two parts of reserved instance spec data: first
 * part comes from reserved instance bought, second part comes from reserved instance cost price table.
 */
public class ReservedInstanceSpecStore implements DiagsRestorable {

    private static final String reservedInstanceSpecDumpFile = "reservedInstanceSpec_dump";

    private static final Logger logger = LogManager.getLogger();

    private final int riBatchSize;

    private final IdentityProvider identityProvider;

    private final DSLContext dsl;

    /**
     * For JUnit testing only.
     */
    @VisibleForTesting
    ReservedInstanceSpecStore() {
        this.dsl = null;
        this.identityProvider = null;
        this.riBatchSize = 10000;
    }

    /**
     * Constructor.
     * @param dsl database context.
     * @param identityProvider identity provivder to generate OIDs.
     * @param riBatchSize max size of a batch to insert into 'reserved_instance_spec'.
     */
    public ReservedInstanceSpecStore(@Nonnull final DSLContext dsl,
                                     @Nonnull final IdentityProvider identityProvider,
                                     final int riBatchSize) {
        this.dsl = Objects.requireNonNull(dsl);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.riBatchSize = riBatchSize;
    }

    /**
     * Given a new list of ReservedInstanceSpecs from the probe with temporary ID's, find
     * the mapping from the tempID to an OID.
     *
     * @param newReservedInstanceSpecs a list of new reserved instances
     * @return a mapping from tempID to ReservedInstanceSpec OID
     */
    public Map<Long, Long> updateReservedInstanceSpec(
            @Nonnull final List<ReservedInstanceSpec> newReservedInstanceSpecs) {
        return updateReservedInstanceSpec(dsl, newReservedInstanceSpecs);
    }

    /**
     * For reserved instance spec data from the mediation cost probes, when updating
     * reserved_instance_spec table, it use {@link ReservedInstanceSpecInfo} to compare with current
     * existing reserved instance specs records in table, if {@link ReservedInstanceSpecInfo} are
     * same, we consider them as duplicate, will not insert duplicate spec data into data.
     * For those new {@link ReservedInstanceSpecInfo}, it will insert them into reserved_instance_spec
     * table as a new reserved instance spec record, and will assign a new Id for them.
     *
     * <p>And this method will return a Map which key is the local id of reserved instance spec and the
     * local id is set by Topology Processor in order to keep the relationship between reserved instance
     * bought with reserved instance bought spec. And the Map's value is the real id of reserved instance
     * spec after update spec table.
     * </p>
     *
     * @param context {@link DSLContext} transactional context.
     * @param newReservedInstanceBoughtSpec if any reserved instance spec enum is not match.
     * @return a Map which key is local Id of reserved instance spec, value is the real id of reserved
     *         instance spec.
     */
    public Map<Long, Long> updateReservedInstanceSpec(
            @Nonnull final DSLContext context,
            @Nonnull final List<ReservedInstanceSpec> newReservedInstanceBoughtSpec) {
        final Map<ReservedInstanceSpecInfo, Long> existingRISpecInfoToId =
                getExistingRISpecInfoToIdMap(context);
        logger.debug("updateReservedInstanceSpec: existingRISpecInfoToId.size={}",
            () -> existingRISpecInfoToId.size());

        final Map<Long, Long> specLocalIdToRealIdMap = new HashMap<>();
        final List<ReservedInstanceSpec> reservedInstanceSpecsToAdd = new ArrayList<>();
        for (ReservedInstanceSpec newSpec : newReservedInstanceBoughtSpec) {
            final ReservedInstanceSpecInfo newSpecInfo = newSpec.getReservedInstanceSpecInfo();
            final Long riSpecId = existingRISpecInfoToId.get(newSpecInfo);
            if (riSpecId != null) {
                specLocalIdToRealIdMap.putIfAbsent(newSpec.getId(), riSpecId);
            } else {
                reservedInstanceSpecsToAdd.add(newSpec);
            }
        }

        final List<ReservedInstanceSpecRecord> reservedInstanceSpecRecordsToAdd = new ArrayList<>();
        // it try to handle the case that if the input "newReservedInstanceBoughtSpec" have multiple
        // different newSpec id with same specInfo. In this case, we should only insert one specInfo
        // into table.
        final Set<ReservedInstanceSpecInfo> reservedInstanceSpecInfoSet =
                reservedInstanceSpecsToAdd.stream()
                    .map(ReservedInstanceSpec::getReservedInstanceSpecInfo)
                    .collect(Collectors.toSet());
        final Map<ReservedInstanceSpecInfo, Long> reservedInstanceSpecInfoToNewIdMap = new HashMap<>();
        for (ReservedInstanceSpecInfo newSpecInfo : reservedInstanceSpecInfoSet) {
            final ReservedInstanceSpecRecord newRecord =
                    createNewReservedInstanceSpecRecord(context, newSpecInfo);
            reservedInstanceSpecInfoToNewIdMap.put(newSpecInfo, newRecord.getId());
            reservedInstanceSpecRecordsToAdd.add(newRecord);
        }
        for (ReservedInstanceSpec newSpec : reservedInstanceSpecsToAdd) {
            specLocalIdToRealIdMap.put(newSpec.getId(),
                    // the Map must contains the specInfo of new spec.
                    reservedInstanceSpecInfoToNewIdMap.get(newSpec.getReservedInstanceSpecInfo()));
        }

        if (!reservedInstanceSpecRecordsToAdd.isEmpty()) {
            context.transaction(configuration -> {
                final DSLContext transactionContext = DSL.using(configuration);
                Lists.partition(reservedInstanceSpecRecordsToAdd, riBatchSize).forEach(batch -> {
                    transactionContext.batchInsert(batch).execute();
                });
            });
        }

        logger.debug("Finished updateReservedInstanceSpec: specLocalIdToRealIdMap.size={}",
            () -> specLocalIdToRealIdMap.size());
        return specLocalIdToRealIdMap;
    }

    /**
     * Get all {@link ReservedInstanceSpec} from reserved instance spec table.
     *
     * @return a list of {@link ReservedInstanceSpec}.
     */
    public List<ReservedInstanceSpec> getAllReservedInstanceSpec() {
        final List<ReservedInstanceSpec> allReservedInstanceSpecs = new ArrayList<>();
        for (ReservedInstanceSpecRecord record : internalGetAllReservedInstanceSpecs(dsl)) {
            allReservedInstanceSpecs.add(reservedInstancesToProto(record));
        }
        return allReservedInstanceSpecs;
    }


    /**
     * Get all {@link ReservedInstanceSpec} within a specific region.
     * @param regionOid The target region OID.
     * @return The list of {@link ReservedInstanceSpec} instances associated with the target region.
     * There is no defined order to the list.
     */
    public List<ReservedInstanceSpec> getAllRISpecsForRegion(long regionOid) {
        try (Stream<ReservedInstanceSpecRecord> recordsStream = dsl
                .selectFrom(RESERVED_INSTANCE_SPEC)
                .where(RESERVED_INSTANCE_SPEC.REGION_ID.equal(regionOid))
                .stream()) {

            return recordsStream.map(this::reservedInstancesToProto)
                    .collect(Collectors.toList());
        }
    }

    /**
     * Get {@link ReservedInstanceSpec} by input ids.
     *
     * @param ids a set of spec ids.
     * @return a list of {@link ReservedInstanceSpec}
     */
    public List<ReservedInstanceSpec> getReservedInstanceSpecByIds(@Nonnull final Set<Long> ids) {
        return dsl.selectFrom(RESERVED_INSTANCE_SPEC)
                .where(RESERVED_INSTANCE_SPEC.ID.in(ids))
                .fetch().stream()
                .map(this::reservedInstancesToProto)
                .collect(Collectors.toList());
    }

    /**
     * Get {@link ReservedInstanceSpec} by input ids.
     *
     * @param ids a set of spec ids.
     * @param context a DSLContext object.
     * @return a list of {@link ReservedInstanceSpec}
     */
    public List<ReservedInstanceSpec> getReservedInstanceSpecByIdsWithContext(@Nonnull final Set<Long> ids,
                    DSLContext context) {
        return context.selectFrom(RESERVED_INSTANCE_SPEC)
                        .where(RESERVED_INSTANCE_SPEC.ID.in(ids))
                        .fetch().stream()
                        .map(this::reservedInstancesToProto)
                        .collect(Collectors.toList());
    }

    private List<ReservedInstanceSpecRecord> internalGetAllReservedInstanceSpecs(
            @Nonnull final DSLContext context) {
        return context.selectFrom(RESERVED_INSTANCE_SPEC).fetch();
    }

    /**
     * Get the all current existing reserved instance spec record from tables. And convert them into
     * a Map from {@link ReservedInstanceSpecInfo} to its Id.
     *
     * @param context {@link DSLContext} transactional context.
     * @return A Map which key is {@link ReservedInstanceSpecInfo} and value is the real id of spec.
     */
    private Map<ReservedInstanceSpecInfo, Long> getExistingRISpecInfoToIdMap(@Nonnull final DSLContext context) {
        final Map<ReservedInstanceSpecInfo, Long> existingRISpecInfoToIdMap = new HashMap<>();
        for (ReservedInstanceSpecRecord record : internalGetAllReservedInstanceSpecs(context)) {
            final ReservedInstanceSpec riSpec = reservedInstancesToProto(record);
            existingRISpecInfoToIdMap.putIfAbsent(riSpec.getReservedInstanceSpecInfo(), riSpec.getId());
        }
        return existingRISpecInfoToIdMap;
    }

    /**
     * Convert {@link ReservedInstanceSpecRecord} to {@link ReservedInstanceSpec}.
     *
     * @param reservedInstanceSpecRecord {@link ReservedInstanceSpecRecord}
     * @return {@link ReservedInstanceSpec}
     */
    private ReservedInstanceSpec reservedInstancesToProto(
            @Nonnull final ReservedInstanceSpecRecord reservedInstanceSpecRecord) {
        return  ReservedInstanceSpec.newBuilder()
                .setId(reservedInstanceSpecRecord.getId())
                .setReservedInstanceSpecInfo(reservedInstanceSpecRecord.getReservedInstanceSpecInfo())
                .build();
    }

    /**
     * Convert {@link ReservedInstanceSpec} to {@link ReservedInstanceSpecRecord}.
     *
     * @param context {@link DSLContext} transactional context.
     * @param reservedInstanceSpecInfo {@link ReservedInstanceSpecInfo}
     * @return {@link ReservedInstanceSpecRecord}
     */
    private ReservedInstanceSpecRecord createNewReservedInstanceSpecRecord(
            @Nonnull DSLContext context,
            @Nonnull final ReservedInstanceSpecInfo reservedInstanceSpecInfo) {
        return context.newRecord(RESERVED_INSTANCE_SPEC, new ReservedInstanceSpecRecord(
                identityProvider.next(),
                reservedInstanceSpecInfo.getType().getOfferingClass().getNumber(),
                reservedInstanceSpecInfo.getType().getPaymentOption().getNumber(),
                reservedInstanceSpecInfo.getType().getTermYears(),
                reservedInstanceSpecInfo.getTenancy().getNumber(),
                reservedInstanceSpecInfo.getOs().getNumber(),
                reservedInstanceSpecInfo.getTierId(),
                reservedInstanceSpecInfo.getRegionId(),
                reservedInstanceSpecInfo));
    }

    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags) throws DiagnosticsException {
        // TODO to be implemented as part of OM-58627
    }

    @Override
    public void collectDiags(@Nonnull final DiagnosticsAppender appender) throws DiagnosticsException {
        dsl.transaction(transactionContext -> {
            final DSLContext transaction = DSL.using(transactionContext);
            Stream<ReservedInstanceSpecRecord> latestRecords = transaction.selectFrom(RESERVED_INSTANCE_SPEC).stream();
            latestRecords.forEach(s -> {
                try {
                    appender.appendString(s.formatJSON());
                } catch (DiagnosticsException e) {
                    logger.error("Exception encountered while appending reserved instance spec records"
                            + " to the diags dump", e);
                }
            });
        });
    }


    @Nonnull
    @Override
    public String getFileName() {
        return reservedInstanceSpecDumpFile;
    }
}
