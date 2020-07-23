package com.vmturbo.cost.component.reserved.instance;

import static com.vmturbo.cost.component.db.Tables.ACCOUNT_TO_RESERVED_INSTANCE_MAPPING;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.UpdatableRecord;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.AccountRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage.RICoverageSource;
import com.vmturbo.cost.component.db.enums.AccountToReservedInstanceMappingRiSourceCoverage;
import com.vmturbo.cost.component.db.tables.records.AccountToReservedInstanceMappingRecord;

/**
 * This class stores and retrieves per-account and per-entity RI Coverage information for
 * discovered and undiscovered accounts/RIs.
 */
public class AccountRIMappingStore {
    private static final Logger logger = LogManager.getLogger();

    private static final int chunkSize = 2000;

    private final DSLContext dsl;

    /**
     * Creates {@link AccountRIMappingStore} instance.
     *
     * @param dsl DSL context.
     */
    public AccountRIMappingStore(@Nonnull DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * Stores (i.e inserts or updates) RI coverage records into Account RI Coverage mapping table.
     *
     * @param accRICoverageList list of protobuf structures (one structure per BA) with a summary
     * of RI coverage per account.
     */
    public void updateAccountRICoverageMappings(@Nonnull final List<AccountRICoverageUpload> accRICoverageList) {
        final List<UpdatableRecord<?>> records = new ArrayList<>();

        final LocalDateTime currentTime = LocalDateTime.now(ZoneOffset.UTC);
        for (AccountRICoverageUpload accRIUpload : accRICoverageList) {
            final Long businessAccountOid = accRIUpload.getAccountId();
            for (Coverage coverage : accRIUpload.getCoverageList()) {
                final Long reservedInstanceId = coverage.getReservedInstanceId();
                final Double usedCoupons = coverage.getCoveredCoupons();
                final RICoverageSource riSource = coverage.getRiCoverageSource();

                // Any value is not set - skip the record, nulls are not allowed in DB table.
                if (!coverage.hasReservedInstanceId() || !coverage.hasCoveredCoupons()
                        || !coverage.hasRiCoverageSource()) {
                    continue;
                }
                AccountToReservedInstanceMappingRiSourceCoverage dbRiSrcCoverage = convertRISource(riSource);
                records.add(dsl.newRecord(ACCOUNT_TO_RESERVED_INSTANCE_MAPPING,
                        new AccountToReservedInstanceMappingRecord(
                                currentTime,
                                businessAccountOid,
                                reservedInstanceId,
                                usedCoupons,
                                dbRiSrcCoverage)));
            }
        }
        int countDel = dsl.deleteFrom(ACCOUNT_TO_RESERVED_INSTANCE_MAPPING).execute();
        Lists.partition(records, chunkSize).forEach(accountsChunk -> dsl.batchInsert(accountsChunk).execute());
        logger.info("BA-RI-Mapping: Count of deleted records: {}, updated: {}",
                countDel, records.size());
    }

    private AccountToReservedInstanceMappingRiSourceCoverage convertRISource(
            @Nonnull final RICoverageSource riSource) {
        switch (riSource) {
            case BILLING:
                return AccountToReservedInstanceMappingRiSourceCoverage.BILLING;
            case SUPPLEMENTAL_COVERAGE_ALLOCATION:
                return AccountToReservedInstanceMappingRiSourceCoverage.SUPPLEMENTAL_COVERAGE_ALLOCATION;
            default:
                throw new IllegalArgumentException("Invalid RI coverage protobuf source argument.");
        }
    }

    private RICoverageSource convertReverseRISource(
            @Nonnull final AccountToReservedInstanceMappingRiSourceCoverage riSource) {
        switch (riSource) {
            case BILLING:
                return RICoverageSource.BILLING;
            case SUPPLEMENTAL_COVERAGE_ALLOCATION:
                return RICoverageSource.SUPPLEMENTAL_COVERAGE_ALLOCATION;
            default:
                throw new IllegalArgumentException("Invalid RI coverage database source argument.");
        }
    }

    /**
     * Delete Account RI coverage records for a list of Business Accounts.
     *
     * @param baOids list of Business Accounts whose RI coverage should be deleted.
     *
     * @return count of deleted rows.
     */
    public int deleteAccountRICoverageMappings(List<Long> baOids) {
        logger.info("Deleting data from AccountRICoverage table for account(s): {}", baOids);
        final int rowsDeleted = dsl.deleteFrom(ACCOUNT_TO_RESERVED_INSTANCE_MAPPING)
                    .where(ACCOUNT_TO_RESERVED_INSTANCE_MAPPING.BUSINESS_ACCOUNT_OID
                                .in(baOids)).execute();
        return rowsDeleted;
    }

    /**
     * Retrieves RI Coverage records per each Business Account given in the  baOids argument.
     * Empty argument list will produce RI Coverage records for all accounts in DB.
     *
     * @param baOids list of BAs for which we are interested to get RI coverage data.
     *
     * @return Map of BA to list of RI Coverage data records.
     */
    public Map<Long, List<AccountRIMappingItem>> getAccountRICoverageMappings(final Collection<Long> baOids) {
        Map<Long, List<AccountRIMappingItem>> riCoverageMap = new HashMap<>();

        List<AccountToReservedInstanceMappingRecord> records = dsl.selectFrom(ACCOUNT_TO_RESERVED_INSTANCE_MAPPING)
                .where(filterByOidsCondition(baOids)).fetch();
        logger.info("Selected {} RI coverage records from AccountRICoverage table for {} account(s): ",
                records.size(), baOids.size());
        for (AccountToReservedInstanceMappingRecord rec : records) {
            Long baOid = rec.getBusinessAccountOid();
            Long riId = rec.getReservedInstanceId();
            Double usedCoupons = rec.getUsedCoupons();
            AccountToReservedInstanceMappingRiSourceCoverage riSrcCoverage = rec.getRiSourceCoverage();

            List<AccountRIMappingItem> riCoverageItems = riCoverageMap.get(baOid);
            if (riCoverageItems == null) {
                riCoverageItems = new ArrayList<>();
                riCoverageMap.put(baOid, riCoverageItems);
            }
            riCoverageItems.add(new AccountRIMappingItem(baOid, riId, usedCoupons,
                    convertReverseRISource(riSrcCoverage)));
        }

        return riCoverageMap;
    }

    /**
     * Condition to filter the BA RI Coverage by the list of BA oids.
     *
     * @param oids The BA oids.
     * @return The condition.
     */
    private Condition filterByOidsCondition(final Collection<Long> oids) {
        return oids.isEmpty() ? DSL.trueCondition() : ACCOUNT_TO_RESERVED_INSTANCE_MAPPING.BUSINESS_ACCOUNT_OID.in(oids);
    }

    /**
     * Class representing item of account RI coverage.
     */
    protected class AccountRIMappingItem {
        private final Long businessAccountOid;
        private final Long reservedInstanceId;
        private final Double usedCoupons;
        private final RICoverageSource riSource;

        public AccountRIMappingItem(Long businessAccountOid, Long reservedInstanceId, Double usedCoupons,
                RICoverageSource riSource) {
            super();
            this.businessAccountOid = businessAccountOid;
            this.reservedInstanceId = reservedInstanceId;
            this.usedCoupons = usedCoupons;
            this.riSource = riSource;
        }

        public Long getBusinessAccountOid() {
            return businessAccountOid;
        }

        public Long getReservedInstanceId() {
            return reservedInstanceId;
        }

        public Double getUsedCoupons() {
            return usedCoupons;
        }

        public RICoverageSource getRiSource() {
            return riSource;
        }

        @Override
        public String toString() {
            return "AccountRICoverageItem [businessAccountOid=" + businessAccountOid + ", reservedInstanceId="
                    + reservedInstanceId + ", usedCoupons=" + usedCoupons + ", riSource=" + riSource + "]";
        }
    }
}
