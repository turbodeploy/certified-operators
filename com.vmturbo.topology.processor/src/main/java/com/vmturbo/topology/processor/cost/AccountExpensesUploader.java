package com.vmturbo.topology.processor.cost;

import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.CLOUD_COST_EXPENSES_SECTION;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.CLOUD_COST_UPLOAD_TIME;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.UPLOAD_REQUEST_BUILD_STAGE;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.UPLOAD_REQUEST_UPLOAD_STAGE;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.ServiceExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.TierExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.Builder;
import com.vmturbo.common.protobuf.cost.Cost.GetAccountExpensesChecksumRequest;
import com.vmturbo.common.protobuf.cost.Cost.UploadAccountExpensesRequest;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc.RIAndExpenseUploadServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.TargetCostData;
import com.vmturbo.topology.processor.stitching.StitchingContext;

/**
 * RICostDataUploader collects Account expense data and Reserved Instance coverage and purchases
 * data from probe discoveries and sends it to the cost component.
 */
public class AccountExpensesUploader {
    private static final Logger logger = LogManager.getLogger();
    private static final String CSP_OUTPUT_FORMAT = "%s::CS::%s";
    private static final int CSP_NAME_INDEX = 0;
    private static final int AZURE_CS_NAME_INDEX = 2;
    private static final int AWS_CS_NAME_INDEX = 3;
    private static final int NUMBER_OF_CS_SPECS_OLD_FORMAT = 4;
    private static final int NUMBER_OF_CS_SPECS_NEW_FORMAT = 3;

    private final RIAndExpenseUploadServiceBlockingStub costServiceClient;

    /**
     * Minimum number of minutes to wait between Expense uploads. Must be >= 0.
     */
    private final int minimumAccountExpensesUploadIntervalMins;

    /**
     * Used to generate time stamps
     */
    private final Clock clock;

    /**
     * Track the last upload time per target. We want to space the uploads apart according
     * to the "minimum account expenses upload interval" setting.
     */
    private Map<Long, Instant> lastUploadTimePerTarget = new HashMap<>();

    public AccountExpensesUploader(RIAndExpenseUploadServiceBlockingStub costServiceClient,
                                   int minimumAccountExpensesUploadIntervalMins, Clock clock) {
        this.costServiceClient = costServiceClient;
        if (minimumAccountExpensesUploadIntervalMins < 0) {
            throw new IllegalArgumentException("minimumAccountExpensesUploadIntervalMins cannot be less than 0.");
        }
        this.minimumAccountExpensesUploadIntervalMins = minimumAccountExpensesUploadIntervalMins;
        this.clock = clock;
    }

    /**
     * Upload the cloud business account expenses.
     *
     * Called in the topology pipeline after the stitching context has been created, but before
     * it has been converted to a topology map. This is because a lot of the data we need is in the
     * raw cloud entity data, much of which we lose in the conversion to topology map.
     *
     * We will be cross-referencing data from the cost DTOs, non-market entities, and topology
     * entities (in stitching entity form), from the billing and discovery probes. So there may be
     * some sensitivity to discovery mismatches between billing and discovery probe data.
     *
     * @param costDataByTarget mapping of target OID to {@link TargetCostData}
     * @param topologyInfo the topology info
     * @param stitchingContext the stitching context, containing the stitched entities
     * @param cloudEntitiesMap cloud entities
     */
    public synchronized void uploadAccountExpenses(Map<Long, TargetCostData> costDataByTarget,
                                                   TopologyInfo topologyInfo,
                                                   StitchingContext stitchingContext,
                                                   CloudEntitiesMap cloudEntitiesMap) {
        // Check if any expenses were discovered yet.
        // Note: costDataByTargetIdSnapshot can be populated while no actual expenses were discovered.
        // For example: we added Azure SP target, but haven't added Azure EA target yet.
        if (costDataByTarget.values().stream()
                .allMatch(targetCostData -> targetCostData.costDataDTOS.isEmpty())) {
            logger.info("no expenses were discovered yet");
            return;
        }

        // Check if any cloud services were discovered yet.
        // For example: the Azure EA discovery completed (so expenses are discovered), but the
        // Azure Subscription targets discoveries haven't finished yet, so we don't have any cloud
        // services entities to match.
        if (stitchingContext.getEntitiesByEntityTypeAndTarget().keySet().stream()
                .noneMatch(entityType -> entityType == EntityType.CLOUD_SERVICE)) {
            logger.info("no cloud services were discovered yet");
            return;
        }

        // check if we are within our minimum upload interval. Note that we are not persisting the
        // last upload time stamp, and starting a new set of upload interval checks every time the
        // component starts. Component restarts should be rare, so we don't expect this to be much
        // of an issue, but if this does become a problem we can add persistence of the last upload
        // time.
        if (shouldSkipProcessingExpenses(costDataByTarget)) {
            logger.info("Skipping upload of cost data, since we're within minimum upload " +
                    "interval since the last upload");
            return;
        }
        // we've passed the throttling checks.

        DataMetricTimer buildTimer = CLOUD_COST_UPLOAD_TIME.labels(CLOUD_COST_EXPENSES_SECTION,
                UPLOAD_REQUEST_BUILD_STAGE).startTimer();

        // get the account expenses
        final List<AccountExpenses> accountExpensesList = createAccountExpenses(cloudEntitiesMap,
                stitchingContext, costDataByTarget);
        logger.debug("Created {} AccountExpenses.", accountExpensesList.size());

        // assemble and execute the upload
        final UploadAccountExpensesRequest.Builder requestBuilder = UploadAccountExpensesRequest.newBuilder();
        accountExpensesList.forEach(requestBuilder::addAccountExpenses);
        buildTimer.observe();
        logger.debug("Building account expenses upload request took {} secs", buildTimer.getTimeElapsedSecs());

        // check if this requests checksum is different than the last requests checksum.
        // the checksum function is indifferent to the order of the expenses or the service/tier expenses.
        long lastRequestChecksum = costServiceClient.getAccountExpensesChecksum(
                GetAccountExpensesChecksumRequest.getDefaultInstance()).getChecksum();
        long newRequestChecksum = calcAccountExpensesUploadRequestChecksum(requestBuilder.build());
        if (newRequestChecksum != lastRequestChecksum) {
            requestBuilder.setTopologyId(topologyInfo.getTopologyId());
            requestBuilder.setChecksum(newRequestChecksum);
            requestBuilder.setCreatedTime(System.currentTimeMillis());

            logger.debug("Request hash [{}] is different from last processed hash [{}], will upload this request.",
                    Long.toUnsignedString(newRequestChecksum), Long.toUnsignedString(lastRequestChecksum));
            try (DataMetricTimer uploadTimer = CLOUD_COST_UPLOAD_TIME.labels(
                    CLOUD_COST_EXPENSES_SECTION, UPLOAD_REQUEST_UPLOAD_STAGE).startTimer()) {
                // we should probably upload empty data too, if we are relying on this as a way to
                // "clear" data when all cloud targets are removed.
                costServiceClient.uploadAccountExpenses(requestBuilder.build());
                logger.debug("Account expenses upload took {} secs", uploadTimer.getTimeElapsedSecs());
                Instant lastUploadTime = clock.instant();
                costDataByTarget.forEach((targetId, targetCostData) ->
                        lastUploadTimePerTarget.put(targetId, lastUploadTime));
            } catch (Exception e) {
                logger.error("Error uploading cloud account expenses", e);
            }
        } else {
            logger.info("Cloud Account Expenses upload step calculated same hash as the last processed hash -- will skip this upload.");
        }
    }

    /**
     * Check whether we can skip creating the account expenses and sending them to the cost
     * component, according to the last upload time of each target which has cost data.
     *
     * @param costDataByTarget mapping of target ID to {@link TargetCostData}
     * @return whether we can skip processing the account expenses for all targets.
     */
    @VisibleForTesting
    boolean shouldSkipProcessingExpenses(Map<Long, TargetCostData> costDataByTarget) {
        return !lastUploadTimePerTarget.isEmpty() &&
                costDataByTarget.values()
                        .stream()
                        .map(this::shouldSkipProcessingTargetExpenses)
                        .reduce(Boolean::logicalAnd)
                        .orElse(false);
    }

    /**
     * Check if we can skip processing the account expenses for this target.
     * We can skip processing the expenses if:
     * 1. There are no cost data DTOs - no expenses to process for this specific target.
     * 2. We already processed the expenses for this target in the last hour.
     *
     * @param targetCostData the target's cost data.
     * @return whether we can skip processing the account expenses for this target.
     */
    private boolean shouldSkipProcessingTargetExpenses(TargetCostData targetCostData) {
        Instant lastUploadTime = lastUploadTimePerTarget.get(targetCostData.targetId);
        return targetCostData.costDataDTOS.isEmpty() ||
                (lastUploadTime != null && clock.instant()
                        .minus(minimumAccountExpensesUploadIntervalMins, ChronoUnit.MINUTES)
                        .isBefore(lastUploadTime));
    }

    /**
     * This method calculates a hash of all the account expenses in the request, regardless of
     * their order in the expenses list.
     *
     * @param request The upload account expenses request
     * @return a hash of all the account expenses in the request
     */
    @VisibleForTesting
    long calcAccountExpensesUploadRequestChecksum(UploadAccountExpensesRequest request) {
        long hash = 41L;
        if (request.getAccountExpensesCount() > 0) {
            hash = (53L * hash) + request.getAccountExpensesList().stream()
                    .map(this::calcAccountExpensesChecksum)
                    .reduce(Long::sum)
                    .get();
        }
        return hash;
    }

    /**
     * This method calculates the hash of the service and tier expenses, regardless of their
     * order in the ServiceExpenses and TierExpenses lists in the account expenses info.
     *
     * @param accountExpenses The account expenses
     * @return a hash of all the service and tier expenses in the account expenses
     */
    private long calcAccountExpensesChecksum(AccountExpenses accountExpenses) {
        long hash = 41L;

        if (accountExpenses.hasAssociatedAccountId()) {
            hash = (53L * hash) + com.google.protobuf.Internal.hashLong(
                    accountExpenses.getAssociatedAccountId());
        }

        if (accountExpenses.hasExpensesDate()) {
            hash = (53L * hash) + com.google.protobuf.Internal.hashLong(
                    accountExpenses.getExpensesDate());
        }

        if (accountExpenses.hasAccountExpensesInfo()) {
            AccountExpenses.AccountExpensesInfo info = accountExpenses.getAccountExpensesInfo();

            if (info.getServiceExpensesCount() > 0) {
                hash = (53L * hash) + info.getServiceExpensesList().stream()
                        .map(ServiceExpenses::hashCode)
                        .reduce(Integer::sum)
                        .get();
            }
            if (info.getTierExpensesCount() > 0) {
                hash = (53L * hash) + info.getTierExpensesList().stream()
                        .map(TierExpenses::hashCode)
                        .reduce(Integer::sum)
                        .get();
            }
        }

        return hash;
    }

    /**
     * Create the set of account expenses to be uploaded.
     * @return a map of account expenses, keyed by business account oid
     */
    @VisibleForTesting
    public List<AccountExpenses> createAccountExpenses(
            CloudEntitiesMap cloudEntitiesMap, StitchingContext stitchingContext,
            Map<Long, TargetCostData> costDataByTargetIdSnapshot) {

        final Table<Long, Long, Builder> accountExpenseTable =
                HashBasedTable.create();
        Map<String, Double> cloudServiceSpentMap = new HashMap<>();
        
        costDataByTargetIdSnapshot.forEach((targetId, targetCostData) -> {
            targetCostData.costDataDTOS.forEach(costData -> {
                // find the expenses builder for the associated account.

                if (!costData.hasAccountId()) {
                    logger.warn("No account id set for costData object {} with cost {}",
                            costData.getId(), costData.getCost());
                    return;
                } else if (!costData.hasUsageDate()) {
                    logger.warn("No usage date set for costData with ID '{}' and account ID '{}'. Skipping",
                                costData.getId(), costData.getAccountId());
                    return;
                }
                
                final Long accountOid = cloudEntitiesMap.getOrDefault(costData.getAccountId(),
                        cloudEntitiesMap.getFallbackAccountOid(targetId));
                if (!cloudEntitiesMap.containsKey(costData.getAccountId())) {
                    logger.warn("Couldn't find biz account oid for local id {}, using fallback account {}.",
                            costData.getAccountId(),
                            accountOid);
                }
                
                final AccountExpenses.Builder accountExpensesBuilder;
                if (accountExpenseTable.contains(accountOid, costData.getUsageDate())) {
                    accountExpensesBuilder = accountExpenseTable.get(accountOid, costData.getUsageDate());
                } else {
                    accountExpensesBuilder = AccountExpenses.newBuilder()
                            .setAssociatedAccountId(accountOid)
                            .setExpensesDate(costData.getUsageDate());
                    
                    accountExpenseTable.put(accountOid, costData.getUsageDate(), accountExpensesBuilder);
                }

                // create an expense entry for each cost object
                if (EntityType.CLOUD_SERVICE.equals(costData.getEntityType())) {
                    // create a ServiceExpenses for Cloud Services
                    ServiceExpenses.Builder serviceExpensesBuilder = ServiceExpenses.newBuilder();
                    // find the related cloud service entity from the topology map. We are creating
                    // one cloud service entity per service, rather than per account + service
                    // combination as the DTO represents.
                    // To do mapping from NME cloud service to TP cloud service, we will create an
                    // account-agnostic cloud service 'local id' by slicing the account id out of the
                    // cloud service's regular account-specific local id. We will use this modified
                    // local id to find the shared, account-agnostic cloud service topology entity.
                    String sharedCloudServiceLocalId = sanitizeCloudServiceId(costData.getId());
                    Long cloudServiceOid = cloudEntitiesMap.get(sharedCloudServiceLocalId);
                    if (cloudServiceOid == null) {
                        logger.warn("Couldn't find a cloud service oid for service {}", sharedCloudServiceLocalId);
                        cloudServiceOid = 0L;
                    }
                    cloudServiceSpentMap.compute(sharedCloudServiceLocalId, (k, v) -> v == null
                        ? (costData.getCost()) : v + (costData.getCost()));
                    serviceExpensesBuilder.setAssociatedServiceId(cloudServiceOid);
                    serviceExpensesBuilder.setExpenses(CurrencyAmount.newBuilder()
                            .setAmount(costData.getCost()).build());
                    accountExpensesBuilder.getAccountExpensesInfoBuilder()
                            .addServiceExpenses(serviceExpensesBuilder);
                    logger.debug("Attached ServiceExpenses {} for service {} to account {}({})", costData.getId(),
                            serviceExpensesBuilder.getAssociatedServiceId(), costData.getAccountId(), accountOid);

                } else if (EntityType.VIRTUAL_MACHINE.equals(costData.getEntityType()) ||
                        EntityType.DATABASE_SERVER.equals(costData.getEntityType())) {
                    // Create TierExpenses for compute / database /storage tiers
                    TierExpenses.Builder tierExpensesBuilder = TierExpenses.newBuilder();
                    // find the compute tier matching our cost id
                    Long tierOid = cloudEntitiesMap.get(costData.getId());
                    if (tierOid == null) {
                        logger.warn("Oid not found for tier {} -- will not add expenses for it.", costData.getId());
                        return;
                    }
                    tierExpensesBuilder.setAssociatedTierId(tierOid);
                    tierExpensesBuilder.setExpenses(CurrencyAmount.newBuilder()
                            .setAmount(costData.getCost()).build());
                    accountExpensesBuilder.getAccountExpensesInfoBuilder()
                            .addTierExpenses(tierExpensesBuilder);
                    logger.debug("Attached TierExpenses {} for tier {} to account {}({})", costData.getId(),
                            tierExpensesBuilder.getAssociatedTierId(), costData.getAccountId(), accountOid);
                }

            });
        });
        
        return accountExpenseTable.values().stream()
                .map(AccountExpenses.Builder::build)
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * This function takes a cloud service id -- that may contain an account id in it -- and removes
     * the account id from it, if it exists. The resulting string should be usable as an account-
     * agnostic cloud service local id.
     * If the input cloud service id does not formatted properly,
     * the output will remain the same and appropriate log message will be printed.
     * currently, the Input Id format for AWS is the one that requires sanitization as follows:
     *
     * Input: "aws::192821421245::CS::AmazonS3" (account id 192821421245)
     * Output: "aws::CS::AmazonS3".
     *
     * Azure and Gcp use cloud service ids that don't have embedded account ids and do not need sanitization.
     *
     * @param accountSpecificCloudServiceId account specified CS id.
     * contains account id which might got hashed.
     * @return edited account specified CS id without its account id/subscription id.
     */
    public String sanitizeCloudServiceId(String accountSpecificCloudServiceId) {
        String[] csIdSpecs = accountSpecificCloudServiceId.split("::");
        if (csIdSpecs.length == NUMBER_OF_CS_SPECS_OLD_FORMAT) {
            // old format is still used by AWS. This case is also needed for loading pre-8.4.1 Azure topologies with the
            // old format: azure::CS::<cloud service name>::<subscription id>.
            final String cspName = csIdSpecs[CSP_NAME_INDEX];
            return cspName.equals("azure")
                ? String.format(CSP_OUTPUT_FORMAT, cspName, csIdSpecs[AZURE_CS_NAME_INDEX])
                : String.format(CSP_OUTPUT_FORMAT, cspName, csIdSpecs[AWS_CS_NAME_INDEX]);
        } else if (csIdSpecs.length == NUMBER_OF_CS_SPECS_NEW_FORMAT) {
            // new format used by Azure and GCP that do not have embedded account id.
            return accountSpecificCloudServiceId;
        } else {
            logger.warn("Cloud Service: '{}' did not sanitize correctly - "
                + "has wrong id format in its costData", accountSpecificCloudServiceId);
            return accountSpecificCloudServiceId;
        }
    }
}