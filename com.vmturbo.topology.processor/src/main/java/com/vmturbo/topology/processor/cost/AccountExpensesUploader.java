package com.vmturbo.topology.processor.cost;

import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.CLOUD_COST_EXPENSES_SECTION;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.CLOUD_COST_UPLOAD_TIME;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.UPLOAD_REQUEST_BUILD_STAGE;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.UPLOAD_REQUEST_UPLOAD_STAGE;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.ServiceExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.TierExpenses;
import com.vmturbo.common.protobuf.cost.Cost.GetAccountExpensesChecksumRequest;
import com.vmturbo.common.protobuf.cost.Cost.UploadAccountExpensesRequest;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc.RIAndExpenseUploadServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.proactivesupport.DataMetricGauge;
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
    private static final int NUMBER_OF_CS_SPECS = 4;

    /**
     * Add cloud service spent metric ratios to topology processor metrics end point.
     * This should be updated during broadcast hourly based on shouldSkipProcessingExpenses()
     * or if there is new cost data information.
     */
    public static final DataMetricGauge CLOUD_SPENT_BREAKDOWN_GAUGE = DataMetricGauge.builder()
            .withName(StringConstants.METRICS_TURBO_PREFIX + "cloud_spend_ratio")
            .withHelp("Cloud Spent Ratio.")
            .withLabelNames("type", "service")
            .build()
            .register();

    /**
     * Tracks the amount of Business Accounts.
     */
    public static final DataMetricGauge BUSINESS_ACCOUNTS_GAUGE = DataMetricGauge.builder()
            .withName(StringConstants.METRICS_TURBO_PREFIX + "business_accounts")
            .withHelp("Business Account Quantity.")
            .build()
            .register();

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
        Map<Long, AccountExpenses.Builder> accountExpensesByOid = createAccountExpenses(cloudEntitiesMap,
                stitchingContext, costDataByTarget);
        logger.debug("Created {} AccountExpenses.", accountExpensesByOid.size());

        // assemble and execute the upload
        UploadAccountExpensesRequest.Builder requestBuilder = UploadAccountExpensesRequest.newBuilder();
        accountExpensesByOid.values().forEach(expenses -> {
            requestBuilder.addAccountExpenses(expenses.build());
        });
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
    public Map<Long, AccountExpenses.Builder> createAccountExpenses(
            CloudEntitiesMap cloudEntitiesMap, StitchingContext stitchingContext,
            Map<Long, TargetCostData> costDataByTargetIdSnapshot) {

        // create the initial AccountExpenses objects w/receipt time based on target discovery time
        // in the future, this will be based on a billing time when that data is available.
        Map<Long, AccountExpenses.Builder> expensesByAccountOid = new HashMap<>();
        stitchingContext.getEntitiesOfType(EntityType.BUSINESS_ACCOUNT).forEach(stitchingEntity -> {
            // TODO: use the discovery time as the expense received time until we can find a better
            // expense-related time source. (e.g. the billing data itself). The time will be
            // converted from a local datetime to unix epoch millis.
            if (!costDataByTargetIdSnapshot.containsKey(stitchingEntity.getTargetId())) {
                // it's possible that we don't have price or billing data for this target, since the
                // billing discoveries have a different cycle time.
                logger.warn("Not creating account expenses for account {} since no targetCostData for target id {}",
                        stitchingEntity.getLocalId(),
                        stitchingEntity.getTargetId());
                return;
            }
            expensesByAccountOid.put(stitchingEntity.getOid(), AccountExpenses.newBuilder()
                    .setAssociatedAccountId(stitchingEntity.getOid()));
        });

        Map<String, Double> cloudServiceSpentMap = new HashMap<>();
        // Find the service expenses from the cost data objects, and assign them to the
        // account expenses created above.
        costDataByTargetIdSnapshot.forEach((targetId, targetCostData) -> {
            targetCostData.costDataDTOS.forEach(costData -> {
                // find the expenses builder for the associated account.

                if (!costData.hasAccountId()) {
                    logger.warn("No account id set for costData object {} with cost {}",
                            costData.getId(), costData.getCost());
                    return;
                }
                Long accountOid = cloudEntitiesMap.getOrDefault(costData.getAccountId(),
                        cloudEntitiesMap.getFallbackAccountOid(targetId));
                if (!cloudEntitiesMap.containsKey(costData.getAccountId())) {
                    logger.warn("Couldn't find biz account oid for local id {}, using fallback account {}.",
                            costData.getAccountId(),
                            accountOid);
                }
                if (!expensesByAccountOid.containsKey(accountOid)) {
                    logger.warn("No expense builder for account oid {}.", accountOid);
                    return;
                }

                AccountExpenses.Builder accountExpensesBuilder = expensesByAccountOid.get(accountOid);

                // Set the usage date for the account expenses for this account to be the value of
                // the first costDataDTO which has a usage date.
                // Since all the usage data is from yesterday and the smallest time frame is DAY,
                // all the expenses should have the same date.
                if (!accountExpensesBuilder.hasExpensesDate() && costData.hasUsageDate()) {
                    accountExpensesBuilder.setExpensesDate(costData.getUsageDate());
                    logger.info("usageTime for account {}: {}", accountOid, costData.getUsageDate());
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
        pushCloudServiceSpentMetrics(cloudServiceSpentMap);
        BUSINESS_ACCOUNTS_GAUGE.setData((double)(expensesByAccountOid.size()));
        return expensesByAccountOid;
    }

    /**
     * This function takes a cloud service id -- that may contain an account id in it -- and removes
     * the account id from it, if it exists. The resulting string should be usable as an account-
     * agnostic cloud service local id.
     * If the input cloud service id does not formatted properly,
     * the output will remain the same and appropriate log message will be printed.
     * currently, the Input Id format for AWS and Azure is not consistent e.g.:
     *
     * Input: "aws::192821421245::CS::AmazonS3" (account id 192821421245)
     * Output: "aws::CS::AmazonS3".
     * Input: "azure::CS::Storage::26080bd2-d98f-4420-a737-9de8"
     * (subscription id 26080bd2-d98f-4420-a737-9de8).
     * Output: "azure::CS::Storage"
     *
     * @param accountSpecificCloudServiceId account specified CS id.
     * contains account id which might got hashed.
     * @return edited account specified CS id without its account id/subscription id.
     */
    public String sanitizeCloudServiceId(String accountSpecificCloudServiceId) {
        String[] csIdSpecs = accountSpecificCloudServiceId.split("::");
        if (csIdSpecs.length != NUMBER_OF_CS_SPECS) {
            logger.warn(
                "Cloud Service: '{}' did not sanitize correctly - "
                        + "has wrong id format in its costData", accountSpecificCloudServiceId);
            return accountSpecificCloudServiceId;
        }
        final String cspName = csIdSpecs[CSP_NAME_INDEX];
        return cspName.equals("azure")
            ? String.format(CSP_OUTPUT_FORMAT, cspName, csIdSpecs[AZURE_CS_NAME_INDEX])
            : String.format(CSP_OUTPUT_FORMAT, cspName, csIdSpecs[AWS_CS_NAME_INDEX]);
    }

    // Expose the ratio spent per cloud service.
    private void pushCloudServiceSpentMetrics(Map<String, Double> cloudServiceSpentMap) {
        CLOUD_SPENT_BREAKDOWN_GAUGE.getLabeledMetrics().forEach((key, val) -> {
            val.setData(0.0);
        });
        double totalAccountCloudServiceExpenses = cloudServiceSpentMap.values().stream().mapToDouble(v -> v).sum();
        if (totalAccountCloudServiceExpenses > 0.0) {
            cloudServiceSpentMap.forEach((serviceId, spent) -> {
                String[] csIdSpecs = serviceId.split("::");
                // Ids included in the cloudServiceSpentMap are already sanitized and will have
                // 3 partitions around the "::". cspName::"CS"::cloudServiceName.
                if (csIdSpecs.length == 3) {
                    final String cspName = csIdSpecs[0];
                    final String cloudServiceName = csIdSpecs[csIdSpecs.length - 1];
                    CLOUD_SPENT_BREAKDOWN_GAUGE.labels(cspName, cloudServiceName).setData(spent / totalAccountCloudServiceExpenses);
                } else {
                    CLOUD_SPENT_BREAKDOWN_GAUGE.labels(serviceId, serviceId).setData(spent / totalAccountCloudServiceExpenses);
                }
            });
        }
    }
}


