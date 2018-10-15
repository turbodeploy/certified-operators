package com.vmturbo.topology.processor.cost;

import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.CLOUD_COST_EXPENSES_SECTION;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.CLOUD_COST_UPLOAD_TIME;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.UPLOAD_REQUEST_BUILD_STAGE;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.UPLOAD_REQUEST_UPLOAD_STAGE;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
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
import com.vmturbo.common.protobuf.cost.Cost.UploadAccountExpensesResponse;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc.RIAndExpenseUploadServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.TargetCostData;
import com.vmturbo.topology.processor.stitching.StitchingContext;

/**
 * RICostDataUploader collects Account expense data and Reserved Instance coverage and purchases
 * data from probe discoveries and sends it to the cost component.
 */
public class AccountExpensesUploader {
    private static final Logger logger = LogManager.getLogger();

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
     * track the last upload time. We want to space the uploads apart according to the "minimum
     * account expenses upload interval" setting
     */
    private Instant lastUploadTime = null;

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
     * it has been converted to a topology map. Ths is because a lot of the data we need is in the
     * raw cloud entity data, much of which we lose in the conversion to topology map.
     *
     * We will be cross-referencing data from the cost DTO's, non-market entities, and topology
     * entities (in stitching entity form), from the billing and discovery probes. So there may be
     * some sensitivity to discovery mismatches between billing and discovery probe data.
     *
     * @param topologyInfo
     * @param stitchingContext
     */
    public synchronized void uploadAccountExpenses(Map<Long, TargetCostData> costDataByTarget,
                                                   TopologyInfo topologyInfo,
                                                   StitchingContext stitchingContext,
                                                   CloudEntitiesMap cloudEntitiesMap) {
        // check if we are within our minimum upload interval. Note that we are not persisting the
        // last upload time stamp, and starting a new set of upload interval checks every time the
        // component starts. Component restarts should be rare, so we don't expect this to be much
        // of an issue, but if this does become a problem we can add persistence of the last upload
        // time.
        if (lastUploadTime != null) {
            if (clock.instant().minus(minimumAccountExpensesUploadIntervalMins, ChronoUnit.MINUTES)
                    .isBefore(lastUploadTime)) {
                // we are within the minimum upload interval -- we need to skip this upload.
                logger.info("Skipping upload of cost data, since we're within minimum upload " +
                        "interval since the last upload at {}", lastUploadTime.toString());
                return;
            }
        }
        // we've passed the throttling checks.

        DataMetricTimer buildTimer = CLOUD_COST_UPLOAD_TIME.labels(CLOUD_COST_EXPENSES_SECTION,
                UPLOAD_REQUEST_BUILD_STAGE).startTimer();

        // get the account expenses
        Map<Long,AccountExpenses.Builder> accountExpensesByOid = createAccountExpenses(cloudEntitiesMap,
                stitchingContext, costDataByTarget);
        logger.debug("Created {} AccountExpenses.", accountExpensesByOid.size());

        // assemble and execute the upload
        UploadAccountExpensesRequest.Builder requestBuilder = UploadAccountExpensesRequest.newBuilder();
        accountExpensesByOid.values().forEach(expenses -> {
            requestBuilder.addAccountExpenses(expenses.build());
        });
        buildTimer.observe();
        logger.debug("Building account expenses upload request took {} secs", buildTimer.getTimeElapsedSecs());

        // check the last processed hash to see if we should upload again
        long lastProcessedHash = costServiceClient.getAccountExpensesChecksum(
                GetAccountExpensesChecksumRequest.getDefaultInstance()).getChecksum();

        // generate a hash of the intermediate build - we won't add topology id in yet because that
        // is expected to change each iteration.
        long requestHash = requestBuilder.build().hashCode();
        if (requestHash != lastProcessedHash) {
            // set the topology id after calculating the hash, so it doesn't affect the hash calc
            requestBuilder.setTopologyId(topologyInfo.getTopologyId());
            requestBuilder.setChecksum(requestHash);
            requestBuilder.setCreatedTime(System.currentTimeMillis());

            logger.debug("Request hash [{}] is different from last processed hash [{}], will upload this request.",
                    Long.toUnsignedString(requestHash), Long.toUnsignedString(lastProcessedHash));
            try (DataMetricTimer uploadTimer = CLOUD_COST_UPLOAD_TIME.labels(
                    CLOUD_COST_EXPENSES_SECTION, UPLOAD_REQUEST_UPLOAD_STAGE).startTimer()) {
                // we should probably upload empty data too, if we are relying on this as a way to
                // "clear" data when all cloud targets are removed.
                UploadAccountExpensesResponse response = costServiceClient.uploadAccountExpenses(requestBuilder.build());
                logger.debug("Account expenses upload took {} secs", uploadTimer.getTimeElapsedSecs());
                lastUploadTime = clock.instant();
            } catch (Exception e) {
                logger.error("Error uploading cloud account expenses", e);
            }
        } else {
            logger.info("Cloud Account Expenses upload step calculated same hash as the last processed hash -- will skip this upload.");
        }
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
        Map<Long,AccountExpenses.Builder> expensesByAccountOid = new HashMap<>();
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
            long discoveryTime = costDataByTargetIdSnapshot.get(stitchingEntity.getTargetId())
                    .discovery.getCompletionTime()
                    .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            expensesByAccountOid.put(stitchingEntity.getOid(), AccountExpenses.newBuilder()
                    .setAssociatedAccountId(stitchingEntity.getOid())
                    .setExpenseReceivedTimestamp(discoveryTime));
        });

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
                    String sharedCloudServiceLocalId = sanitizeCloudServiceId(costData.getId(),
                            costData.getAccountId());
                    Long cloudServiceOid = cloudEntitiesMap.get(sharedCloudServiceLocalId);
                    if (cloudServiceOid == null) {
                        logger.warn("Couldn't find a cloud service oid for service {}", sharedCloudServiceLocalId);
                        cloudServiceOid = 0L;
                    }
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

        return expensesByAccountOid;
    }

    /**
     * This function takes a cloud service id -- that may contain an account id in it -- and removes
     * the account id from it, if it exists. The resulting string should be usable as an account-
     * agnostic cloud service local id. e.g.:
     *
     * Input: "aws::192821421245::CS::AmazonS3" (account id 192821421245)
     * Output: "aws::CS::AmazonS3"
     *
     * @param accountSpecificCloudServiceId
     * @return
     */
    public String sanitizeCloudServiceId(String accountSpecificCloudServiceId, String accountId) {
        return accountSpecificCloudServiceId.replace("::"+ accountId, "");
    }

}
