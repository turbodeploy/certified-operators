package com.vmturbo.topology.processor.cost;

import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentData;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentData.CloudCommitmentDataBucket;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentData.CloudCommitmentDataBucket.CloudCommitmentDataPoint;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.UploadCloudCommitmentDataRequest;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentUploadServiceGrpc.CloudCommitmentUploadServiceBlockingStub;
import com.vmturbo.common.protobuf.cloud.CloudCommon.CloudStatGranularity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.TargetCostData;

/**
 * Upload cloud commitment data to the cost component.
 */
public class CloudCommitmentCostUploader {

    private static final Logger logger = LogManager.getLogger();

    private final CloudCommitmentUploadServiceBlockingStub client;

    CloudCommitmentCostUploader(@Nonnull final CloudCommitmentUploadServiceBlockingStub client) {
        this.client = client;
    }

    /**
     * Uploads commitment data to the cost component.
     *
     * @param topologyInfo topology info for the current broadcast.
     * @param cloudEntitiesMap nap of cloud entities in the current topology broadcast.
     * @param costDataByTargetIdSnapshot cost data cached for each target.
     */
    synchronized void uploadCloudCommitmentData(TopologyInfo topologyInfo,
                                                @Nonnull final CloudEntitiesMap cloudEntitiesMap,
                                                @Nonnull final Map<Long, TargetCostData> costDataByTargetIdSnapshot) {

        final CloudCommitmentData.Builder dataBuilder = CloudCommitmentData.newBuilder();

        costDataByTargetIdSnapshot.forEach((targetId, costData) -> {
            if (null != costData.cloudCommitmentData) {
                costData.cloudCommitmentData.forEach(nme -> {
                    if (nme.hasCloudCommitmentData()) {
                        nme.getCloudCommitmentData().getCoverageDataList().forEach(coverageBucket -> dataBuilder.addCoverageData(convertBucket(cloudEntitiesMap, coverageBucket)));
                        nme.getCloudCommitmentData().getUtilizationDataList().forEach(utilizationBucket -> dataBuilder.addUtilizationData(convertBucket(cloudEntitiesMap, utilizationBucket)));
                    }
                });
            }
        });

        final UploadCloudCommitmentDataRequest.Builder requestBuilder =
                UploadCloudCommitmentDataRequest.newBuilder()
                .setCloudCommitmentData(dataBuilder);

        client.uploadCloudCommitmentData(requestBuilder.build());
        logger.info("Uploading commit data  for {} targets for topology {}",
                costDataByTargetIdSnapshot.size(), topologyInfo.getTopologyId());
    }

    private CloudCommitmentDataBucket convertBucket(@Nonnull final CloudEntitiesMap cloudEntitiesMap,
                                                    final NonMarketEntityDTO.CloudCommitmentData.CloudCommitmentDataBucket utilizationCoverageDataBucket) {
        final CloudCommitmentDataBucket.Builder newBucket = CloudCommitmentDataBucket.newBuilder()
            .setTimestampMillis(utilizationCoverageDataBucket.getTimestampMillis())
            .setGranularity(CloudStatGranularity.valueOf(utilizationCoverageDataBucket.getGranularity().name()));
        utilizationCoverageDataBucket.getSamplesList().forEach(sample ->
            newBucket.addSample(convertSample(cloudEntitiesMap, sample)));
        return newBucket.build();
    }


    private CloudCommitmentDataPoint convertSample(@Nonnull final CloudEntitiesMap cloudEntitiesMap,
                                                   NonMarketEntityDTO.CloudCommitmentData.CloudCommitmentDataBucket.CloudCommitmentDataPoint rawSample) {

        final CloudCommitmentDataPoint.Builder newSample = CloudCommitmentDataPoint.newBuilder();
        if (rawSample.getCapacity().hasAmount()) {
            newSample.setCapacity(CloudCommitmentAmount.newBuilder()
                .setAmount(rawSample.getCapacity().getAmount()));
            if (rawSample.getUsed().hasAmount()) {
                newSample.setUsed(CloudCommitmentAmount.newBuilder()
                        .setAmount(rawSample.getUsed().getAmount()));
            } else if (rawSample.getUsed().hasCoupons()) {
                logger.warn("Dropping used for data point {} since used"
                        + " is in coupons and capacity is an amount value.", rawSample);
            }

        }
        if (rawSample.getCapacity().hasCoupons()) {
            newSample.setCapacity(CloudCommitmentAmount.newBuilder()
                .setCoupons(rawSample.getCapacity().getCoupons()));
            if (rawSample.getUsed().hasCoupons()) {
                newSample.setUsed(CloudCommitmentAmount.newBuilder()
                        .setCoupons(rawSample.getUsed().getCoupons()));
            } else if (rawSample.getUsed().hasAmount()) {
                logger.warn("Dropping used for data point {} since used"
                        + " is an amount and capacity is in coupons.", rawSample);
            }
        }
        if (rawSample.hasCommitmentId() && cloudEntitiesMap.containsKey(rawSample.getCommitmentId())) {
            newSample.setCommitmentId(cloudEntitiesMap.get(rawSample.getCommitmentId()));
        }
        if (rawSample.hasEntityId()) {
            if (cloudEntitiesMap.containsKey(rawSample.getEntityId())) {
                Long entityId = cloudEntitiesMap.get(rawSample.getEntityId());
                newSample.setEntityId(entityId);
            } else {
                logger.warn("Could not find entity OID for entity {}.", rawSample.getEntityId());

            }
        }
        if (rawSample.hasRegion()) {
            if (cloudEntitiesMap.containsKey(rawSample.getRegion())) {
                Long regionId = cloudEntitiesMap.get(rawSample.getRegion());
                newSample.setRegionId(regionId);
            } else {
                logger.warn("Could not find region OID for region {}.", rawSample.getEntityId());

            }
        }
        if (rawSample.hasService()) {
            if (cloudEntitiesMap.containsKey(rawSample.getService())) {
                Long serviceId = cloudEntitiesMap.get(rawSample.getService());
                newSample.setCloudServiceId(serviceId);
            } else {
                logger.warn("Could not find service for {}.", rawSample.getServiceProviderId());

            }

        }
        if (rawSample.hasAccountId()) {
            if (cloudEntitiesMap.containsKey(rawSample.getAccountId())) {
                Long accountId = cloudEntitiesMap.get(rawSample.getAccountId());
                newSample.setAccountId(accountId);
            } else {
                logger.warn("Could not find account for {}.", rawSample.getAccountId());

            }
        }
        if (rawSample.hasServiceProviderId()) {
            if (cloudEntitiesMap.containsKey(rawSample.getServiceProviderId())) {
                Long spId = cloudEntitiesMap.get(rawSample.getServiceProviderId());
                newSample.setServiceProviderId(spId);
            } else {
                logger.warn("Could not find service provider for {}.", rawSample.getServiceProviderId());

            }
        }

        logger.debug("Added data point {} to the cloud commitment data upload.", rawSample.toString());
        return newSample.build();
    }
}
