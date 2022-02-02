package com.vmturbo.cloud.common.commitment;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommoditiesBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommodityBought;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * A journal for recording discrete vector amounts commitment coverage, providing for validation of building of the amounts
 * into a {@link CloudCommitmentAmount}.
 */
@NotThreadSafe
public class CommitmentAmountJournal {

    private static final CloudCommitmentCoverageTypeInfo COUPON_SUPPORTED_TYPE = CloudCommitmentCoverageTypeInfo.newBuilder()
            .setCoverageType(CloudCommitmentCoverageType.COUPONS)
            // intentionally do not set subtype
            .build();

    private static final CloudCommitmentCoverageTypeInfo COMMODITIES_SUPPORTED_TYPE = CloudCommitmentCoverageTypeInfo.newBuilder()
            .setCoverageType(CloudCommitmentCoverageType.COMMODITY)
            // intentionally do not set subtype
            .build();

    private final Map<CloudCommitmentCoverageTypeInfo, Double> commitmentAmountVectorMap = new ConcurrentHashMap<>();

    private final SetOnce<CloudCommitmentCoverageTypeInfo> supportedType = new SetOnce<>();

    private CommitmentAmountJournal(@Nonnull CloudCommitmentAmount initialAmount) {
        CommitmentAmountUtils.groupByKey(initialAmount)
                .forEach(this::addAmount);
    }

    /**
     * Creates a new empty journal.
     * @return Creates a new empty journal.
     */
    public static CommitmentAmountJournal create() {
        return new CommitmentAmountJournal(CommitmentAmountUtils.EMPTY_COMMITMENT_AMOUNT);
    }

    /**
     * Creates a new journal, initialized with {@code initialAmount}.
     * @param initialAmount The initial {@link CloudCommitmentAmount}.
     * @return The created journal.
     */
    public static CommitmentAmountJournal create(@Nonnull CloudCommitmentAmount initialAmount) {
        return new CommitmentAmountJournal(initialAmount);
    }

    /**
     * Reduces the collection of {@code journals} to a newly created aggregate journal. Requires that all
     * journals have the same base type.
     *
     * @param journals The journals to reduce.
     * @return The new aggregate journal.
     */
    public static CommitmentAmountJournal reduce(@Nonnull Collection<CommitmentAmountJournal> journals) {
        final CommitmentAmountJournal aggregateJournal = CommitmentAmountJournal.create();

        journals.forEach(journal ->
                journal.getVectorMap().forEach(aggregateJournal::addAmount));

        return aggregateJournal;
    }

    /**
     * Adds the amount of {@code vectorType} to this journal. If the {@code vectorType} conflicts with the base
     * type of a prior amount added to this journal, an exception will be thrown.
     * @param vectorType The vector type of {@code amountToAdd}.
     * @param amountToAdd The amount to add.
     */
    public void addAmount(@Nonnull CloudCommitmentCoverageTypeInfo vectorType,
                          double amountToAdd) {

        if (vectorType.hasCoverageType()) {

            verifyVectorTypeIsSupported(vectorType, supportedType.ensureSet(() -> resolveSupportedType(vectorType)));

            commitmentAmountVectorMap.compute(vectorType, (type, baseAmount) ->
                    baseAmount != null && Double.isFinite(baseAmount)
                            ? baseAmount + amountToAdd
                            : amountToAdd);

        } else if (amountToAdd != 0.0) {
            throw new IllegalArgumentException();
        } // else it is an empty commitment amount
    }

    /**
     * Gets the recorded amount added to this journal for {@code vectorType}. If no value has been added
     * for that type, zero will be returned.
     * @param vectorType The commitment amount vector type.
     * @return The amount associated with the specified vector type added to this journal.
     */
    public double getVectorAmount(@Nonnull CloudCommitmentCoverageTypeInfo vectorType) {

        return commitmentAmountVectorMap.getOrDefault(vectorType, 0.0);
    }

    /**
     * An unmodifiable map of commitments amounts added to this journal, broken down by the vector type.
     * @return An unmodifiable map of commitments amounts added to this journal, broken down by the vector type.
     */
    public Map<CloudCommitmentCoverageTypeInfo, Double> getVectorMap() {
        return Collections.unmodifiableMap(commitmentAmountVectorMap);
    }

    /**
     * Converts the commitment amounts added to this journal into a {@link CloudCommitmentAmount}.
     * @return The {@link CloudCommitmentAmount}.
     */
    @Nonnull
    public CloudCommitmentAmount getCommitmentAmount() {
        return supportedType.getValue().map(type -> {

            switch (type.getCoverageType()) {

                case COUPONS:
                    return CloudCommitmentAmount.newBuilder()
                            .setCoupons(commitmentAmountVectorMap.get(
                                    Iterables.getOnlyElement(commitmentAmountVectorMap.keySet())))
                            .build();
                case SPEND_COMMITMENT:
                    return CloudCommitmentAmount.newBuilder()
                            .setAmount(CurrencyAmount.newBuilder()
                                    .setCurrency(type.getCoverageSubtype()).setAmount(
                                            commitmentAmountVectorMap.get(
                                                    Iterables.getOnlyElement(commitmentAmountVectorMap.keySet()))))
                            .build();
                case COMMODITY:
                    return CloudCommitmentAmount.newBuilder()
                            .setCommoditiesBought(CommittedCommoditiesBought.newBuilder()
                                    .addAllCommodity(commitmentAmountVectorMap.entrySet()
                                            .stream()
                                            .map(commodityEntry -> CommittedCommodityBought.newBuilder()
                                                    .setCommodityType(CommodityType.forNumber(commodityEntry.getKey().getCoverageSubtype()))
                                                    .setCapacity(commodityEntry.getValue())
                                                    .build())
                                            .collect(ImmutableList.toImmutableList())))
                            .build();
                default:
                    throw new UnsupportedOperationException(
                            String.format("Cloud commitment amount type %s is not supported", type.getCoverageType()));
            }

        }).orElse(CommitmentAmountUtils.EMPTY_COMMITMENT_AMOUNT);
    }

    private CloudCommitmentCoverageTypeInfo resolveSupportedType(@Nonnull CloudCommitmentCoverageTypeInfo vectorType) {

        switch (vectorType.getCoverageType()) {
            case COUPONS:
                return COUPON_SUPPORTED_TYPE;
            case SPEND_COMMITMENT:
                return vectorType;
            case COMMODITY:
                return COMMODITIES_SUPPORTED_TYPE;
            default:
                throw new UnsupportedOperationException();
        }
    }

    private void verifyVectorTypeIsSupported(@Nonnull CloudCommitmentCoverageTypeInfo vectorType,
                                             @Nonnull CloudCommitmentCoverageTypeInfo supportedBaseType) {

        final boolean isSupported = supportedBaseType.getCoverageType() == vectorType.getCoverageType()
                && (!supportedBaseType.hasCoverageSubtype() || supportedBaseType.getCoverageSubtype() == vectorType.getCoverageSubtype());

        Verify.verify(isSupported, "Unsupported vector type of %s. Supported base type is %s",
                vectorType, supportedBaseType);
    }
}
