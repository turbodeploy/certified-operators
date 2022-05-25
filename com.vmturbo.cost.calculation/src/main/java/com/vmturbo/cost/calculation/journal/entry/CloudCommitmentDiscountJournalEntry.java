package com.vmturbo.cost.calculation.journal.entry;

import static com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType.COMMODITY;
import static com.vmturbo.cost.calculation.journal.CostJournal.CommodityTypeFilter.INCLUDE_ALL;
import static com.vmturbo.cost.calculation.journal.CostJournal.CostSourceFilter.EXCLUDE_CLOUD_COMMITMENT_DISCOUNTS_FILTER;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableList;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.cloud.common.commitment.TopologyCommitmentData;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageVector;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.journal.CostItem;
import com.vmturbo.cost.calculation.journal.CostItem.CostSourceLink;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.journal.CostJournal.CommodityTypeFilter;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.trax.Trax;
import com.vmturbo.trax.TraxNumber;

/**
 * A {@link QualifiedJournalEntry} for discounted on demand rates due to cloud commitment coverage.
 *
 * @param <E> see {@link QualifiedJournalEntry}
 */
@Immutable
public class CloudCommitmentDiscountJournalEntry<E> implements QualifiedJournalEntry<E> {

    private final TopologyCommitmentData commitmentData;
    private final CloudCommitmentCoverageVector coverageVector;
    private final CostCategory costCategory;
    private final CostSource costSource;

    /**
     * Constructor.
     * @param commitmentData the cloud commitment covering part of this entity's costs
     * @param coverageVector how much of the total capacity is covered for this coverage type
     * @param costCategory the category of cost to be discounted
     * @param costSource the source of the discount
     */
    public CloudCommitmentDiscountJournalEntry(
                    @Nonnull final TopologyCommitmentData commitmentData,
                    @Nonnull final CloudCommitmentCoverageVector coverageVector,
                    @Nonnull final CostCategory costCategory,
                    @Nonnull final CostSource costSource) {

        this.commitmentData = commitmentData;
        this.coverageVector = coverageVector;
        this.costCategory = costCategory;
        this.costSource = costSource;
        if (!coverageVector.getVectorType().getCoverageType().equals(COMMODITY)) {
            throw new IllegalArgumentException("CloudCommitmentDiscountJournalEntry only supports commodity-based discounts at this time.");
        }
        if (coverageVector.getCapacity() <= 0 || coverageVector.getUsed() <= 0) {
            throw new IllegalArgumentException("Invalid CoverageVector - both capacity and used must be greater than zero.");
        }
    }

    @Override
    public Collection<CostItem> calculateHourlyCost(
                    @NotNull EntityInfoExtractor<E> infoExtractor,
                    @NotNull DiscountApplicator<E> discountApplicator,
                    @NotNull CostJournal.RateExtractor rateExtractor) {

        CommodityTypeFilter commodityTypeFilter = commodityType()
                        .map(CommodityTypeFilter::includeOnly)
                        .orElse(INCLUDE_ALL);
        Collection<CostItem> costItems = rateExtractor.lookupCostWithFilter(
                        this.costCategory,
                        EXCLUDE_CLOUD_COMMITMENT_DISCOUNTS_FILTER,
                        commodityTypeFilter);

        return costItems.stream().map(costItem -> {
            TraxNumber discount = costItem.cost()
                            .times(getCoverageRatio().times(-1).compute())
                            .compute(String.format(
                                            "Cloud Commitment discounted %s cost (Cost Source Link = %s",
                                            this.costCategory,
                                            costItem.costSourceLink()));

            return CostItem.builder()
                            .costSourceLink(CostSourceLink.of(
                                            this.costSource,
                                            Optional.of(costItem.costSourceLink())))
                            .cost(discount)
                            .commodity(commodityType())
                            .build();
        }).collect(ImmutableList.toImmutableList());
    }

    /**
     * The ratio of used coverage to coverage capacity of the covered entity.
     * @return the coverage ratio.
     */
    @NotNull
    public TraxNumber getCoverageRatio() {
        return Trax.trax(this.coverageVector.getUsed())
                        .dividedBy(this.coverageVector.getCapacity())
                        .compute(String.format(
                                        "Cloud Commitment Coverage Ratio: Used %f divided by Capacity %f",
                                        this.coverageVector.getUsed(),
                                        this.coverageVector.getCapacity()));
    }

    @Nonnull
    @Override
    public Optional<CostSource> getCostSource() {
        return Optional.of(costSource);
    }

    @Nonnull
    @Override
    public CostCategory getCostCategory() {
        return costCategory;
    }

    @Override
    public Optional<CommodityType> commodityType() {
        return Optional.ofNullable(CommodityType.forNumber(coverageVector.getVectorType()
                                                     .getCoverageSubtype()));
    }

    public TopologyCommitmentData getCommitmentData() {
        return commitmentData;
    }


    public CloudCommitmentCoverageVector getCoverageVector() {
        return coverageVector;
    }

    @Override
    public int hashCode() {
        return Objects.hash(commitmentData, coverageVector, costCategory, costSource);
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof CloudCommitmentDiscountJournalEntry)) {
            return false;
        } else if (obj == this) {
            return true;
        } else {
            final CloudCommitmentDiscountJournalEntry other = (CloudCommitmentDiscountJournalEntry)obj;
            return new EqualsBuilder()
                            .append(commitmentData, other.commitmentData)
                            .append(coverageVector, other.coverageVector)
                            .append(costCategory, other.costCategory)
                            .append(costSource, other.costSource)
                            .build();
         }
    }
}
