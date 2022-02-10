package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * SMA representation of a provide, either on-demand or discounted.
 * This is a template for a specific context.
 */
public class SMATemplate {

    /*
     * Instance variables set in the contstructor.
     */
    /*
     * Unique identifier of Template within the scope of a context
     */
    private final long oid;

    /*
     * name of the template; e.g. t2.micro
     * Only set in constructor and JUnit tests
     */
    private final String name;

    /*
     * name of the template family; e.g. "t2" from t2.micro
     */
    private final String family;

    private final CloudCommitmentAmount commitmentAmount;

    /*
     * The compute tier in XL data structures.  Needed to compute costs.
     */
    private TopologyEntityDTO computeTier;


    /**
     * The scaling penalty that will be applied as a tie breaker in the
     * natural template selection when the template prices are the same.
     */
    private final float scalingPenalty;

    /**
     * Constructor of the SMATemplate.
     *
     * @param oid the oid of the template. unique per context
     * @param name the template name
     * @param family the family the template belongs to.
     * @param commitmentAmount the commitment amount
     * @param computeTier link back to XL data structures for compute tier.  Needed to compute cost.
     * @param scalingPenalty The scaling penalty from the compute tier info
     *                      that will be applied as a tie breaker.
     */
    public SMATemplate(final long oid,
                       @Nonnull final String name,
                       @Nonnull final String family,
                       @Nonnull final CloudCommitmentAmount commitmentAmount,
                       final TopologyEntityDTO computeTier,
                       final float scalingPenalty) {
        this.oid = oid;
        this.name = Objects.requireNonNull(name, "name is null");
        this.family = Objects.requireNonNull(family, "family is null");
        this.commitmentAmount = commitmentAmount;
        this.computeTier = computeTier;
        this.scalingPenalty = scalingPenalty;
    }

    public void setComputeTier(final TopologyEntityDTO computeTier) {
        this.computeTier = computeTier;
    }

    @Nonnull
    public long getOid() {
        return oid;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    public String getFamily() {
        return family;
    }

    public CloudCommitmentAmount getCommitmentAmount() {
        return commitmentAmount;
    }

    public TopologyEntityDTO getComputeTier() {
        return computeTier;
    }


    /**
     * Getter for the scalingPenalty.
     *
     * @return the scaling penalty.
     */
    public float getScalingPenalty() {
        return scalingPenalty;
    }


    @Override
    public String toString() {
        return "SMATemplate{" +
            "OID='" + oid + "'" +
            ", name='" + name + '\'' +
            ", family='" + family + '\'' +
            ", commitmentAmount=" + commitmentAmount +
            ", scalingPenalty=" + scalingPenalty
                + '}';
    }

    /**
     * Determine if two templates are equivalent.  They are equivalent if they have same oid.
     * @return hash code based on oid
     */
    @Override
    public int hashCode() {
        return Objects.hash(oid);
    }

    /**
     * Determine if two templates are equivalent.  They are equivalent if they have same oid.
     * @param obj the other RI
     * @return true if the RI's are equivalent.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SMATemplate that = (SMATemplate)obj;
        return oid == that.oid;
    }
}
