package com.vmturbo.stitching.billing;

import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.EntityField;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.DTOFieldSpecImpl;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.StitchingPoint;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.utilities.EntityFieldMergers;
import com.vmturbo.stitching.utilities.MergeEntities;
import com.vmturbo.stitching.utilities.MergeEntities.MergeEntitiesDetails;
import com.vmturbo.topology.graph.OwnershipGraph;

/**
 * Responsible for stitching business accounts discovered by the AWS Billing probe and business
 * accounts discovered by the AWS Probe. In particular, this operation modifies business account
 * relationships (i.e. which account owns which account).
 *
 * <p/>The AWS probe may or may not discover business account relationships, depending on the
 * permissions configured for the AWS account (inside AWS). The AWS Billing probe will always
 * discover business account relationships. So we want to add the business account relationships
 * discovered by the billing probe to the business account relationships discovered by the AWS
 * probe.
 *
 * <p/>In case of conflicts, we favour the AWS probe, since the data it has is generally more
 * up-to-date. For example, if AWS says BA1 owns BA2, but AWS Billing says no one owns BA2, we
 * assume AWS is correct, and in the final topology BA1 will own BA2.
 */
public class AwsBillingBusinessAccountStitchingOperation implements StitchingOperation<String, String> {

    private static final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public Optional<StitchingScope<StitchingEntity>> getScope(
            @Nonnull final StitchingScopeFactory<StitchingEntity> stitchingScopeFactory,
            long targetId) {
        return Optional.of(stitchingScopeFactory.probeEntityTypeScope(
            SDKProbeType.AWS.getProbeType(), EntityType.BUSINESS_ACCOUNT));
    }

    @Nonnull
    @Override
    public EntityType getInternalEntityType() {
        return EntityType.BUSINESS_ACCOUNT;
    }

    @Nonnull
    @Override
    public Optional<EntityType> getExternalEntityType() {
        return Optional.of(EntityType.BUSINESS_ACCOUNT);
    }

    @Override
    public Collection<String> getInternalSignature(@Nonnull final StitchingEntity internalEntity) {
        return Collections.singleton(internalEntity.getLocalId());
    }

    @Override
    public Collection<String> getExternalSignature(@Nonnull final StitchingEntity externalEntity) {
        return Collections.singleton(externalEntity.getLocalId());
    }

    @Nonnull
    @Override
    public TopologicalChangelog<StitchingEntity> stitch(
            @Nonnull final Collection<StitchingPoint> stitchingPoints,
            @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        final OwnershipGraph.Builder<Long> ownershipGraph = OwnershipGraph.newBuilder(id -> id);
        final Map<StitchingEntity, StitchingEntity> billingToAwsMap = new IdentityHashMap<>();
        // Go through the stitching points and build up the ownership graph from the
        // AWS accounts.
        stitchingPoints.forEach(stitchingPoint -> {
            // The business account discovered by the billing probe.
            final StitchingEntity billingAccount = stitchingPoint.getInternalEntity();

            // The business account discovered by the main AWS probe
            final Collection<? extends StitchingEntity> externalMatches = stitchingPoint.getExternalMatches();
            if (externalMatches.size() > 1) {
                logger.warn("Found more than 1 external match for the business account {}, proceeding with the " +
                    "first match", billingAccount);
            }

            final StitchingEntity awsAccount = externalMatches.iterator().next();

            billingToAwsMap.put(billingAccount, awsAccount);

            getOwnedAccounts(awsAccount).forEach((ownedAccountId, ownedAccount) -> {
                final boolean success = ownershipGraph.addOwner(awsAccount.getOid(), ownedAccountId);
                if (!success) {
                    // The graph as reported from the AWS probe has an invalid ownership relationship!
                    logger.warn("AWS probe reported inconsistent ownership relationship " +
                            "from account {} to {}", awsAccount, ownedAccount);
                    resultBuilder.queueChangeRelationships(awsAccount,
                        acct -> acct.removeConnection(ownedAccount, ConnectionType.OWNS_CONNECTION));
                }
            });
        });

        // Go through the billing accounts associated with each AWS account, and remove
        // any conflicting ownership relationships.
        billingToAwsMap.keySet().forEach((billingAccount) -> {
            // Go through the billing accounts, and remove any ownership relationships
            // that are overridden by the relationships from the AWS accounts.
            getOwnedAccounts(billingAccount).forEach((ownedOid, ownedAccount) -> {
                final boolean success = ownershipGraph.addOwner(billingAccount.getOid(), ownedOid);
                if (!success) {
                    // This ownership connection conflicts with one of the ones defined by the
                    // AWS probe.
                    //
                    // Remove the connection from the billing account.
                    logger.info("Removing ownership relationship between AWS Billing account {} " +
                        "and {} because it conflicts with the relationships reported by AWS Probe",
                        billingAccount, ownedAccount);
                    resultBuilder.queueChangeRelationships(billingAccount,
                        acct -> acct.removeConnection(ownedAccount, ConnectionType.OWNS_CONNECTION));
                }
            });
        });

        // Queue merges of billing and AWS accounts.
        // We do this AFTER we update all the relationships, so that all relationship changes
        // take effect between any of the merges.
        billingToAwsMap.forEach((billingAccount, awsAccount) -> {
            final MergeEntitiesDetails mergeEntitiesDetails = MergeEntities.mergeEntity(billingAccount)
                    .onto(awsAccount);
            String displayName = awsAccount.getEntityBuilder().getDisplayName();
            if (displayName != null && displayName.isEmpty()) {
                EntityField entityField = EntityField.newBuilder()
                        .setFieldName(SupplyChainConstants.DISPLAY_NAME).build();
                mergeEntitiesDetails.addFieldMerger(
                        EntityFieldMergers.getAttributeFieldMerger(
                                new DTOFieldSpecImpl(entityField)));
            }
            resultBuilder.queueEntityMerger(mergeEntitiesDetails);
        });

        return resultBuilder.build();
    }

    @Nonnull
    private Map<Long, StitchingEntity> getOwnedAccounts(@Nonnull final StitchingEntity account) {
        return account.getConnectedToByType()
            .getOrDefault(ConnectionType.OWNS_CONNECTION, Collections.emptySet()).stream()
            .filter(e -> e.getEntityType() == EntityType.BUSINESS_ACCOUNT)
            .collect(Collectors.toMap(StitchingEntity::getOid, Function.identity()));
    }

}
