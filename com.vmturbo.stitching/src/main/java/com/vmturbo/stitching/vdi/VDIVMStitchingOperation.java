package com.vmturbo.stitching.vdi;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.BusinessUserData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.SessionData;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommodityBoughtMetadata;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingPoint;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;

/**
 * A Stitching operation that stitches the VDI discovered VM with the underlying VCenter VM.
 */
public class VDIVMStitchingOperation extends VDIStitchingOperation {

    private final Logger logger = LogManager.getLogger();

    public VDIVMStitchingOperation() {
        super(EntityType.VIRTUAL_MACHINE,
                ImmutableSet.of(CommodityType.ACTIVE_SESSIONS),
                ImmutableList.of(
                        CommodityBoughtMetadata.newBuilder()
                            .setProviderType(EntityType.DESKTOP_POOL)
                            .setReplacesProvider(EntityType.VIRTUAL_DATACENTER)
                            .addCommodityMetadata(CommodityType.CPU_ALLOCATION)
                            .addCommodityMetadata(CommodityType.MEM_ALLOCATION)
                            .build(),
                        CommodityBoughtMetadata.newBuilder()
                                .setProviderType(EntityType.STORAGE)
                                .setReplacesProvider(EntityType.STORAGE)
                                .addCommodityMetadata(CommodityType.STORAGE_CLUSTER)
                                .build(),
                        CommodityBoughtMetadata.newBuilder()
                                .setProviderType(EntityType.PHYSICAL_MACHINE)
                                .setReplacesProvider(EntityType.PHYSICAL_MACHINE)
                                .addCommodityMetadata(CommodityType.CLUSTER)
                                .build()
                        ));
    }


    @Nonnull
    @Override
    public TopologicalChangelog stitch(@Nonnull final Collection<StitchingPoint> stitchingPoints,
                                       @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        super.stitch(stitchingPoints, resultBuilder);
        //In addition to the standard stitching, this will also resolve the VM OID
        // for the session data in the business users consumed by this VM.
        for (StitchingPoint stitchingPoint : stitchingPoints) {
            resolveVMCloneSourceReference(stitchingPoint, resultBuilder);
        }
        return resultBuilder.build();

    }

    private void resolveVMCloneSourceReference(final StitchingPoint stitchingPoint,
                                               final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        StitchingEntity vdiVMEntity = stitchingPoint.getInternalEntity();
        final Collection<? extends StitchingEntity> vcVMEntities = stitchingPoint.getExternalMatches();
        if (stitchingPoint.getExternalMatches().size() == 0) {
            logger.error("No external matches found for internal entity {}",
                    stitchingPoint.getInternalEntity().getEntityBuilder().toString());
            return;
        }
        if (vcVMEntities.size() > 1) {
            logger.warn("Encountered more than one VM with the same uuid for stitching point" +
                            " and will continuing stitching with the first match. ",
                    stitchingPoint.getInternalEntity().getEntityBuilder().toString());
        }
        final StitchingEntity vcVMEntity = vcVMEntities.stream().findFirst().get();
        for (StitchingEntity consumerEntity : vdiVMEntity.getConsumers()) {

            if (consumerEntity.getEntityType() == EntityType.BUSINESS_USER &&
                    consumerEntity.getEntityBuilder().hasBusinessUserData()) {
                resultBuilder.queueUpdateEntityAlone(consumerEntity, updateSessionDataVMId -> {
                    final BusinessUserData.Builder buDataBuilder = consumerEntity.getEntityBuilder()
                            .getBusinessUserData().toBuilder();
                    List<SessionData> newSessionList = new ArrayList<>();
                    boolean vmSessionFound = false;
                    for (SessionData sessionData : buDataBuilder.getSessionDataList()) {
                        if (sessionData.hasVirtualMachine()) {
                            if (sessionData.getVirtualMachine().equals(vdiVMEntity.getLocalId())) {
                                newSessionList.add(sessionData.toBuilder()
                                .setVirtualMachine(String.valueOf(vcVMEntity.getOid()))
                                .build());
                                vmSessionFound = true;
                            } else {
                                newSessionList.add(sessionData.toBuilder().build());
                            }
                        } else {
                            logger.error(" No Virtual machine data in session data for" +
                                            " consumer {} of VM {}. Session data  retained as is" +
                                            " for the VM oid resolution.",
                                    consumerEntity.getOid(), vcVMEntity.getOid());
                            newSessionList.add(sessionData.toBuilder().build());
                        }
                    }
                    if (!vmSessionFound) {
                        logger.error(" No session data was found for" + " consumer {} of VM {}" +
                                        " even though the BU is consuming from this VM.",
                                consumerEntity.getOid(), vcVMEntity.getOid());
                    }
                    buDataBuilder.clearSessionData();
                    buDataBuilder.addAllSessionData(newSessionList);
                    consumerEntity.getEntityBuilder().setBusinessUserData(buDataBuilder.build());
                });
            }
        }
    }
}
