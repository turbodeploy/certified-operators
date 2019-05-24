package com.vmturbo.stitching.vdi;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
public class VMStitchingOperation extends VDIStitchingOperation {

    private final Logger logger = LogManager.getLogger();

    public VMStitchingOperation() {
        super(EntityType.VIRTUAL_MACHINE,
                ImmutableSet.of(CommodityType.ACTIVE_SESSIONS),
                CommodityBoughtMetadata.newBuilder()
                        .setProviderType(EntityType.DESKTOP_POOL)
                        .setReplacesProvider(EntityType.VIRTUAL_DATACENTER)
                        .addCommodityMetadata(CommodityType.CPU_ALLOCATION)
                        .addCommodityMetadata(CommodityType.MEM_ALLOCATION)
                        .build());
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
        if (vcVMEntities.size() > 1) {
            logger.warn("Encountered more than one VM with the same uuid for stitching point",
                    stitchingPoint.getInternalEntity().getEntityBuilder().toString());
        }
        final StitchingEntity vcVMEntity = vcVMEntities.stream().findFirst().get();
        for (StitchingEntity consumerEntity : vdiVMEntity.getConsumers()) {

            if (consumerEntity.getEntityType() == EntityType.BUSINESS_USER &&
                    consumerEntity.getEntityBuilder().hasBusinessUserData()) {
                resultBuilder.queueUpdateEntityAlone(consumerEntity, updateSessionDataVMId -> {
                    final BusinessUserData.Builder buDataBuilder = consumerEntity.getEntityBuilder()
                            .getBusinessUserData().toBuilder();
                    List<SessionData> newSessioList = new ArrayList<>();
                    for (SessionData sessionData : buDataBuilder.getSessionDataList()) {
                        if (sessionData.hasVirtualMachine()) {
                            newSessioList.add(sessionData.toBuilder()
                                    .setVirtualMachine(vcVMEntity.getOid() + "").build());
                        }
                    }
                    buDataBuilder.clearSessionData();
                    buDataBuilder.addAllSessionData(newSessioList);
                    consumerEntity.getEntityBuilder().setBusinessUserData(buDataBuilder.build());
                });
            }
        }
    }
}
