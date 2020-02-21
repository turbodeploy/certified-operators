package com.vmturbo.stitching.vdi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.BusinessUserData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.SessionData;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommodityBoughtMetadata;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;

/**
 * A Stitching operation that stitches the VDI discovered VM with the underlying VCenter VM.
 */
public class VDIVMStitchingOperation extends VDIStitchingOperation {

    private final Logger logger = LogManager.getLogger();

    public VDIVMStitchingOperation() {
        super(EntityType.VIRTUAL_MACHINE,
                Collections.singleton(CommodityType.ACTIVE_SESSIONS),
                Arrays.asList(
                        CommodityBoughtMetadata.newBuilder()
                            .setProviderType(EntityType.DESKTOP_POOL)
                            .setReplacesProvider(EntityType.VIRTUAL_DATACENTER)
                            .addCommodityMetadata(CommodityType.CPU_ALLOCATION)
                            .addCommodityMetadata(CommodityType.MEM_ALLOCATION)
                            .build(),
                        CommodityBoughtMetadata.newBuilder()
                            .setProviderType(EntityType.STORAGE)
                            .addCommodityMetadata(CommodityType.STORAGE_CLUSTER)
                            .build(),
                        CommodityBoughtMetadata.newBuilder()
                            .setProviderType(EntityType.PHYSICAL_MACHINE)
                            .addCommodityMetadata(CommodityType.CLUSTER)
                            .build()
                        ));
    }

    @Override
    protected void stitch(@Nonnull StitchingEntity internalEntity,
            @Nonnull StitchingEntity externalEntity,
            @Nonnull StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        super.stitch(internalEntity, externalEntity, resultBuilder);
        // In addition to the standard stitching, this will also resolve the VM OID
        // for the session data in the business users consumed by this VM.
        resolveVMCloneSourceReference(internalEntity, externalEntity, resultBuilder);
    }

    private void resolveVMCloneSourceReference(final StitchingEntity vdiVMEntity,
            final StitchingEntity vcVMEntity,
            final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        for (StitchingEntity consumerEntity : vdiVMEntity.getConsumers()) {
            if (consumerEntity.getEntityType() == EntityType.BUSINESS_USER &&
                    consumerEntity.getEntityBuilder().hasBusinessUserData()) {
                resultBuilder.queueUpdateEntityAlone(consumerEntity, updateSessionDataVMId -> {
                    final BusinessUserData.Builder buDataBuilder = consumerEntity.getEntityBuilder()
                            .getBusinessUserData().toBuilder();
                    final List<SessionData> newSessionList = new ArrayList<>();
                    boolean vmSessionFound = false;
                    for (SessionData sessionData : buDataBuilder.getSessionDataList()) {
                        if (sessionData.hasVirtualMachine()) {
                            if (sessionData.getVirtualMachine().equals(vdiVMEntity.getLocalId())) {
                                newSessionList.add(sessionData.toBuilder()
                                .setVirtualMachine(String.valueOf(vcVMEntity.getOid()))
                                .build());
                                vmSessionFound = true;
                            } else {
                                newSessionList.add(sessionData);
                            }
                        } else {
                            logger.error(" No Virtual machine data in session data for" +
                                            " consumer {} of VM {}. Session data  retained as is" +
                                            " for the VM oid resolution.",
                                    consumerEntity.getOid(), vcVMEntity.getOid());
                            newSessionList.add(sessionData);
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
