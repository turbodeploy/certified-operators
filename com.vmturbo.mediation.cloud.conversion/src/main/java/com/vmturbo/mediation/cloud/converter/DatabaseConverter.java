package com.vmturbo.mediation.cloud.converter;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.cloud.IEntityConverter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * CloudDiscoveryConverter for Database from cloud targets. The relationship for AWS/Azure is as following:
 *
 * AWS:   DB -> DBServer -> VM [profileId is in DBServer]
 * Azure: DB -> DBServer -> DC [profileId is in DB]
 *
 * For AWS, we keep DB and DBServer, create DatabaseTier for each distinct DBServer profile,
 * switch provider of DB from VM to DatabaseTier and then remove fake VM. Also we connect DB
 * and DBServer to Region.
 *
 * For Azure, we keep DB, create DatabaseTier for each distinct DB profile, switch provider of
 * DB from DBServer to DatabaseTier and then remove fake DBServer. We may change it if we want
 * to keep DBServer for Azure. Also we connect DB to Region.
 */
public class DatabaseConverter implements IEntityConverter {

    private SDKProbeType probeType;

    public DatabaseConverter(@Nonnull SDKProbeType probeType) {
        this.probeType = probeType;
    }

    @Override
    public boolean convert(@Nonnull EntityDTO.Builder entity, @Nonnull CloudDiscoveryConverter converter) {
        if (probeType == SDKProbeType.AWS) {
            // find AZ and connect it to AZ: DB -> DBServer -> VM -> AZ
            entity.getCommoditiesBoughtList().stream()
                    .filter(c -> converter.getRawEntityDTO(c.getProviderId()).getEntityType() ==
                            EntityType.DATABASE_SERVER)
                    .flatMap(c -> converter.getRawEntityDTO(c.getProviderId())
                            .getCommoditiesBoughtList().stream())
                    .filter(c -> converter.getRawEntityDTO(c.getProviderId()).getEntityType() ==
                            EntityType.VIRTUAL_MACHINE)
                    .flatMap(c -> converter.getRawEntityDTO(c.getProviderId())
                            .getCommoditiesBoughtList().stream())
                    .filter(c -> converter.getRawEntityDTO(c.getProviderId()).getEntityType() ==
                            EntityType.PHYSICAL_MACHINE)
                    .findAny()
                    .ifPresent(c -> entity.addLayeredOver(c.getProviderId()));
        } else if (probeType == SDKProbeType.AZURE) {
            // if it is from Azure, then DB has profileId, DB -> DBServer -> DC
            List<CommodityBought> newCommodityBoughtList = entity.getCommoditiesBoughtList().stream()
                    .map(commodityBought -> {
                        CommodityBought.Builder cbBuilder = commodityBought.toBuilder();

                        String providerId = commodityBought.getProviderId();
                        EntityDTO provider = converter.getRawEntityDTO(providerId);
                        EntityType providerEntityType = provider.getEntityType();

                        if (providerEntityType == EntityType.DATABASE_SERVER) {
                            // find region and connect DB to Region
                            provider.getCommoditiesBoughtList().stream()
                                    .filter(cb -> converter.getNewEntityBuilder(cb.getProviderId())
                                            .getEntityType() == EntityType.REGION)
                                    .findAny()
                                    .ifPresent(cb -> entity.addLayeredOver(cb.getProviderId()));

                            // change commodity provider from DBS to DT
                            cbBuilder.setProviderId(entity.getProfileId());
                            cbBuilder.setProviderType(EntityType.DATABASE_TIER);
                        }

                        return cbBuilder.build();
                    }).collect(Collectors.toList());

            // set new commodities bought
            entity.clearCommoditiesBought();
            entity.addAllCommoditiesBought(newCommodityBoughtList);
        }

        // DB owned by business account
        converter.ownedByBusinessAccount(entity.getId());

        return true;
    }
}
