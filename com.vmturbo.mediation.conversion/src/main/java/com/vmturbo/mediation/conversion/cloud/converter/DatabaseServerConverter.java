package com.vmturbo.mediation.conversion.cloud.converter;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.conversion.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.conversion.cloud.IEntityConverter;
import com.vmturbo.mediation.conversion.util.ConverterUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Converter for DatabaseServer from AWS/Azure. The original relationship is as following:
 *
 * AWS:   DB (fake) -> DBServer -> VM [profileId is in DBServer]
 * Azure: DB -> DBServer (fake) -> DC [profileId is in DB]
 *
 * For Azure, it just returns true since DBServer is fake and need to be removed.
 * For AWS, it finds or creates ComputeTier for each distinct DBServer profile, changes bought
 * commodities from VM to CT, merges sold commodities from VM to CT, and connect to Region.
 */
public class DatabaseServerConverter implements IEntityConverter {

    private SDKProbeType probeType;

    public DatabaseServerConverter(@Nonnull SDKProbeType probeType) {
        this.probeType = probeType;
    }

    @Override
    public boolean convert(@Nonnull EntityDTO.Builder entity, @Nonnull CloudDiscoveryConverter converter) {
        // if from Azure, then DBServer is fake, and doesn't have profileId
        // return false so it will be removed
        if (probeType == SDKProbeType.AZURE) {
            return false;
        }

        // aws: change provider from fake VM to DatabaseServerTier
        List<CommodityBought> newCommodityBoughtList = entity.getCommoditiesBoughtList().stream()
                .map(commodityBought -> {
                    CommodityBought.Builder cbBuilder = commodityBought.toBuilder();
                    // remove Application commodity
                    ConverterUtils.removeApplicationCommodity(cbBuilder);

                    String providerId = commodityBought.getProviderId();
                    EntityDTO provider = converter.getRawEntityDTO(providerId);
                    EntityType providerEntityType = provider.getEntityType();

                    if (providerEntityType == EntityType.VIRTUAL_MACHINE) {
                        // find AZ of the VM and connect DBS to AZ
                        provider.getCommoditiesBoughtList().stream()
                                .filter(c -> {
                                    final EntityType providerType =
                                        converter.getRawEntityDTO(c.getProviderId()).getEntityType();
                                    return providerType == EntityType.PHYSICAL_MACHINE ||
                                        providerType == EntityType.AVAILABILITY_ZONE;
                                })
                                .findAny()
                                .ifPresent(c -> entity.addLayeredOver(c.getProviderId()));


                        // change commodity provider from VM to DST
                        cbBuilder.setProviderId(entity.getProfileId());
                        cbBuilder.setProviderType(EntityType.DATABASE_SERVER_TIER);
                    }

                    return cbBuilder.build();
                }).collect(Collectors.toList());

        // set new commodities bought
        entity.clearCommoditiesBought();
        entity.addAllCommoditiesBought(newCommodityBoughtList);


        // DBServer owned by business account
        converter.ownedByBusinessAccount(entity.getId());

        return true;
    }
}
