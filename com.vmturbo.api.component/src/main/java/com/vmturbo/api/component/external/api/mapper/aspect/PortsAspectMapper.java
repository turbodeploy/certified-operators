package com.vmturbo.api.component.external.api.mapper.aspect;

import static com.vmturbo.components.common.utils.StringConstants.RELATION;
import static com.vmturbo.components.common.utils.StringConstants.RELATION_BOUGHT;
import static com.vmturbo.components.common.utils.StringConstants.RELATION_SOLD;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.MessageOrBuilder;

import com.vmturbo.api.component.external.api.util.CommodityCommonFieldsExtractor;
import com.vmturbo.api.component.external.api.util.StatsUtils;
import com.vmturbo.api.component.external.api.util.StatsUtils.PrecisionEnum;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.PortsAspectApiDTO;
import com.vmturbo.api.dto.statistic.PortChannelApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.commons.Pair;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * Map topology extension data that are related to ports.
 **/
public class PortsAspectMapper implements IAspectMapper {

    @Nullable
    @Override
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        final PortsAspectApiDTO aspect = new PortsAspectApiDTO();

        final List<StatApiDTO> nonAggregatedPorts = Lists.newArrayList();
        final List<PortChannelApiDTO> portChannels = Lists.newArrayList();
        final List<MessageOrBuilder> commodityBoughtsOrSolds = Lists.newArrayList();
        final Set<String> aggregateKeys = Sets.newHashSet();
        final Map<String, List<MessageOrBuilder>> keyToCommodities = Maps.newHashMap();

        createAggregatesRelationship(entity, commodityBoughtsOrSolds, aggregateKeys,
            keyToCommodities);
        for (final MessageOrBuilder commodityBoughtOrSold : commodityBoughtsOrSolds) {
            populatePorts(nonAggregatedPorts, portChannels, commodityBoughtOrSold,
                aggregateKeys, keyToCommodities);
        }
        aspect.setPortChannels(portChannels);
        aspect.setPorts(nonAggregatedPorts);
        return aspect;
    }

    /**
     * Create the map of commodity key to commodity bought and sold and setup aggregate keys. We
     * need to use the information above to fast get the related commodity data when creating the
     * channel or stat API DTOs
     *
     * @param entity the {@link TopologyEntityDTO} retrieved from repository.
     * @param commodityBoughtsOrSolds the port commodity bought or sold DTOs.
     * @param aggregateKeys set of commodity keys of the aggregate commodities.
     * @param keyToCommodities map of commodity key to the port commodity bought or sold DTOs.
     */
    private void createAggregatesRelationship(@Nonnull final TopologyEntityDTO entity,
        @Nonnull final List<MessageOrBuilder> commodityBoughtsOrSolds,
        @Nonnull final Set<String> aggregateKeys,
        @Nonnull final Map<String, List<MessageOrBuilder>> keyToCommodities) {
        // key to commodity bought
        for (CommodityBoughtDTO commodityBoughtDTO : entity.getCommoditiesBoughtFromProvidersList()
            .stream().map(CommoditiesBoughtFromProvider::getCommodityBoughtList)
                .flatMap(List::stream).collect(Collectors.toList())) {
            aggregateKeys.addAll(commodityBoughtDTO.getAggregatesList());
            keyToCommodities.computeIfAbsent(commodityBoughtDTO.getCommodityType().getKey(),
                k -> Lists.newArrayList()).add(commodityBoughtDTO);
            commodityBoughtsOrSolds.add(commodityBoughtDTO);
        }
        // key to commodity sold
        for (CommoditySoldDTO commoditySoldDTO : entity.getCommoditySoldListList()) {
            aggregateKeys.addAll(commoditySoldDTO.getAggregatesList());
            keyToCommodities.computeIfAbsent(commoditySoldDTO.getCommodityType().getKey(),
                k -> Lists.newArrayList()).add(commoditySoldDTO);
            commodityBoughtsOrSolds.add(commoditySoldDTO);
        }
    }

    /**
     * Update the nonAggregatedPorts and portChannels list with ports values.
     *
     * @param nonAggregatedPorts list of independent ports.
     * @param portChannels list of port channels.
     * @param commodityBoughtOrSold commodity bought to add in the list if it's related to port.
     * @param aggregateKeys set of commodity keys of the aggregate commodities.
     * @param keyToCommodities map of commodity key to the port commodity bought or sold DTOs.
     */
    private void populatePorts(@Nonnull final List<StatApiDTO> nonAggregatedPorts,
        @Nonnull final List<PortChannelApiDTO> portChannels,
        @Nonnull final MessageOrBuilder commodityBoughtOrSold,
        @Nonnull final Set<String> aggregateKeys,
        @Nonnull final Map<String, List<MessageOrBuilder>> keyToCommodities) {
        final String portUnits = CommodityTypeUnits.NET_THROUGHPUT.getUnits();
        final CommodityType commodityTypeAndKey = CommodityCommonFieldsExtractor.getCommodityType(
            commodityBoughtOrSold);
        if (commodityTypeAndKey.getType() == CommodityDTO.CommodityType.PORT_CHANEL_VALUE) {
            final PortChannelApiDTO channel = new PortChannelApiDTO();
            channel.setDisplayName(CommodityCommonFieldsExtractor
                .getDisplayName(commodityBoughtOrSold));
            channel.setValues(getStatValues(commodityBoughtOrSold));
            channel.setCapacity(getStatCapacity(commodityBoughtOrSold));
            channel.setUnits(portUnits);

            final StatFilterApiDTO relationFilter = new StatFilterApiDTO();
            relationFilter.setType(RELATION);
            relationFilter.setValue(CommodityCommonFieldsExtractor
                .isCommoditySold(commodityBoughtOrSold) ? RELATION_SOLD : RELATION_BOUGHT);

            final StatFilterApiDTO commKeyFilter = new StatFilterApiDTO();
            commKeyFilter.setType(StringConstants.KEY);
            commKeyFilter.setValue(commodityTypeAndKey.getKey());

            channel.setFilters(Lists.newArrayList(relationFilter, commKeyFilter));
            // find all of the aggregate net throughput ports and convert them to stat api
            channel.setPorts(CommodityCommonFieldsExtractor.getAggregates(commodityBoughtOrSold)
                .stream()
                .flatMap(key -> keyToCommodities.getOrDefault(key, Lists.newArrayList()).stream())
                .filter(net -> CommodityCommonFieldsExtractor.getCommodityType(net).getType() ==
                    CommodityDTO.CommodityType.NET_THROUGHPUT_VALUE)
                .map(this::mapToPort)
                .collect(Collectors.toList()));

            // convert to units if available
            final Pair<String, Integer> unitsMultiplierPair = StatsUtils.getConvertedUnits(
                commodityTypeAndKey.getType(), CommodityTypeUnits.NET_THROUGHPUT);
            int multiplier = unitsMultiplierPair.second;
            StatsUtils.convertDTOValues(channel.getValues(), multiplier);
            StatsUtils.convertDTOValues(channel.getCapacity(), multiplier);
            channel.setUnits(unitsMultiplierPair.first);

            portChannels.add(channel);
        } else if (commodityTypeAndKey.getType() ==
            CommodityDTO.CommodityType.NET_THROUGHPUT_VALUE) {
            // independent port
            if (CommodityCommonFieldsExtractor.getAggregates(commodityBoughtOrSold).isEmpty() &&
                !aggregateKeys.contains(commodityTypeAndKey.getKey())) {
                nonAggregatedPorts.add(mapToPort(commodityBoughtOrSold));
            }
        }
    }

    /**
     * Convert the port commodity to a {@link StatValueApiDTO}.
     *
     * @param commodityBoughtOrSold the port commodity.
     * @return the converted port stat DTO.
     */
    @Nonnull
    private StatApiDTO mapToPort(@Nonnull final MessageOrBuilder commodityBoughtOrSold) {
        final StatApiDTO port = new StatApiDTO();
        final String portUnits = CommodityTypeUnits.NET_THROUGHPUT.getUnits();
        final CommodityType commodityTypeAndKey = CommodityCommonFieldsExtractor.getCommodityType(
            commodityBoughtOrSold);
        port.setName(CommodityCommonFieldsExtractor.getDisplayName(commodityBoughtOrSold));
        port.setUnits(portUnits);
        port.setCapacity(getStatCapacity(commodityBoughtOrSold));
        port.setValues(getStatValues(commodityBoughtOrSold));

        final StatFilterApiDTO relationFilter = new StatFilterApiDTO();
        relationFilter.setType(RELATION);
        relationFilter.setValue(CommodityCommonFieldsExtractor
            .isCommoditySold(commodityBoughtOrSold) ? RELATION_SOLD : RELATION_BOUGHT);

        final StatFilterApiDTO commKeyFilter = new StatFilterApiDTO();
        commKeyFilter.setType(StringConstants.KEY);
        commKeyFilter.setValue(commodityTypeAndKey.getKey());

        port.setFilters(Lists.newArrayList(relationFilter, commKeyFilter));
        // convert to Kbit/sec
        final Pair<String, Integer> unitsMultiplierPair = StatsUtils.getConvertedUnits(
            commodityTypeAndKey.getType(), CommodityTypeUnits.NET_THROUGHPUT);
        int multiplier = unitsMultiplierPair.second;
        StatsUtils.convertDTOValues(port.getValues(), multiplier);
        StatsUtils.convertDTOValues(port.getCapacity(), multiplier);
        port.setUnits(unitsMultiplierPair.first);
        return port;
    }

    /**
     * Extract the used and peek values from the commodity.
     *
     * @param commodityBoughtOrSold the port commodity.
     * @return StatValueApiDTO used values of the commodity.
     */
    @Nonnull
    private StatValueApiDTO getStatValues(@Nonnull final MessageOrBuilder commodityBoughtOrSold) {
        final StatValueApiDTO dto = new StatValueApiDTO();
        float used = StatsUtils.round(CommodityCommonFieldsExtractor.getUsed(commodityBoughtOrSold),
            PrecisionEnum.STATS.getPrecision());
        float peak = StatsUtils.round(CommodityCommonFieldsExtractor.getPeak(commodityBoughtOrSold),
            PrecisionEnum.STATS.getPrecision());
        // in case there is no max used, the float is -1
        peak = used > peak ? used : peak;
        dto.setAvg(used);
        dto.setMax(peak);
        dto.setMin(used);
        dto.setTotal(used);
        return dto;
    }

    /**
     * Extract the capacity value from the commodity.
     *
     * @param commodityBoughtOrSold the port commodity.
     * @return StatValueApiDTO capacity values of the commodity.
     */
    @Nonnull
    private StatValueApiDTO getStatCapacity(@Nonnull final MessageOrBuilder commodityBoughtOrSold) {
        final StatValueApiDTO dto = new StatValueApiDTO();
        float capacity = StatsUtils.round(
            CommodityCommonFieldsExtractor.getCapacity(commodityBoughtOrSold),
                PrecisionEnum.STATS.getPrecision());
        dto.setAvg(capacity);
        dto.setMax(capacity);
        dto.setMin(capacity);
        dto.setTotal(capacity);
        return dto;
    }

    @Override
    public boolean supportsGroup() {
        return true;
    }

    @Nonnull
    @Override
    public String getAspectName() {
        return "portsAspect";
    }
}
