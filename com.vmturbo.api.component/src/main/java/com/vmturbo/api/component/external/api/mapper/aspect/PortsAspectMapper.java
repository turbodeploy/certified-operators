package com.vmturbo.api.component.external.api.mapper.aspect;

import static com.vmturbo.common.protobuf.utils.StringConstants.RELATION;
import static com.vmturbo.common.protobuf.utils.StringConstants.RELATION_BOUGHT;
import static com.vmturbo.common.protobuf.utils.StringConstants.RELATION_SOLD;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.MessageOrBuilder;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.util.CommodityCommonFieldsExtractor;
import com.vmturbo.api.component.external.api.util.StatsUtils;
import com.vmturbo.api.component.external.api.util.StatsUtils.PrecisionEnum;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.PortsAspectApiDTO;
import com.vmturbo.api.dto.statistic.PortChannelApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.commons.Pair;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * Map topology extension data that are related to ports.
 **/
public class PortsAspectMapper extends AbstractAspectMapper {

    private final RepositoryApi repositoryApi;

    public PortsAspectMapper(@Nonnull final RepositoryApi repositoryApi) {
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
    }

    @Nullable
    @Override
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        final PortsAspectApiDTO aspect = new PortsAspectApiDTO();

        final List<StatApiDTO> nonAggregatedPorts = Lists.newArrayList();
        final List<PortChannelApiDTO> portChannels = Lists.newArrayList();
        final List<MessageOrBuilder> commodityBoughtsOrSolds = Lists.newArrayList();
        final Set<String> aggregateKeys = Sets.newHashSet();
        // Note: the reason we are using only key string instead of TopologyDTO.CommodityType as
        // the identifier of a commodity here because we only get the key string with no commodity
        // type from the property map in discovery response, and it is really rare to happen that
        // different kind of commodities has the same key. In this case we restrict the aggregate
        // commodities type to NET_THROUGHPUT.
        final Map<String, List<MessageOrBuilder>> keyToCommodities = Maps.newHashMap();
        final Map<CommodityType, CommoditySoldDTO> keyToProviderCommoditySold = Maps.newHashMap();

        createAggregatesRelationship(entity, commodityBoughtsOrSolds, aggregateKeys,
            keyToCommodities);
        retrieveCommoditiesBoughtCapacity(entity, keyToProviderCommoditySold);
        for (final MessageOrBuilder commodityBoughtOrSold : commodityBoughtsOrSolds) {
            populatePorts(nonAggregatedPorts, portChannels, commodityBoughtOrSold,
                aggregateKeys, keyToCommodities, keyToProviderCommoditySold);
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
     * Since the commodities bought have no capacity value, we need to retrieve the related
     * capacity data from corresponding commodities sold on other entities. E.g. we are looking at
     * a IO Module buy from a Chassis, we cannot get the capacity value for the commodities bought
     * directly, so we need to fetch the Chassis entity and iterate the commodities sold, then keep
     * the key and type to the commodity in the map. We will put the commodity sold capacity data as
     * the corresponding IO Module commodity bought capacity value to show on the UI.
     *
     * @param entity the {@link TopologyEntityDTO} retrieved from repository.
     * @param keyToProviderCommoditySold the map of commodity type and key to the commodity itself.
     */
    private void retrieveCommoditiesBoughtCapacity(@Nonnull final TopologyEntityDTO entity,
        @Nonnull final Map<CommodityType, CommoditySoldDTO> keyToProviderCommoditySold) {
        // collect all of the provider OIDs and make a batch query
        final Set<Long> providerOids = Sets.newHashSet();
        for (final CommoditiesBoughtFromProvider commoditiesBoughtFromProvider : entity
            .getCommoditiesBoughtFromProvidersList()) {
            providerOids.add(commoditiesBoughtFromProvider.getProviderId());
        }
        if (!providerOids.isEmpty()) {
            repositoryApi.entitiesRequest(providerOids)
                .getFullEntities()
                .forEach(provider -> {
                    provider.getCommoditySoldListList().forEach(commoditySoldDTO -> {
                        keyToProviderCommoditySold.putIfAbsent(commoditySoldDTO.getCommodityType(),
                            commoditySoldDTO);
                    });
                });
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
     * @param keyToProviderCommoditySold the map of commodity type and key to the commodity itself.
     */
    private void populatePorts(@Nonnull final List<StatApiDTO> nonAggregatedPorts,
            @Nonnull final List<PortChannelApiDTO> portChannels,
            @Nonnull final MessageOrBuilder commodityBoughtOrSold,
            @Nonnull final Set<String> aggregateKeys,
            @Nonnull final Map<String, List<MessageOrBuilder>> keyToCommodities,
            @Nonnull final Map<CommodityType, CommoditySoldDTO> keyToProviderCommoditySold) {
        final String portUnits = CommodityTypeUnits.NET_THROUGHPUT.getUnits();
        final CommodityType commodityTypeAndKey = CommodityCommonFieldsExtractor.getCommodityType(
            commodityBoughtOrSold);
        if (commodityTypeAndKey.getType() == CommodityDTO.CommodityType.PORT_CHANEL_VALUE) {
            final PortChannelApiDTO channel = new PortChannelApiDTO();
            channel.setDisplayName(CommodityCommonFieldsExtractor
                .getDisplayName(commodityBoughtOrSold));
            channel.setValues(getStatValues(
                CommodityCommonFieldsExtractor.getUsed(commodityBoughtOrSold),
                CommodityCommonFieldsExtractor.getPeak(commodityBoughtOrSold)));
            // if the current commodity is commodity bought, we need to retrieve the related sold
            // commodity capacity value, or set this field as 0.
            final MessageOrBuilder commodityWithCapacity =
                (commodityBoughtOrSold instanceof CommodityBoughtDTO &&
                    keyToProviderCommoditySold.containsKey(commodityTypeAndKey))
                    ? keyToProviderCommoditySold.get(commodityTypeAndKey)
                    : commodityBoughtOrSold;
            channel.setCapacity(getStatCapacity(
                CommodityCommonFieldsExtractor.getCapacity(commodityWithCapacity)));
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
                .map(commodity -> mapToPort(commodity, keyToProviderCommoditySold))
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
                nonAggregatedPorts.add(mapToPort(commodityBoughtOrSold, keyToProviderCommoditySold));
            }
        }
    }

    /**
     * Convert the port commodity to a {@link StatValueApiDTO}.
     *
     * @param commodityBoughtOrSold the port commodity.
     * @param keyToProviderCommoditySold the map of commodity type and key to the commodity itself.
     * @return the converted port stat DTO.
     */
    @Nonnull
    private StatApiDTO mapToPort(@Nonnull final MessageOrBuilder commodityBoughtOrSold,
            @Nonnull final Map<CommodityType, CommoditySoldDTO> keyToProviderCommoditySold) {
        final StatApiDTO port = new StatApiDTO();
        final String portUnits = CommodityTypeUnits.NET_THROUGHPUT.getUnits();
        final CommodityType commodityTypeAndKey = CommodityCommonFieldsExtractor.getCommodityType(
            commodityBoughtOrSold);
        port.setName(CommodityCommonFieldsExtractor.getDisplayName(commodityBoughtOrSold));
        // if the current commodity is commodity bought, we need to retrieve the related sold
        // commodity capacity value, or set this field as 0.
        final MessageOrBuilder commodityWithCapacity =
            (commodityBoughtOrSold instanceof CommodityBoughtDTO &&
                keyToProviderCommoditySold.containsKey(commodityTypeAndKey))
                ? keyToProviderCommoditySold.get(commodityTypeAndKey)
                : commodityBoughtOrSold;
        port.setCapacity(getStatCapacity(
            CommodityCommonFieldsExtractor.getCapacity(commodityWithCapacity)));
        port.setValues(getStatValues(
            CommodityCommonFieldsExtractor.getUsed(commodityBoughtOrSold),
            CommodityCommonFieldsExtractor.getPeak(commodityBoughtOrSold)));
        port.setUnits(portUnits);

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
     * Build the used and peek values for stats API from the commodity.
     *
     * @param used the port commodity used value
     * @param peak the port commodity max used value
     * @return StatValueApiDTO used values of the commodity.
     */
    @Nonnull
    private StatValueApiDTO getStatValues(final double used, final double peak) {
        final StatValueApiDTO dto = new StatValueApiDTO();
        float roundedUsed = StatsUtils.round(used, PrecisionEnum.STATS.getPrecision());
        float roundedPeak = StatsUtils.round(peak, PrecisionEnum.STATS.getPrecision());
        // in case there is no max used, the float is -1
        roundedPeak = roundedUsed > roundedPeak ? roundedUsed : roundedPeak;
        dto.setAvg(roundedUsed);
        dto.setMax(roundedPeak);
        dto.setMin(roundedUsed);
        dto.setTotal(roundedUsed);
        return dto;
    }

    /**
     * Build the capacity value for stats API from the commodity.
     *
     * @param capacity the port commodity capacity value.
     * @return StatValueApiDTO capacity values of the commodity.
     */
    @Nonnull
    private StatValueApiDTO getStatCapacity(final double capacity) {
        final StatValueApiDTO dto = new StatValueApiDTO();
        float roundedCapacity = StatsUtils.round(capacity, PrecisionEnum.STATS.getPrecision());
        dto.setAvg(roundedCapacity);
        dto.setMax(roundedCapacity);
        dto.setMin(roundedCapacity);
        dto.setTotal(roundedCapacity);
        return dto;
    }

    @Override
    public boolean supportsGroup() {
        return true;
    }

    @Nonnull
    @Override
    public AspectName getAspectName() {
        return AspectName.PORTS;
    }
}
