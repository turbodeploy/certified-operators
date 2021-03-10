package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.StringUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;

import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.dto.template.TemplateApiDTO;
import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.common.api.mappers.EnvironmentTypeMapper;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord.StatValue;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.EntityFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsResponse;
import com.vmturbo.common.protobuf.cost.CostMoles;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles.PolicyServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.api.util.ImmutableThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ImmutableThinTargetInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * Tests the methods of {@link ServiceEntityMapper}.
 */
public class ServiceEntityMapperTest {
    private final ThinTargetCache targetCache = Mockito.mock(ThinTargetCache.class);
    private static final long TARGET_ID = 10L;
    private static final String TARGET_DISPLAY_NAME = "display name";
    private static final String PROBE_TYPE = "probe type";
    private static final String PROBE_CATEGORY = "probe category";
    private static final long PROBE_ID = 123123123;

    /**
     * {@link JwtClientInterceptor}.
     *
     * @return the {@link JwtClientInterceptor}
     */
    @Bean
    public JwtClientInterceptor jwtClientInterceptor() {
        return new JwtClientInterceptor();
    }

    private final PolicyDTOMoles.PolicyServiceMole policyMole =
            Mockito.spy(new PolicyServiceMole());

    private final CostMoles.CostServiceMole costServiceMole =
            Mockito.spy(new CostMoles.CostServiceMole());

    private final CostMoles.ReservedInstanceBoughtServiceMole reservedInstanceBoughtServiceMole =
            Mockito.spy(new CostMoles.ReservedInstanceBoughtServiceMole());

    private final SupplyChainProtoMoles.SupplyChainServiceMole supplyChainMole =
            Mockito.spy(new SupplyChainServiceMole());

    /**
     * Rule to provide GRPC server and channels for GRPC services for test purposes.
     */
    @Rule
    public GrpcTestServer grpcServer =
            GrpcTestServer.newServer(policyMole, costServiceMole, reservedInstanceBoughtServiceMole,
                    supplyChainMole);

    private ServiceEntityMapper mapper;

    @Before
    public void setup() {
        ConnectedEntityMapper connectedEntityMapper = Mockito.mock(ConnectedEntityMapper.class);
        mapper = new ServiceEntityMapper(targetCache,
                CostServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                SupplyChainServiceGrpc.newBlockingStub(grpcServer.getChannel())
                        .withInterceptors(jwtClientInterceptor()), connectedEntityMapper);
        final ThinTargetInfo thinTargetInfo = ImmutableThinTargetInfo.builder()
                .probeInfo(ImmutableThinProbeInfo.builder()
                        .category(PROBE_CATEGORY)
                        .uiCategory(PROBE_CATEGORY)
                        .type(PROBE_TYPE)
                        .oid(PROBE_ID)
                        .build())
                .displayName(TARGET_DISPLAY_NAME)
                .oid(TARGET_ID)
                .isHidden(false)
                .build();
        Mockito.when(targetCache.getTargetInfo(TARGET_ID)).thenReturn(Optional.of(thinTargetInfo));
    }

    @Test
    public void testApiToServiceEntity() {
        final String displayName = "entity display name";
        final String consumerDisplayName = "app1";
        final String providerDisplayName1 = "Standard_D2";
        final String providerDisplayName2 = "storage-tier";
        final long oid = 152L;
        final long consumerOid = 190L;
        final long providerOid1 = 142L;
        final long providerOid2 = 162L;

        final EntityType entityType = EntityType.VIRTUAL_MACHINE;
        final EntityState entityState = EntityState.POWERED_ON;
        final EnvironmentType environmentType = EnvironmentType.ON_PREM;
        final String tagKey = "tag";
        final String tagValue = "value";
        final String localName = "qqq";

        final ApiPartialEntity apiEntity = ApiPartialEntity.newBuilder()
                .setDisplayName(displayName)
                .setOid(oid)
                .setEntityType(entityType.getNumber())
                .setEntityState(entityState)
                .setEnvironmentType(environmentType)
                .putDiscoveredTargetData(TARGET_ID,
                        PerTargetEntityInformation.newBuilder().setVendorId(localName).build())
                .setTags(Tags.newBuilder()
                        .putTags(tagKey, TagValuesDTO.newBuilder().addValues(tagValue).build()))
                .addConsumers(RelatedEntity.newBuilder()
                        .setOid(consumerOid)
                        .setEntityType(EntityType.APPLICATION_COMPONENT_VALUE)
                        .setDisplayName(consumerDisplayName))
                .addProviders(RelatedEntity.newBuilder()
                        .setDisplayName(providerDisplayName1)
                        .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                        .setOid(providerOid1))
                // non-primary tiers should be ignored
                // providers should take precedence over connected entities
                .addProviders(RelatedEntity.newBuilder()
                        .setDisplayName(providerDisplayName2)
                        .setEntityType(EntityType.STORAGE_TIER_VALUE)
                        .setOid(providerOid2))
                .addConnectedTo(RelatedEntity.newBuilder()
                        .setOid(172)
                        .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                        .setDisplayName("connected-primary-tier"))
                .build();

        final ServiceEntityApiDTO serviceEntityApiDTO = mapper.toServiceEntityApiDTO(apiEntity);

        assertEquals(displayName, serviceEntityApiDTO.getDisplayName());
        assertEquals(oid, Long.parseLong(serviceEntityApiDTO.getUuid()));
        assertEquals(entityType.getNumber(),
                ApiEntityType.fromString(serviceEntityApiDTO.getClassName()).typeNumber());
        assertEquals(entityState,
                UIEntityState.fromString(serviceEntityApiDTO.getState()).toEntityState());
        assertEquals(environmentType,
                EnvironmentTypeMapper.fromApiToXL(serviceEntityApiDTO.getEnvironmentType()));
        assertEquals(1, serviceEntityApiDTO.getTags().size());
        assertEquals(1, serviceEntityApiDTO.getTags().get(tagKey).size());
        assertEquals(tagValue, serviceEntityApiDTO.getTags().get(tagKey).get(0));
        assertEquals(providerDisplayName1,
                serviceEntityApiDTO.getTemplate().getDisplayName());

        final Map<String, String> target2id = serviceEntityApiDTO.getVendorIds();
        Assert.assertNotNull(target2id);
        assertEquals(localName, target2id.get(TARGET_DISPLAY_NAME));

        checkDiscoveredBy(serviceEntityApiDTO.getDiscoveredBy());

        // check consumers
        assertEquals(1, serviceEntityApiDTO.getConsumers().size());
        assertEquals(String.valueOf(consumerOid),
                serviceEntityApiDTO.getConsumers().get(0).getUuid());
        assertEquals(ApiEntityType.APPLICATION_COMPONENT.apiStr(),
                serviceEntityApiDTO.getConsumers().get(0).getClassName());
        assertEquals(consumerDisplayName,
                serviceEntityApiDTO.getConsumers().get(0).getDisplayName());

        // check providers
        assertEquals(2, serviceEntityApiDTO.getProviders().size());
        final Map<String, BaseApiDTO> providers = serviceEntityApiDTO.getProviders()
                .stream()
                .collect(Collectors.toMap(BaseApiDTO::getUuid, Function.identity()));
        final BaseApiDTO provider1 = providers.get(String.valueOf(providerOid1));
        assertEquals(ApiEntityType.COMPUTE_TIER.apiStr(), provider1.getClassName());
        assertEquals(providerDisplayName1, provider1.getDisplayName());
        final BaseApiDTO provider2 = providers.get(String.valueOf(providerOid2));
        assertEquals(ApiEntityType.STORAGE_TIER.apiStr(), provider2.getClassName());
        assertEquals(providerDisplayName2, provider2.getDisplayName());
    }

    private void checkDiscoveredBy(@Nonnull final TargetApiDTO targetApiDTO) {
        MatcherAssert.assertThat(targetApiDTO.getUuid(), Matchers.is(Long.toString(TARGET_ID)));
        MatcherAssert.assertThat(targetApiDTO.getDisplayName(), Matchers.is(TARGET_DISPLAY_NAME));
        MatcherAssert.assertThat(targetApiDTO.getCategory(), Matchers.is(PROBE_CATEGORY));
        MatcherAssert.assertThat(targetApiDTO.getType(), Matchers.is(PROBE_TYPE));
    }

    @Test
    public void testSetBasicMinimalFields() {
        final MinimalEntity minimalEntity = MinimalEntity.newBuilder()
                .setOid(7L)
                .setDisplayName("foo")
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .build();

        final ServiceEntityApiDTO dto = ServiceEntityMapper.toBaseServiceEntityApiDTO(minimalEntity);

        MatcherAssert.assertThat(dto.getUuid(), Matchers.is("7"));
        MatcherAssert.assertThat(dto.getDisplayName(), Matchers.is("foo"));
        MatcherAssert.assertThat(dto.getClassName(),
                Matchers.is(ApiEntityType.VIRTUAL_MACHINE.apiStr()));
    }

    /**
     * Tests the correct translation from {@link TopologyEntityDTO} to {@link ServiceEntityApiDTO}.
     */
    @Test
    public void testToServiceEntityApiDTO() {
        final String displayName = "entity display name";
        final long oid = 152L;
        final EntityType entityType = EntityType.VIRTUAL_MACHINE;
        final EntityState entityState = EntityState.POWERED_ON;
        final EnvironmentType environmentType = EnvironmentType.ON_PREM;
        final String tagKey = "tag";
        final String tagValue = "value";

        final TopologyEntityDTO topologyEntityDTO = TopologyEntityDTO.newBuilder()
                .setDisplayName(displayName)
                .setOid(oid)
                .setEntityType(entityType.getNumber())
                .setEntityState(entityState)
                .setEnvironmentType(environmentType)
                .setOrigin(Origin.newBuilder()
                        .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                                .putDiscoveredTargetData(TARGET_ID,
                                        PerTargetEntityInformation.getDefaultInstance())))
                .setTags(Tags.newBuilder()
                        .putTags(tagKey, TagValuesDTO.newBuilder().addValues(tagValue).build()))
                .build();

        final ServiceEntityApiDTO serviceEntityApiDTO =
                mapper.toServiceEntityApiDTO(topologyEntityDTO);

        assertEquals(displayName, serviceEntityApiDTO.getDisplayName());
        assertEquals(oid, Long.parseLong(serviceEntityApiDTO.getUuid()));
        assertEquals(entityType.getNumber(),
                ApiEntityType.fromString(serviceEntityApiDTO.getClassName()).typeNumber());
        assertEquals(entityState,
                UIEntityState.fromString(serviceEntityApiDTO.getState()).toEntityState());
        assertEquals(environmentType,
                EnvironmentTypeMapper.fromApiToXL(serviceEntityApiDTO.getEnvironmentType()));
        assertEquals(1, serviceEntityApiDTO.getTags().size());
        assertEquals(1, serviceEntityApiDTO.getTags().get(tagKey).size());
        assertEquals(tagValue, serviceEntityApiDTO.getTags().get(tagKey).get(0));

        checkDiscoveredBy(serviceEntityApiDTO.getDiscoveredBy());
    }

    @Test
    public void testToServiceEntityApiDTOWithEmptyDisplayName() {
        final String displayName = "";
        final long oid = 152L;
        final EntityType entityType = EntityType.VIRTUAL_MACHINE;

        final TopologyEntityDTO topologyEntityDTO = TopologyEntityDTO.newBuilder()
                .setDisplayName(displayName)
                .setOid(oid)
                .setEntityType(entityType.getNumber())
                .build();

        final ServiceEntityApiDTO serviceEntityApiDTO =
                mapper.toServiceEntityApiDTO(topologyEntityDTO);
        Assert.assertFalse(StringUtils.isEmpty(serviceEntityApiDTO.getDisplayName()));
        assertEquals(oid, (Long.parseLong(serviceEntityApiDTO.getDisplayName())));
        assertEquals(oid, (Long.parseLong(serviceEntityApiDTO.getUuid())));
        assertEquals(entityType.getNumber(),
                ApiEntityType.fromString(serviceEntityApiDTO.getClassName()).typeNumber());
    }

    @Test
    public void testToServiceEntityApiDTOWithVMsCount() {
        final String displayName = "Test Zone";
        final String localName = "Test Zone Local Name";
        final long zoneId = 10L;
        final long vmId = 20L;

        final ApiPartialEntity apiEntity = createPartialEntity(displayName, zoneId, localName).build();

        final List<GetMultiSupplyChainsResponse> supplyChainResponses =
                ImmutableList.of(buildMultiSupplyChainResponse(zoneId, vmId));
        Mockito.when(supplyChainMole.getMultiSupplyChains(org.mockito.Matchers.any()))
                .thenReturn(supplyChainResponses);

        final ServiceEntityApiDTO serviceEntityApiDTO = mapper.toServiceEntityApiDTO(apiEntity);

        assertEquals(1, serviceEntityApiDTO.getNumRelatedVMs().intValue());
    }

    /**
     * Configure {@link ApiPartialEntity}
     * @param name name of entity
     * @param id of entity
     * @param vendorLocalName vendorId for discovered target data
     * @return Builder of {@link ApiPartialEntity}
     */
    private ApiPartialEntity.Builder createPartialEntity(final String name, long id, String vendorLocalName) {
        return ApiPartialEntity.newBuilder()
                .setDisplayName(name)
                .setOid(id)
                .setEntityType(EntityType.AVAILABILITY_ZONE.getNumber())
                .setEnvironmentType(EnvironmentType.CLOUD)
                .putDiscoveredTargetData(TARGET_ID,
                        PerTargetEntityInformation.newBuilder().setVendorId(vendorLocalName).build());

    }

    /**
     * Test for {@link ServiceEntityMapper#setPriceValuesForEntityComponents(Map)}.
     */
    @Test
    public void testSetPriceValuesForEntityComponents() {
        final HashMap<Long, ServiceEntityApiDTO> entities = new HashMap<>();
        final ServiceEntityApiDTO entityApiDTO1 = new ServiceEntityApiDTO();
        entities.put(1L, entityApiDTO1);
        final ServiceEntityApiDTO entityApiDTO2 = new ServiceEntityApiDTO();
        entityApiDTO2.setTemplate(new TemplateApiDTO());
        entities.put(2L, entityApiDTO2);

        Mockito.when(costServiceMole.getCloudCostStats(GetCloudCostStatsRequest.newBuilder()
                .addCloudCostStatsQuery(CloudCostStatsQuery.newBuilder()
                        .setEntityFilter(
                                EntityFilter.newBuilder().addAllEntityId(entities.keySet())))
                .build()))
                .thenReturn(Collections.singletonList(GetCloudCostStatsResponse.newBuilder()
                        .addCloudStatRecord(CloudCostStatRecord.newBuilder()
                                .addStatRecords(StatRecord.newBuilder()
                                        .setAssociatedEntityId(1)
                                        .setCategory(CostCategory.ON_DEMAND_COMPUTE)
                                        .setCostSource(CostSource.ON_DEMAND_RATE)
                                        .setValues(StatValue.newBuilder().setAvg(5F)))
                                .addStatRecords(StatRecord.newBuilder()
                                        .setAssociatedEntityId(1)
                                        .setCategory(CostCategory.STORAGE)
                                        .setCostSource(CostSource.ON_DEMAND_RATE)
                                        .setValues(StatValue.newBuilder().setAvg(5F)))
                                .addStatRecords(StatRecord.newBuilder()
                                        .setAssociatedEntityId(2)
                                        .setCategory(CostCategory.STORAGE)
                                        .setCostSource(CostSource.ON_DEMAND_RATE)
                                        .setValues(StatValue.newBuilder().setAvg(10F)))
                                .addStatRecords(StatRecord.newBuilder()
                                        .setAssociatedEntityId(2)
                                        .setCategory(CostCategory.STORAGE)
                                        .setCostSource(CostSource.ON_DEMAND_RATE)
                                        .setValues(StatValue.newBuilder().setAvg(5F))))
                        .build()));

        mapper.setPriceValuesForEntityComponents(entities);
        assertEquals(10D, entityApiDTO1.getCostPrice(), 0.001);
        assertEquals(5D, entityApiDTO1.getTemplate().getPrice(), 0.001);
        assertEquals(15D, entityApiDTO2.getCostPrice(), 0.001);
        Assert.assertNull(entityApiDTO2.getTemplate().getPrice());
    }

    private GetMultiSupplyChainsResponse buildMultiSupplyChainResponse(final long seedId,
            final long nodeId) {
        return GetMultiSupplyChainsResponse.newBuilder()
                .setSeedOid(seedId)
                .setSupplyChain(
                        SupplyChain.newBuilder().addSupplyChainNodes(makeSupplyChainNode(nodeId)))
                .build();
    }

    private SupplyChainNode makeSupplyChainNode(final long oid) {
        return SupplyChainNode.newBuilder()
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .putMembersByState(com.vmturbo.api.enums.EntityState.ACTIVE.ordinal(),
                        MemberList.newBuilder().addMemberOids(oid).build())
                .build();
    }

    /**
     * Check whether region filter correctly returns the count of VMs.
     */
    @Test
    public void toServiceEntityApiDTORegion() {
        final String regionName = "Azure East US 2";
        final String vendorId = "vendorId-1";
        final long regionId = 101;
        final long vmId1 = 201;
        final long vmId2 = 202;

        final ApiPartialEntity apiEntity = ApiPartialEntity.newBuilder()
                .setDisplayName(regionName)
                .setOid(regionId)
                .setEntityType(EntityType.REGION.getNumber())
                .setEnvironmentType(EnvironmentType.CLOUD)
                .putDiscoveredTargetData(TARGET_ID,
                        PerTargetEntityInformation.newBuilder().setVendorId(vendorId).build())
                .build();

        // Add both VMs to the mock region response.
        final List<GetMultiSupplyChainsResponse> supplyChainResponses =
                ImmutableList.of(GetMultiSupplyChainsResponse.newBuilder()
                        .setSeedOid(regionId)
                        .setSupplyChain(SupplyChain.newBuilder()
                                .addSupplyChainNodes(SupplyChainNode.newBuilder()
                                        .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                                        .putMembersByState(0, MemberList.newBuilder()
                                                .addAllMemberOids(ImmutableSet.of(vmId1, vmId2))
                                                .build())
                                        .build())
                                .build())
                        .build());
        Mockito.when(supplyChainMole.getMultiSupplyChains(org.mockito.Matchers.any()))
                .thenReturn(supplyChainResponses);

        final ServiceEntityApiDTO serviceEntityApiDTO = mapper.toServiceEntityApiDTO(apiEntity);

        assertEquals(String.valueOf(regionId), serviceEntityApiDTO.getUuid());
        assertEquals(2, serviceEntityApiDTO.getNumRelatedVMs().intValue());
    }

    /**
     * Tests order of collection is being preserved when new collection returned.
     */
    @Test
    public void toServiceEntityApiDTOMaintainsOrderOfCollection() {
        //GIVEN
        List<ApiPartialEntity> partialEntities = new LinkedList<>();

        partialEntities.add(createPartialEntity("name", 0L, "").build());
        partialEntities.add(createPartialEntity("name", 20L, "").build());
        partialEntities.add(createPartialEntity("name", 10L, "").build());
        partialEntities.add(createPartialEntity("name", 4L, "").build());

        //WHEN
        List<ServiceEntityApiDTO> results = mapper.toServiceEntityApiDTO(partialEntities);

        //THEN
        Assert.assertEquals(String.valueOf(0L), results.get((int)0L).getUuid());
        Assert.assertEquals(String.valueOf(20L), results.get((int)1L).getUuid());
        Assert.assertEquals(String.valueOf(10L), results.get((int)2L).getUuid());
        Assert.assertEquals(String.valueOf(4L), results.get((int)3L).getUuid());

    }


}
