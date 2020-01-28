package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.context.annotation.Bean;

import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;
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
    private final ThinTargetCache targetCache = mock(ThinTargetCache.class);
    private final static long TARGET_ID = 10L;
    private final static String TARGET_DISPLAY_NAME = "display name";
    private final static String PROBE_TYPE = "probe type";
    private final static String PROBE_CATEGORY = "probe category";
    private final static long PROBE_ID = 123123123;

    private final PolicyDTOMoles.PolicyServiceMole policyMole = spy(new PolicyServiceMole());

    private final CostMoles.CostServiceMole costServiceMole = spy(new CostMoles.CostServiceMole());

    private final CostMoles.ReservedInstanceBoughtServiceMole reservedInstanceBoughtServiceMole =
                    spy(new CostMoles.ReservedInstanceBoughtServiceMole());

    private SupplyChainProtoMoles.SupplyChainServiceMole supplyChainMole =
            spy(new SupplyChainServiceMole());

    /**
     * Rule to provide GRPC server and channels for GRPC services for test purposes.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(policyMole, costServiceMole, reservedInstanceBoughtServiceMole, supplyChainMole);


    @Before
    public void setup() {
        final ThinTargetInfo thinTargetInfo = ImmutableThinTargetInfo.builder()
            .probeInfo(ImmutableThinProbeInfo.builder()
                .category(PROBE_CATEGORY)
                .type(PROBE_TYPE)
                .oid(PROBE_ID)
                .build())
            .displayName(TARGET_DISPLAY_NAME)
            .oid(TARGET_ID)
            .isHidden(false)
            .build();
        when(targetCache.getTargetInfo(TARGET_ID)).thenReturn(Optional.of(thinTargetInfo));
    }

    @Bean
    public JwtClientInterceptor jwtClientInterceptor() {
        return new JwtClientInterceptor();
    }

    @Test
    public void testApiToServiceEntity() {
        final ServiceEntityMapper mapper = new ServiceEntityMapper(targetCache,
                        CostServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                        SupplyChainServiceGrpc.newBlockingStub(grpcServer.getChannel())
                            .withInterceptors(jwtClientInterceptor()));

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

        final ApiPartialEntity apiEntity =
            ApiPartialEntity.newBuilder()
                .setDisplayName(displayName)
                .setOid(oid)
                .setEntityType(entityType.getNumber())
                .setEntityState(entityState)
                .setEnvironmentType(environmentType)
                .putDiscoveredTargetData(TARGET_ID,
                                         PerTargetEntityInformation
                                                          .newBuilder()
                                                          .setVendorId(localName)
                                                          .build())
                .setTags(
                    Tags.newBuilder()
                        .putTags(
                            tagKey,
                            TagValuesDTO.newBuilder().addValues(tagValue).build()))
                .addConsumers(RelatedEntity.newBuilder()
                    .setOid(consumerOid)
                    .setEntityType(EntityType.APPLICATION_VALUE)
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

        final ServiceEntityApiDTO serviceEntityApiDTO =
            mapper.toServiceEntityApiDTO(apiEntity);

        Assert.assertEquals(displayName, serviceEntityApiDTO.getDisplayName());
        Assert.assertEquals(oid, (long)Long.valueOf(serviceEntityApiDTO.getUuid()));
        Assert.assertEquals(
            entityType.getNumber(),
            UIEntityType.fromString(serviceEntityApiDTO.getClassName()).typeNumber());
        Assert.assertEquals(
            entityState,
            UIEntityState.fromString(serviceEntityApiDTO.getState()).toEntityState());
        Assert.assertEquals(
            environmentType,
            EnvironmentTypeMapper.fromApiToXL(serviceEntityApiDTO.getEnvironmentType()).get());
        Assert.assertEquals(1, serviceEntityApiDTO.getTags().size());
        Assert.assertEquals(1, serviceEntityApiDTO.getTags().get(tagKey).size());
        Assert.assertEquals(tagValue, serviceEntityApiDTO.getTags().get(tagKey).get(0));
        Assert.assertEquals(providerDisplayName1, serviceEntityApiDTO.getTemplate().getDisplayName());

        Map<String, String> target2id = serviceEntityApiDTO.getVendorIds();
        Assert.assertNotNull(target2id);
        Assert.assertEquals(localName, target2id.get(TARGET_DISPLAY_NAME));

        checkDiscoveredBy(serviceEntityApiDTO.getDiscoveredBy());

        // check consumers
        Assert.assertEquals(1, serviceEntityApiDTO.getConsumers().size());
        Assert.assertEquals(String.valueOf(consumerOid), serviceEntityApiDTO.getConsumers().get(0).getUuid());
        Assert.assertEquals(UIEntityType.APPLICATION.apiStr(), serviceEntityApiDTO.getConsumers().get(0).getClassName());
        Assert.assertEquals(consumerDisplayName, serviceEntityApiDTO.getConsumers().get(0).getDisplayName());

        // check providers
        Assert.assertEquals(2, serviceEntityApiDTO.getProviders().size());
        Map<String, BaseApiDTO> providers = serviceEntityApiDTO.getProviders().stream()
                .collect(Collectors.toMap(BaseApiDTO::getUuid, Function.identity()));
        BaseApiDTO provider1 = providers.get(String.valueOf(providerOid1));
        Assert.assertEquals(UIEntityType.COMPUTE_TIER.apiStr(), provider1.getClassName());
        Assert.assertEquals(providerDisplayName1, provider1.getDisplayName());
        BaseApiDTO provider2 = providers.get(String.valueOf(providerOid2));
        Assert.assertEquals(UIEntityType.STORAGE_TIER.apiStr(), provider2.getClassName());
        Assert.assertEquals(providerDisplayName2, provider2.getDisplayName());
    }

    private void checkDiscoveredBy(@Nonnull final TargetApiDTO targetApiDTO) {
        assertThat(targetApiDTO.getUuid(), is(Long.toString(TARGET_ID)));
        assertThat(targetApiDTO.getDisplayName(), is(TARGET_DISPLAY_NAME));
        assertThat(targetApiDTO.getCategory(), is(PROBE_CATEGORY));
        assertThat(targetApiDTO.getType(), is(PROBE_TYPE));
    }

    @Test
    public void testSetBasicMinimalFields() {
        MinimalEntity minimalEntity = MinimalEntity.newBuilder()
            .setOid(7L)
            .setDisplayName("foo")
            .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
            .build();

        ServiceEntityApiDTO dto = ServiceEntityMapper.toBasicEntity(minimalEntity);

        assertThat(dto.getUuid(), is("7"));
        assertThat(dto.getDisplayName(), is("foo"));
        assertThat(dto.getClassName(), is(UIEntityType.VIRTUAL_MACHINE.apiStr()));
    }

    /**
     * Tests the correct translation from {@link TopologyEntityDTO} to {@link ServiceEntityApiDTO}.
     */
    @Test
    public void testToServiceEntityApiDTO() throws Exception {
        final ServiceEntityMapper mapper = new ServiceEntityMapper(targetCache,
                        CostServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                        SupplyChainServiceGrpc.newBlockingStub(grpcServer.getChannel())
                            .withInterceptors(jwtClientInterceptor()));

        final String displayName = "entity display name";
        final long oid = 152L;
        final EntityType entityType = EntityType.VIRTUAL_MACHINE;
        final EntityState entityState = EntityState.POWERED_ON;
        final EnvironmentType environmentType = EnvironmentType.ON_PREM;
        final String tagKey = "tag";
        final String tagValue = "value";

        final TopologyEntityDTO topologyEntityDTO =
            TopologyEntityDTO.newBuilder()
                .setDisplayName(displayName)
                .setOid(oid)
                .setEntityType(entityType.getNumber())
                .setEntityState(entityState)
                .setEnvironmentType(environmentType)
                .setOrigin(
                    Origin.newBuilder()
                        .setDiscoveryOrigin(
                            DiscoveryOrigin.newBuilder().putDiscoveredTargetData(TARGET_ID,
                                PerTargetEntityInformation.getDefaultInstance())))
                .setTags(
                    Tags.newBuilder()
                        .putTags(
                            tagKey,
                            TagValuesDTO.newBuilder().addValues(tagValue).build()))
                .build();

        final ServiceEntityApiDTO serviceEntityApiDTO =
            mapper.toServiceEntityApiDTO(topologyEntityDTO);

        Assert.assertEquals(displayName, serviceEntityApiDTO.getDisplayName());
        Assert.assertEquals(oid, (long)Long.valueOf(serviceEntityApiDTO.getUuid()));
        Assert.assertEquals(
            entityType.getNumber(),
            UIEntityType.fromString(serviceEntityApiDTO.getClassName()).typeNumber());
        Assert.assertEquals(
            entityState,
            UIEntityState.fromString(serviceEntityApiDTO.getState()).toEntityState());
        Assert.assertEquals(
            environmentType,
            EnvironmentTypeMapper.fromApiToXL(serviceEntityApiDTO.getEnvironmentType()).get());
        Assert.assertEquals(1, serviceEntityApiDTO.getTags().size());
        Assert.assertEquals(1, serviceEntityApiDTO.getTags().get(tagKey).size());
        Assert.assertEquals(tagValue, serviceEntityApiDTO.getTags().get(tagKey).get(0));

        checkDiscoveredBy(serviceEntityApiDTO.getDiscoveredBy());
    }

    @Test
    public void testToServiceEntityApiDTOWithEmptyDisplayName() {
        final ServiceEntityMapper mapper = new ServiceEntityMapper(targetCache,
                        CostServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                        SupplyChainServiceGrpc.newBlockingStub(grpcServer.getChannel())
                            .withInterceptors(jwtClientInterceptor()));

        final String displayName = "";
        final long oid = 152L;
        final EntityType entityType = EntityType.VIRTUAL_MACHINE;

        final TopologyEntityDTO topologyEntityDTO =
            TopologyEntityDTO.newBuilder()
                .setDisplayName(displayName)
                .setOid(oid)
                .setEntityType(entityType.getNumber())
                .build();

        final ServiceEntityApiDTO serviceEntityApiDTO =
            mapper.toServiceEntityApiDTO(topologyEntityDTO);
        Assert.assertFalse(StringUtils.isEmpty(serviceEntityApiDTO.getDisplayName()));
        Assert.assertEquals(oid, (Long.parseLong(serviceEntityApiDTO.getDisplayName())));
        Assert.assertEquals(oid, (Long.parseLong(serviceEntityApiDTO.getUuid())));
        Assert.assertEquals(
            entityType.getNumber(),
            UIEntityType.fromString(serviceEntityApiDTO.getClassName()).typeNumber());
    }

    @Test
    public void testToServiceEntityApiDTOWithVMsCount() {
        final ServiceEntityMapper mapper = new ServiceEntityMapper(targetCache,
                CostServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                SupplyChainServiceGrpc.newBlockingStub(grpcServer.getChannel())
                    .withInterceptors(jwtClientInterceptor()));

        final String displayName = "Test Zone";
        final String localName = "Test Zone Local Name";
        final long zoneId = 10L;
        final long vmId = 20L;

        final ApiPartialEntity apiEntity =
            ApiPartialEntity.newBuilder()
                .setDisplayName(displayName)
                .setOid(zoneId)
                .setEntityType(EntityType.AVAILABILITY_ZONE.getNumber())
                .setEnvironmentType(EnvironmentType.CLOUD)
                .putDiscoveredTargetData(TARGET_ID,
                        PerTargetEntityInformation
                            .newBuilder()
                            .setVendorId(localName)
                            .build())
                .build();

        final List<GetMultiSupplyChainsResponse> supplyChainResponses = ImmutableList
                .of(buildMultiSupplyChainResponse(zoneId, vmId));
        when(supplyChainMole.getMultiSupplyChains(any())).thenReturn(supplyChainResponses);

        final ServiceEntityApiDTO serviceEntityApiDTO = mapper.toServiceEntityApiDTO(apiEntity);

        Assert.assertEquals(1, serviceEntityApiDTO.getNumRelatedVMs().intValue());
    }

    private GetMultiSupplyChainsResponse buildMultiSupplyChainResponse(long seedId, long nodeId) {
        return GetMultiSupplyChainsResponse.newBuilder().setSeedOid(seedId).setSupplyChain(SupplyChain
            .newBuilder().addSupplyChainNodes(makeSupplyChainNode(nodeId))).build();
    }

    private SupplyChainNode makeSupplyChainNode(long oid) {
        return SupplyChainNode.newBuilder().setEntityType(UIEntityType.VIRTUAL_MACHINE.apiStr())
            .putMembersByState(com.vmturbo.api.enums.EntityState.ACTIVE.ordinal(),
                    MemberList.newBuilder().addMemberOids(oid).build())
            .build();
    }

}
