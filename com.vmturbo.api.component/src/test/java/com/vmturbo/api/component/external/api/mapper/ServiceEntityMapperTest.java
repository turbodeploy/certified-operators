package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.context.annotation.Bean;

import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.CostMoles;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles.PolicyServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
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

    private final static String PROVIDER_DISPLAY_NAME = "Standard_D2";
    private final static long PROVIDER_OID = 132L;

    private final PolicyDTOMoles.PolicyServiceMole policyMole = spy(new PolicyServiceMole());

    private final CostMoles.CostServiceMole costServiceMole = spy(new CostMoles.CostServiceMole());

    private final CostMoles.ReservedInstanceBoughtServiceMole reservedInstanceBoughtServiceMole =
                    spy(new CostMoles.ReservedInstanceBoughtServiceMole());
    /**
     * Rule to provide GRPC server and channels for GRPC services for test purposes.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(policyMole, costServiceMole, reservedInstanceBoughtServiceMole);
    private Cache<Long, Float> priceCache;

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
        priceCache = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build();
    }

    @Bean
    public SupplyChainServiceBlockingStub supplyChainRpcService() {
        return SupplyChainServiceGrpc.newBlockingStub(grpcServer.getChannel())
                .withInterceptors(jwtClientInterceptor());
    }

    @Bean
    public JwtClientInterceptor jwtClientInterceptor() {
        return new JwtClientInterceptor();
    }

    @Test
    public void testApiToServiceEntity() {
        final ServiceEntityMapper mapper = new ServiceEntityMapper(targetCache,
                        CostServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                        supplyChainRpcService());

        final String displayName = "entity display name";
        final long oid = 152L;
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
                .addProviders(createComputeTierProvider())
                // non-primary tiers should be ignored
                // providers should take precedence over connected entities
                .addProviders(RelatedEntity.newBuilder()
                    .setDisplayName("storage-tier")
                    .setEntityType(EntityType.STORAGE_TIER_VALUE)
                    .setOid(PROVIDER_OID + 10))
                .addConnectedTo(RelatedEntity.newBuilder()
                    .setOid(PROVIDER_OID + 20)
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
        Assert.assertEquals(PROVIDER_DISPLAY_NAME, serviceEntityApiDTO.getTemplate().getDisplayName());

        Map<String, String> target2id = serviceEntityApiDTO.getVendorIds();
        Assert.assertNotNull(target2id);
        Assert.assertEquals(localName, target2id.get(TARGET_DISPLAY_NAME));

        checkDiscoveredBy(serviceEntityApiDTO.getDiscoveredBy());
    }

    private RelatedEntity createComputeTierProvider() {
        return RelatedEntity.newBuilder()
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .setOid(PROVIDER_OID)
                .setDisplayName(PROVIDER_DISPLAY_NAME)
                .build();
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
                        supplyChainRpcService());

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
                        supplyChainRpcService());

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

}
