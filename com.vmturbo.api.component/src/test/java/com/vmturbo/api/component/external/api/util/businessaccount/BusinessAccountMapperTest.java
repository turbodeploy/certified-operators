package com.vmturbo.api.component.external.api.util.businessaccount;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.BusinessAccountData.AccountType;
import com.vmturbo.topology.processor.api.util.ImmutableThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ImmutableThinTargetInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * {@link BusinessAccountMapper} Tests.
 */
public class BusinessAccountMapperTest {

    private static final long TARGET_ID = 10L;
    private static final String TARGET_DISPLAY_NAME = "target name";
    private static final String PROBE_TYPE = "probe type";
    private static final String PROBE_CATEGORY = "probe category";
    private static final long PROBE_ID = 123123123;
    private static final ThinTargetInfo THIN_TARGET_INFO = ImmutableThinTargetInfo.builder()
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

    private static final long AZURE_TARGET_ID = 11L;
    private static final String AZURE_TARGET_DISPLAY_NAME = "azure target name";
    private static final String AZURE_PROBE_TYPE = "azure subscription";
    private static final String AZURE_PROBE_CATEGORY = "azure category";
    private static final long AZURE_PROBE_ID = 124124124;
    private static final ThinTargetInfo AZURE_THIN_TARGET_INFO = ImmutableThinTargetInfo.builder()
            .probeInfo(ImmutableThinProbeInfo.builder()
                    .category(AZURE_PROBE_CATEGORY)
                    .uiCategory(AZURE_PROBE_CATEGORY)
                    .type(AZURE_PROBE_TYPE)
                    .oid(AZURE_PROBE_ID)
                    .build())
            .displayName(AZURE_TARGET_DISPLAY_NAME)
            .oid(AZURE_TARGET_ID)
            .isHidden(false)
            .build();

    @Mock
    private ThinTargetCache thinTargetCache;

    @Mock
    SupplementaryDataFactory supplementaryDataFactory;

    @Mock
    UserSessionContext userSessionContext;

    private BusinessAccountMapper businessAccountMapper;


    /**
     * Set up for tests.
     */
    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);

        businessAccountMapper = new BusinessAccountMapper(
                        thinTargetCache, supplementaryDataFactory, userSessionContext);
    }

    /**
     * Tests mapping accountType from {@link BusinessAccountInfo} to {@link BusinessUnitApiDTO}.
     */
    @Test
    public void testbuildDiscoveredBusinessUnitApiDTOAccountType() {
        //GIVEN
        BusinessAccountInfo bizInfo = BusinessAccountInfo.newBuilder().setAccountType(AccountType.Government_US).build();
        TopologyEntityDTO topologyEntityDTO = TopologyEntityDTO.newBuilder().setOid(1111L).setEntityType(1)
                        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setBusinessAccount(bizInfo))
                        .build();

        SupplementaryData supplementaryData = mock(SupplementaryData.class);
        doReturn(Optional.empty()).when(supplementaryData).getCostPrice(Mockito.any());

        //WHEN
        BusinessUnitApiDTO businessUnitApiDTO = businessAccountMapper.buildDiscoveredBusinessUnitApiDTO(topologyEntityDTO, supplementaryData);

        //THEN
        assertNotNull(businessUnitApiDTO);
        assertEquals(businessUnitApiDTO.getAccountType(), com.vmturbo.api.enums.AccountType.GOVERNMENT_US);
    }

    /**
     * Tests mapping empty accountType from {@link BusinessAccountInfo}.
     */
    @Test
    public void testbuildDiscoveredBusinessUnitApiDTOEmptyAccountType() {
        //GIVEN
        TopologyEntityDTO topologyEntityDTO = TopologyEntityDTO.newBuilder().setOid(1111L).setEntityType(1)
                        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setBusinessAccount(BusinessAccountInfo.getDefaultInstance()))
                        .build();

        SupplementaryData supplementaryData = mock(SupplementaryData.class);
        doReturn(Optional.empty()).when(supplementaryData).getCostPrice(Mockito.any());

        //WHEN
        BusinessUnitApiDTO businessUnitApiDTO = businessAccountMapper.buildDiscoveredBusinessUnitApiDTO(topologyEntityDTO, supplementaryData);

        //THEN
        assertNotNull(businessUnitApiDTO);
        assertNull(businessUnitApiDTO.getAccountType());
    }

    /**
     * Tests mapping cloudType from {@link BusinessAccountInfo} to {@link BusinessUnitApiDTO}.
     */
    @Test
    public void testbuildDiscoveredBusinessUnitApiDTOCloudType() {
        //GIVEN
        DiscoveryOrigin.Builder discoveryOrigin = DiscoveryOrigin.newBuilder();
        discoveryOrigin.putDiscoveredTargetData(TARGET_ID, PerTargetEntityInformation.newBuilder().build());
        discoveryOrigin.putDiscoveredTargetData(AZURE_TARGET_ID, PerTargetEntityInformation.newBuilder().build());
        TopologyEntityDTO topologyEntityDTO = TopologyEntityDTO.newBuilder().setOid(1111L).setEntityType(1)
                .setOrigin(Origin.newBuilder()
                        .setDiscoveryOrigin(discoveryOrigin)
                        .build())
                .build();

        when(thinTargetCache.getTargetInfo(TARGET_ID)).thenReturn(Optional.of(THIN_TARGET_INFO));
        when(thinTargetCache.getTargetInfo(AZURE_TARGET_ID)).thenReturn(Optional.of(AZURE_THIN_TARGET_INFO));

        SupplementaryData supplementaryData = mock(SupplementaryData.class);
        doReturn(Optional.empty()).when(supplementaryData).getCostPrice(Mockito.any());

        //WHEN
        BusinessUnitApiDTO businessUnitApiDTO = businessAccountMapper
                .buildDiscoveredBusinessUnitApiDTO(topologyEntityDTO, supplementaryData);

        //THEN
        assertNotNull(businessUnitApiDTO);
        assertEquals(CloudType.AZURE, businessUnitApiDTO.getCloudType());
    }
}
