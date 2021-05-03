package com.vmturbo.api.component.external.api.util.businessaccount;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.BusinessAccountData.AccountType;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * {@link BusinessAccountMapper} Tests.
 */
public class BusinessAccountMapperTest {

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
}
