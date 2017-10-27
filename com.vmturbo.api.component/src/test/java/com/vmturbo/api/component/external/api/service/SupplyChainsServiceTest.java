package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.assertj.core.util.Sets;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.api.component.external.api.service.SupplyChainsService.FilterSet;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcher;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainEntryDTO;
import com.vmturbo.api.dto.supplychain.SupplyChainStatsApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.EntitiesCountCriteria;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.SupplyChainDetailType;

@RunWith(MockitoJUnitRunner.class)
public class SupplyChainsServiceTest {
    private static final long LIVE_TOPOLOGY_CONTEXT_ID = 7777777L;

    @Mock
    private SupplyChainFetcher supplyChainsFetcherMock;

    @Mock
    private GroupExpander groupExpanderMock;

    @Mock
    private SupplyChainFetcher.OperationBuilder supplyChainFetcherOperationBuilderMock;

    private final SupplyChainTestUtils supplyChainTestUtils = new SupplyChainTestUtils();

    private SupplyChainsService service;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(supplyChainsFetcherMock.newOperation())
                .thenReturn(supplyChainFetcherOperationBuilderMock);
        when(supplyChainFetcherOperationBuilderMock.entityTypes(anyListOf(String.class)))
                .thenReturn(supplyChainFetcherOperationBuilderMock);
        when(supplyChainFetcherOperationBuilderMock.topologyContextId(anyLong()))
                .thenReturn(supplyChainFetcherOperationBuilderMock);
        when(supplyChainFetcherOperationBuilderMock.addSeedUuids(anySetOf(String.class)))
                .thenReturn(supplyChainFetcherOperationBuilderMock);
        when(supplyChainFetcherOperationBuilderMock.environmentType(any(EnvironmentType.class)))
                .thenReturn(supplyChainFetcherOperationBuilderMock);
        when(supplyChainFetcherOperationBuilderMock.supplyChainDetailType(any(SupplyChainDetailType.class)))
                .thenReturn(supplyChainFetcherOperationBuilderMock);
        when(supplyChainFetcherOperationBuilderMock.includeHealthSummary(anyBoolean()))
                .thenReturn(supplyChainFetcherOperationBuilderMock);

        service = new SupplyChainsService(supplyChainsFetcherMock,
                LIVE_TOPOLOGY_CONTEXT_ID, groupExpanderMock);


    }


    @Test
    public void testGetSupplyChainStatsGroupByEntityType() throws Exception {
        // arrange
        final ImmutableList<String> searchUuids = ImmutableList.of("1");
        final Set<String> searchUuidSet = Sets.newHashSet(searchUuids);
        when(groupExpanderMock.expandUuids(eq(searchUuidSet))).thenReturn(
                ImmutableSet.of(1L));
        // set up to return three entity types
        SupplychainApiDTO answer = new SupplychainApiDTO();
        answer.setSeMap(ImmutableMap.of(
                "PhysicalMachine", supplyChainTestUtils
                        .createSupplyChainEntryDTO("PhysicalMachine", 2L, 3L, 4L),
                "VirtualMachine", supplyChainTestUtils
                        .createSupplyChainEntryDTO("VirtualMachine", 5L, 6L),
                "Application", supplyChainTestUtils
                        .createSupplyChainEntryDTO("Application", 7L)
        ));
        when(supplyChainFetcherOperationBuilderMock.fetch()).thenReturn(answer);


        // act
        SupplyChainStatsApiInputDTO statsRequest = new SupplyChainStatsApiInputDTO();
        List<EntitiesCountCriteria> groupByCriteria = ImmutableList.of(
                EntitiesCountCriteria.entityType
        );
        statsRequest.setGroupBy(groupByCriteria);
        statsRequest.setUuids(searchUuids);
        statsRequest.setTypes(ImmutableList.of("PhysicalMachine"));

        List<StatSnapshotApiDTO> result = service.getSupplyChainStats(statsRequest);

        // assert
        assertThat(result.size(), equalTo(1));
        StatSnapshotApiDTO snapshotApiDTO = result.get(0);
        // PM, VM, App
        assertThat(snapshotApiDTO.getStatistics().size(), equalTo(3));
        // 3 PMs, 2 VMs, 1 App
        snapshotApiDTO.getStatistics().forEach(statApiDTO -> {
            // just one "groupBy" filter in this test
            assertThat(statApiDTO.getFilters().size(), equalTo(1));
            final StatFilterApiDTO statFilterApiDTO = statApiDTO.getFilters().get(0);
            switch(statFilterApiDTO.getValue()) {
                case "PhysicalMachine":
                    assertThat(statApiDTO.getValue(), equalTo(3.0f));
                    break;
                case "VirtualMachine":
                    assertThat(statApiDTO.getValue(), equalTo(2.0f));
                    break;
                case "Application":
                    assertThat(statApiDTO.getValue(), equalTo(1.0f));
                    break;
                default:
                    fail("Unexpected value: " + statFilterApiDTO.getValue());
            }
        });
    }


    @Test
    public void testGetSupplyChainStatsGroupBySeverity() throws Exception {
        // arrange
        final ImmutableList<String> searchUuids = ImmutableList.of("1");
        final Set<String> searchUuidSet = Sets.newHashSet(searchUuids);
        when(groupExpanderMock.expandUuids(eq(searchUuidSet))).thenReturn(
                ImmutableSet.of(1L));
        // set up to return three entity types
        SupplychainApiDTO answer = new SupplychainApiDTO();
        final SupplychainEntryDTO pmSupplyChainEntryDTO = supplyChainTestUtils
                .createSupplyChainEntryDTO("PhysicalMachine", 2L, 3L, 4L);
        supplyChainTestUtils.addHealthSummary(pmSupplyChainEntryDTO, ImmutableMap.of(
                2L, "normal", 3L, "normal", 4L, "critical"));

        final SupplychainEntryDTO vmSupplyChainEntryDTO = supplyChainTestUtils
                .createSupplyChainEntryDTO("PhysicalMachine", 5L, 6L);
        // sum health summary counts should equal entity count
        supplyChainTestUtils.addHealthSummary(vmSupplyChainEntryDTO, ImmutableMap.of(
                5L, "critical",
                6L, "minor"
        ));
        final SupplychainEntryDTO appSupplyChainEntryDTO = supplyChainTestUtils
                .createSupplyChainEntryDTO("PhysicalMachine", 7L);

        supplyChainTestUtils.addHealthSummary(appSupplyChainEntryDTO, ImmutableMap.of(
                7L, "major"
        ));
        answer.setSeMap(ImmutableMap.of(
                "PhysicalMachine", pmSupplyChainEntryDTO,
                "VirtualMachine", vmSupplyChainEntryDTO,
                "Application", appSupplyChainEntryDTO
        ));
        when(supplyChainFetcherOperationBuilderMock.fetch()).thenReturn(answer);


        // act
        SupplyChainStatsApiInputDTO statsRequest = new SupplyChainStatsApiInputDTO();
        List<EntitiesCountCriteria> groupByCriteria = ImmutableList.of(
                EntitiesCountCriteria.severity
        );
        statsRequest.setGroupBy(groupByCriteria);
        statsRequest.setUuids(searchUuids);
        statsRequest.setTypes(ImmutableList.of("PhysicalMachine"));

        List<StatSnapshotApiDTO> result = service.getSupplyChainStats(statsRequest);

        // assert
        assertThat(result.size(), equalTo(1));
        StatSnapshotApiDTO snapshotApiDTO = result.get(0);
        // normal, minor, major, critical
        assertThat(snapshotApiDTO.getStatistics().size(), equalTo(4));
        // 3 PMs, 2 VMs, 1 App
        snapshotApiDTO.getStatistics().forEach(statApiDTO -> {
            // just one "groupBy" filter in this test
            assertThat(statApiDTO.getFilters().size(), equalTo(1));
            final StatFilterApiDTO statFilterApiDTO = statApiDTO.getFilters().get(0);
            switch(statFilterApiDTO.getValue()) {
                case "critical":
                    assertThat(statApiDTO.getValue(), equalTo(2.0f));
                    break;
                case "major":
                    assertThat(statApiDTO.getValue(), equalTo(1.0f));
                    break;
                case "minor":
                    assertThat(statApiDTO.getValue(), equalTo(1.0f));
                    break;
                case "normal":
                    assertThat(statApiDTO.getValue(), equalTo(2.0f));
                    break;
                default:
                    fail("Unexpected value: " + statFilterApiDTO.getValue());
            }
        });
    }

    /**
     * Group by both 'entityType' and 'severity', resulting in counts for the cross-product
     * of distinct values for each.
     * <ul>
     *     <li>PM - Normal: 2; Critical: 1
     *     <li>VM - Critical: 1; Minor: 1;
     *     <li>App =  Major: 1
     * </ul>
     * Should result in 5 different stats values with counts as above.
     * @throws Exception if the supplychain fetch operation fails
     */
    @Test
    public void testGetSupplyChainStatsGroupByEntityTypeAndSeverity() throws Exception {
        // arrange
        final ImmutableList<String> searchUuids = ImmutableList.of("1");
        final Set<String> searchUuidSet = Sets.newHashSet(searchUuids);
        when(groupExpanderMock.expandUuids(eq(searchUuidSet))).thenReturn(
                ImmutableSet.of(1L));
        // set up to return three entity types
        SupplychainApiDTO answer = new SupplychainApiDTO();
        final SupplychainEntryDTO pmSupplyChainEntryDTO = supplyChainTestUtils
                .createSupplyChainEntryDTO("PhysicalMachine", 2L, 3L, 4L);
        supplyChainTestUtils.addHealthSummary(pmSupplyChainEntryDTO, ImmutableMap.of(
                2L, "normal", 3L, "normal", 4L, "critical"));

        final SupplychainEntryDTO vmSupplyChainEntryDTO = supplyChainTestUtils
                .createSupplyChainEntryDTO("VirtualMachine", 5L, 6L);
        // sum health summary counts should equal entity count
        supplyChainTestUtils.addHealthSummary(vmSupplyChainEntryDTO, ImmutableMap.of(
                5L, "critical",
                6L, "minor"
        ));
        final SupplychainEntryDTO appSupplyChainEntryDTO = supplyChainTestUtils
                .createSupplyChainEntryDTO("Application", 7L);

        supplyChainTestUtils.addHealthSummary(appSupplyChainEntryDTO, ImmutableMap.of(
                7L, "major"
        ));
        answer.setSeMap(ImmutableMap.of(
                "PhysicalMachine", pmSupplyChainEntryDTO,
                "VirtualMachine", vmSupplyChainEntryDTO,
                "Application", appSupplyChainEntryDTO
        ));
        when(supplyChainFetcherOperationBuilderMock.fetch()).thenReturn(answer);


        // act
        SupplyChainStatsApiInputDTO statsRequest = new SupplyChainStatsApiInputDTO();
        List<EntitiesCountCriteria> groupByCriteria = ImmutableList.of(
                EntitiesCountCriteria.entityType,
                EntitiesCountCriteria.severity
        );
        statsRequest.setGroupBy(groupByCriteria);
        statsRequest.setUuids(searchUuids);

        List<StatSnapshotApiDTO> result = service.getSupplyChainStats(statsRequest);

        // assert
        assertThat(result.size(), equalTo(1));
        StatSnapshotApiDTO snapshotApiDTO = result.get(0);
        // (PM, Normal): 2; (PM, Critical): 1; (VM, Critical): 1; (VM, Minor): 1; (App, Major): 1
        assertThat(snapshotApiDTO.getStatistics().size(), equalTo(5));
        snapshotApiDTO.getStatistics().forEach(statApiDTO -> {
            // two "groupBy" filters in this test, entityType and severity
            assertThat(statApiDTO.getFilters().size(), equalTo(2));
            // if entityType first
            if (statApiDTO.getFilters().get(0).getType().equals("entityType")) {
                // switch on entityType
                switch(statApiDTO.getFilters().get(0).getValue()) {
                    case "PhysicalMachine":
                        // switch on the second filter - severity
                        if (statApiDTO.getFilters().get(1).getValue().equals("normal")) {
                            assertThat(statApiDTO.getValue(), equalTo(2.0F));
                        } else if (statApiDTO.getFilters().get(1).getValue().equals("critical")) {
                            assertThat(statApiDTO.getValue(), equalTo(1.0F));
                        } else {
                            fail("Unexpected severity: " + statApiDTO.getFilters().get(1).getValue());
                        }
                        break;
                    case "VirtualMachine":
                        // switch on the second filter - severity
                        if (statApiDTO.getFilters().get(1).getValue().equals("minor")) {
                            assertThat(statApiDTO.getValue(), equalTo(1.0F));
                        } else if (statApiDTO.getFilters().get(1).getValue().equals("critical")) {
                            assertThat(statApiDTO.getValue(), equalTo(1.0F));
                        } else {
                            fail("Unexpected severity: " + statApiDTO.getFilters().get(1).getValue());
                        }
                        break;
                    case "Application":
                        // switch on the second filter - severity
                        if (statApiDTO.getFilters().get(1).getValue().equals("major")) {
                            assertThat(statApiDTO.getValue(), equalTo(1.0F));
                        } else {
                            fail("Unexpected severity: " + statApiDTO.getFilters().get(1).getValue());
                        }
                        break;
                    default:
                        fail("Unexpected entityType value: " + statApiDTO.getFilters().get(1).getValue());
                        break;
                }
            } else if (statApiDTO.getFilters().get(0).getType().equals("severity")) {
                // first filter is severity
                switch(statApiDTO.getFilters().get(0).getValue()) {
                    case "critical":
                        // switch on the second filter - entityType
                        if (statApiDTO.getFilters().get(1).getValue().equals("PhysicalMachine")) {
                            assertThat(statApiDTO.getValue(), equalTo(1.0F));
                        } else if (statApiDTO.getFilters().get(1).getValue().equals("VirtualMachine")) {
                            assertThat(statApiDTO.getValue(), equalTo(1.0F));
                        } else {
                        fail("Unexpected entityType: " + statApiDTO.getFilters().get(1).getValue());
                    }
                        break;
                    case "major":
                        // switch on the second filter - entityType
                        if (statApiDTO.getFilters().get(1).getValue().equals("Application")) {
                            assertThat(statApiDTO.getValue(), equalTo(1.0F));
                        } else {
                            fail("Unexpected entityType: " + statApiDTO.getFilters().get(1).getValue());
                        }
                        break;
                    case "minor":
                        // switch on the second filter - entityType
                        if (statApiDTO.getFilters().get(1).getValue().equals("VirtualMachine")) {
                            assertThat(statApiDTO.getValue(), equalTo(1.0F));
                        } else {
                            fail("Unexpected entityType: " + statApiDTO.getFilters().get(1).getValue());
                        }
                        break;
                    case "normal":
                        // switch on the second filter - entityType
                        if (statApiDTO.getFilters().get(1).getValue().equals("PhysicalMachine")) {
                            assertThat(statApiDTO.getValue(), equalTo(2.0F));
                        } else {
                            fail("Unexpected entityType: " + statApiDTO.getFilters().get(1).getValue());
                        }
                        break;
                    default:
                        fail("Unexpected severity value: " + statApiDTO.getFilters().get(1).getValue());
                        break;
                }
            } else {
                fail("Unexpected filter: " + statApiDTO.getFilters().get(0).getType());
            }
        });
    }

    /**
     * Test the 'equals()' method of FilterSet to be used as the key in tabulating
     * FilterSet values.
     */
    @Test
    public void testFeatureSetEquals() {
        SupplyChainsService.FilterSet fs1 = new SupplyChainsService.FilterSet();
        final String featureType = "type";
        final String featureValue = "value";
        final String featureType2 = "type2";
        final String featureValue2 = "value2";
        addFeature(fs1, featureType, featureValue);
        addFeature(fs1, featureType, featureValue2);
        addFeature(fs1, featureType2, featureValue2);

        FilterSet fs2 = new FilterSet();
        addFeature(fs2, featureType, featureValue2);
        addFeature(fs2, featureType2, featureValue2);
        addFeature(fs2, featureType, featureValue);

        assertTrue(fs1.equals(fs2));
        assertTrue(fs2.equals(fs1));
    }

    /**
     * Test the 'equals()' method of FilterSet to be used as the key in tabulating
     * FilterSet values.
     */
    @Test
    public void testFeatureSetNotEqualsSize() {
        SupplyChainsService.FilterSet fs1 = new SupplyChainsService.FilterSet();
        final String featureType = "type";
        final String featureValue = "value";
        final String featureType2 = "type2";
        final String featureValue2 = "value2";
        addFeature(fs1, featureType, featureValue);
        addFeature(fs1, featureType2, featureValue2);

        SupplyChainsService.FilterSet fs2 = new SupplyChainsService.FilterSet();
        addFeature(fs2, featureType, featureValue);

        assertFalse(fs1.equals(fs2));
        assertFalse(fs2.equals(fs1));
    }

    /**
     * Test the 'equals()' method of FilterSet to be used as the key in tabulating
     * FilterSet values.
     */
    @Test
    public void testFeatureSetNotEqualsValues() {
        FilterSet fs1 = new SupplyChainsService.FilterSet();
        final String featureType = "type";
        final String featureValue = "value";
        final String featureType2 = "type2";
        final String featureValue2 = "value2";
        final String featureValue3 = "value3";

        addFeature(fs1, featureType, featureValue);
        addFeature(fs1, featureType2, featureValue2);

        FilterSet fs2 = new SupplyChainsService.FilterSet();
        addFeature(fs2, featureType2, featureValue);
        addFeature(fs2, featureType, featureValue3);

        assertFalse(fs1.equals(fs2));
        assertFalse(fs2.equals(fs1));
    }

    private void addFeature(@Nonnull SupplyChainsService.FilterSet filterSet,
                            @Nonnull String featureType,
                            @Nonnull String featureValue) {
        StatFilterApiDTO f1 = new StatFilterApiDTO();
        f1.setType(featureType);
        f1.setValue(featureValue);
        filterSet.addFilter(f1);
    }

}
