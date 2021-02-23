package com.vmturbo.history.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.Optional;

import org.junit.Test;

import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Test utilities for working with Commodity Names.
 */
public class HistoryStatsUtilsTest {
    @Test
    public void formatCommodityNameTest() {
        // Arrange
        int sdkType = CommodityType.CPU_ALLOCATION.getNumber();
        String mixedCaseDBType = CommodityTypeUnits.CPU_ALLOCATION.getMixedCase();

        // Act
        String formattedName = HistoryStatsUtils.formatCommodityName(sdkType);

        // Assert
        assertThat(formattedName, is(mixedCaseDBType));
    }

    @Test
    public void formatCommodityNamePrefixTest() {
        // Arrange
        int sdkType = CommodityType.CPU_ALLOCATION.getNumber();
        String mixedCaseDBType = CommodityTypeUnits.CPU_ALLOCATION.getMixedCase();
        final String prefixString = "prefix";

        // Act
        String formattedName = HistoryStatsUtils.formatCommodityName(sdkType,
                Optional.of(prefixString));

        // Assert
        assertThat(formattedName, is(prefixString + mixedCaseDBType));
    }

    @Test
    public void formatCommodityNameEmptyPrefixTest() {
        // Arrange
        int sdkType = CommodityType.CPU_ALLOCATION.getNumber();
        String mixedCaseDBType = CommodityTypeUnits.CPU_ALLOCATION.getMixedCase();

        // Act
        String formattedName = HistoryStatsUtils.formatCommodityName(sdkType);

        // Assert
        assertThat(formattedName, is(mixedCaseDBType));
    }

    @Test
    public void formatUnmappedSDKCommodity() {
        // Arrange
        int sdkType = CommodityType.SLA_COMMODITY.getNumber();
        String mixedCaseDBType = CommodityTypeUnits.SLA_COMMODITY.getMixedCase();

        // Act
        String formattedName = HistoryStatsUtils.formatCommodityName(sdkType);

        // Assert
        assertThat(formattedName, is(mixedCaseDBType));
    }

    @Test
    public void invalidCommodityTypeNumberTest() {
        // Arrange
        int sdkType = -1;

        // Act
        String formattedName = HistoryStatsUtils.formatCommodityName(sdkType);

        // Assert
        assertThat(formattedName, is(nullValue()));
    }
}
