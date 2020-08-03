package com.vmturbo.repository.topology;

import com.vmturbo.repository.dto.CommoditiesBoughtRepoFromProviderDTO;
import org.junit.Test;

/**
 * Test for {@link CommodityMapper}
 */
public class CommodityMapperTest {

    /**
     * Verify NPE is not thrown.
     */
    @Test
    public void convertToCommoditiesBoughtFromProvider() {
        CommodityMapper.convertToCommoditiesBoughtFromProvider(new CommoditiesBoughtRepoFromProviderDTO());
    }
}