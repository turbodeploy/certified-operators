package com.vmturbo.components.common.utils;

import static com.vmturbo.components.common.utils.DataPacks.IDataPack.MISSING;
import static com.vmturbo.components.common.utils.DataPacks.packInts;
import static com.vmturbo.components.common.utils.DataPacks.unpackInts;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.components.common.utils.DataPacks.CommodityTypeDataPack;
import com.vmturbo.components.common.utils.DataPacks.DataPack;
import com.vmturbo.components.common.utils.DataPacks.IDataPack;
import com.vmturbo.components.common.utils.DataPacks.LongDataPack;
import com.vmturbo.platform.common.dto.CommonDTOREST.CommodityDTO;

/**
 * Tests of the {@link DataPacks} class and its nested classes.
 */
public class DataPacksTest {

    private static final CommodityType CPU_NO_KEY = createCommodityType(CommodityDTO.CommodityType.CPU, null);
    private static final CommodityType CPU_KEY1 = createCommodityType(CommodityDTO.CommodityType.CPU, "key1");
    private static final CommodityType CPU_KEY2 = createCommodityType(CommodityDTO.CommodityType.CPU, "key2");
    private static final CommodityType CPU_XXX = createCommodityType(CommodityDTO.CommodityType.CPU, "xxx");
    private static final CommodityType MEM_NO_KEY = createCommodityType(CommodityDTO.CommodityType.MEM, "");

    /**
     * Test that the int-packer works.
     */
    @Test
    public void testPackInts() {
        assertThat(packInts(0, 0), is(0L));
        assertThat(packInts(0, 1), is(1L));
        assertThat(packInts(1, 0), is(0x0000000100000000L));
        assertThat(packInts(1, 1), is(0x0000000100000001L));
        assertThat(packInts(0, -1), is(0xffffffffL));
        assertThat(packInts(-1, 0), is(0xffffffff00000000L));
        assertThat(packInts(-1, -1), is(-1L));
        assertThat(packInts(0x573ac, 0x732448), is(0x000573ac00732448L));
    }

    /**
     * Test that the int-unpacker works.
     */
    @Test
    public void testUnpackInts() {
        assertThat(unpackInts(0L), is(new int[]{0, 0}));
        assertThat(unpackInts(1L), is(new int[]{0, 1}));
        assertThat(unpackInts(0x0000000100000000L), is(new int[]{1, 0}));
        assertThat(unpackInts(0x0000000100000001L), is(new int[]{1, 1}));
        assertThat(unpackInts(0xffffffffL), is(new int[]{0, -1}));
        assertThat(unpackInts(0xffffffff00000000L), is(new int[]{-1, 0}));
        assertThat(unpackInts(-1L), is(new int[]{-1, -1}));
        assertThat(unpackInts(0x000573ac00732448L), is(new int[]{0x573ac, 0x732448}));
    }

    /**
     * Test all methods of {@link DataPack}, using string as a member type.
     */
    @Test
    public void testThatStringPackWorks() {
        IDataPack<String> pack = new DataPack<>();
        // pack = []
        assertThat(pack.size(), is(0));
        assertThat(pack.toIndex("foo"), is(0));
        assertThat(pack.toIndex("bar"), is(1));
        assertThat(pack.toIndex("frotz"), is(2));
        assertThat(pack.toIndex(""), is(3));
        // pack = [foo, bar, frotz, ""]
        assertThat(pack.toIndex("foo"), is(0));
        assertThat(pack.fromIndex(0), is("foo"));
        assertThat(pack.fromIndex(1), is("bar"));
        assertThat(pack.fromIndex(2), is("frotz"));
        assertThat(pack.fromIndex(3), is(""));
        // xyzzy is not in pack
        assertThat(pack.toIndex("xyzzy", false), is(MISSING));
        assertThat(pack.size(), is(4));
        pack.clear();
        // pack = []
        assertThat(pack.size(), is(0));
        assertThat(pack.toIndex("xyzzy"), is(0));
        // pack = [xyzzy]
        assertThat(pack.fromIndex(0), is("xyzzy"));
        assertThat(pack.isFrozen(), is(false));
        assertThat(pack.canLookupValues(), is(true));
        pack.freeze(false);
        // pack = [xyzzy], will not accept new values but can lookup existing values
        assertThat(pack.isFrozen(), is(true));
        assertThat(pack.canLookupValues(), is(true));
        assertThat(pack.size(), is(1));
        assertThat(pack.toIndex("xyzzy"), is(0));
        assertThat(pack.fromIndex(0), is("xyzzy"));
        assertThat(pack.toIndex("foo"), is(MISSING));
        pack.freeze(true);
        // pack = [xyzzy], wil not accept new values, cannot lookup values
        assertThat(pack.isFrozen(), is(true));
        assertThat(pack.canLookupValues(), is(false));
        assertThat(pack.size(), is(1));
        assertThat(pack.toIndex("xyzzy"), is(MISSING));
        assertThat(pack.fromIndex(0), is("xyzzy"));
        assertThat(pack.toIndex("foo"), is(MISSING));
        // we can clear a frozen pack
        pack.clear();
        assertThat(pack.size(), is(0));
    }

    /**
     * Test all methods of {@link LongDataPack}.
     */
    @Test
    public void testThatLongPackWorks() {
        IDataPack<Long> pack = new LongDataPack();
        // pack = []
        assertThat(pack.size(), is(0));
        assertThat(pack.toIndex(1L), is(0));
        assertThat(pack.toIndex(-1L), is(1));
        assertThat(pack.toIndex(0xdeadbeefdeadbeefL), is(2));
        assertThat(pack.toIndex(0L), is(3));
        // pack = [1, -1, deadbeefdeadbeef, 0]
        assertThat(pack.toIndex(1L), is(0));
        assertThat(pack.fromIndex(0), is(1L));
        assertThat(pack.fromIndex(1), is(-1L));
        assertThat(pack.fromIndex(2), is(0xdeadbeefdeadbeefL));
        assertThat(pack.fromIndex(3), is(0L));
        // 12345 is not in pack
        assertThat(pack.toIndex(12345L, false), is(MISSING));
        assertThat(pack.size(), is(4));
        pack.clear();
        // pack = []
        assertThat(pack.size(), is(0));
        assertThat(pack.toIndex(12345L), is(0));
        // pack = [12345]
        assertThat(pack.fromIndex(0), is(12345L));
        assertThat(pack.isFrozen(), is(false));
        assertThat(pack.canLookupValues(), is(true));
        pack.freeze(false);
        // pack = [12345], will not accept new values but can lookup existing values
        assertThat(pack.isFrozen(), is(true));
        assertThat(pack.canLookupValues(), is(true));
        assertThat(pack.size(), is(1));
        assertThat(pack.toIndex(12345L), is(0));
        assertThat(pack.fromIndex(0), is(12345L));
        assertThat(pack.toIndex(23456L), is(MISSING));
        pack.freeze(true);
        // pack = [12345], wil not accept new values, cannot lookup values
        assertThat(pack.isFrozen(), is(true));
        assertThat(pack.canLookupValues(), is(false));
        assertThat(pack.size(), is(1));
        assertThat(pack.toIndex(12345L), is(MISSING));
        assertThat(pack.fromIndex(0), is(12345L));
        assertThat(pack.toIndex(23456L), is(MISSING));
        // we can clear a frozen pack
        pack.clear();
        assertThat(pack.size(), is(0));
    }

    /**
     * Test all methods of {@link CommodityTypeDataPack}.
     */
    @Test
    public void testThatCommodityTypeSetWorks() {
        IDataPack<CommodityType> pack = new CommodityTypeDataPack();
        // pack = []
        assertThat(pack.size(), is(0));
        assertThat(pack.toIndex(CPU_NO_KEY), is(0));
        assertThat(pack.toIndex(CPU_KEY1), is(1));
        assertThat(pack.toIndex(CPU_KEY2), is(2));
        // pack = [(CPU, ""), (CPU, "key1"), (CPU, "key2")]
        assertThat(pack.toIndex(CPU_NO_KEY), is(0));
        assertThat(pack.fromIndex(0), is(CPU_NO_KEY));
        assertThat(pack.fromIndex(1), is(CPU_KEY1));
        assertThat(pack.fromIndex(2), is(CPU_KEY2));
        // (CPU, "xxx") and (MEM, "") are not in pack
        assertThat(pack.toIndex(CPU_XXX, false), is(MISSING));
        assertThat(pack.toIndex(MEM_NO_KEY, false), is(MISSING));
        assertThat(pack.size(), is(3));
        pack.clear();
        // pack = []
        assertThat(pack.size(), is(0));
        assertThat(pack.toIndex(CPU_XXX), is(0));
        // pack = [(CPU, "xxx")]
        assertThat(pack.fromIndex(0), is(CPU_XXX));
        assertThat(pack.isFrozen(), is(false));
        assertThat(pack.canLookupValues(), is(true));
        pack.freeze(false);
        // pack = [(CPU, "xxx")], will not accept new values but can lookup existing values
        assertThat(pack.isFrozen(), is(true));
        assertThat(pack.canLookupValues(), is(true));
        assertThat(pack.size(), is(1));
        assertThat(pack.toIndex(CPU_XXX), is(0));
        assertThat(pack.fromIndex(0), is(CPU_XXX));
        assertThat(pack.toIndex(MEM_NO_KEY), is(MISSING));
        pack.freeze(true);
        // pack = [(CPU, "xxx")], wil not accept new values, cannot lookup values
        assertThat(pack.isFrozen(), is(true));
        assertThat(pack.canLookupValues(), is(false));
        assertThat(pack.size(), is(1));
        assertThat(pack.toIndex(CPU_XXX), is(MISSING));
        assertThat(pack.fromIndex(0), is(CPU_XXX));
        assertThat(pack.toIndex(MEM_NO_KEY), is(MISSING));
        // we can clear a frozen pack
        pack.clear();
        assertThat(pack.size(), is(0));
    }

    private static CommodityType createCommodityType(CommodityDTO.CommodityType type, String key) {
        return CommodityType.newBuilder()
                .setType(type.getValue())
                .setKey(key != null ? key : "")
                .build();
    }
}
