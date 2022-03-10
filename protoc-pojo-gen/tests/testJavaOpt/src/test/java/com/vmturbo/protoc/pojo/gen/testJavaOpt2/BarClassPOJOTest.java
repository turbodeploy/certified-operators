package com.vmturbo.protoc.pojo.gen.testjavaopt2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.protoc.pojo.gen.testjavaopt2.BarClassDTO.Bar.MyOneOfCase;
import com.vmturbo.protoc.pojo.gen.testjavaopt2.BarClassDTO.Bar.OtherMessage;
import com.vmturbo.protoc.pojo.gen.testjavaopt2.BarClassDTO.Bar.Plant;
import com.vmturbo.protoc.pojo.gen.testjavaopt2.BarClassPOJO.BarImpl;
import com.vmturbo.protoc.pojo.gen.testjavaopt2.BarClassPOJO.BarImpl.BlahImpl;
import com.vmturbo.protoc.pojo.gen.testjavaopt2.BarClassPOJO.BarImpl.BlahView;
import com.vmturbo.protoc.pojo.gen.testjavaopt2.BarClassPOJO.BarImpl.OtherMessageImpl;
import com.vmturbo.protoc.pojo.gen.testjavaopt2.BarClassPOJO.BarImpl.OtherMessageView;
import com.vmturbo.protoc.pojo.gen.testjavaopt2.BarClassPOJO.BarView;

/**
 * BarClassPOJOTest.
 */
public class BarClassPOJOTest {

    /**
     * Expected exception rule.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final BarImpl bar = new BarImpl();
    private final BarClassDTO.Bar defaultProto = BarClassDTO.Bar.getDefaultInstance();

    private final ByteString fooBytes = ByteString.copyFromUtf8("foo");
    private final ByteString barBytes = ByteString.copyFromUtf8("bar");

    /**
     * testHasPrimitive.
     */
    @Test
    public void testHasPrimitive() {
        assertFalse(bar.hasBar());
        bar.setBar(0L);
        assertTrue(bar.hasBar());

        bar.setBar(1L);
        assertTrue(bar.hasBar());
    }

    /**
     * testHasEnum.
     */
    @Test
    public void testHasEnum() {
        assertFalse(bar.hasGardenPlant());
        bar.setGardenPlant(Plant.BUSH);
        assertTrue(bar.hasGardenPlant());
        assertFalse(bar.hasBar());
    }

    /**
     * testHasString.
     */
    @Test
    public void testHasString() {
        assertFalse(bar.hasSomeString());
        bar.setSomeString("");
        assertTrue(bar.hasSomeString());
    }

    /**
     * testHasBoolean.
     */
    @Test
    public void testHasBoolean() {
        assertFalse(bar.hasMyBoolean());
        bar.setMyBoolean(bar.getMyBoolean());
        assertTrue(bar.hasMyBoolean());
    }

    /**
     * testHasSubMessage.
     */
    @Test
    public void testHasSubMessage() {
        assertFalse(bar.hasMyOtherMessage());
        bar.setMyOtherMessage(new OtherMessageImpl());
        assertTrue(bar.hasMyOtherMessage());
    }

    /**
     * testClearPrimitiveField.
     */
    @Test
    public void testClearPrimitiveField() {
        bar.setBar(0L);
        assertTrue(bar.hasBar());
        bar.clearBar();
        assertFalse(bar.hasBar());
    }

    /**
     * testGetVsGetOrCreate on a single (non-repeated, non-oneOf) field.
     */
    @Test
    public void testGetVsGetOrCreateSingleField() {
        final BarClassDTO.Bar.Builder builder = BarClassDTO.Bar.newBuilder();
        assertFalse(builder.hasMyOtherMessage());
        assertFalse(builder.getMyOtherMessage().hasMyInt());
        assertFalse(builder.hasMyOtherMessage());

        builder.getMyOtherMessageBuilder().setMyInt(1);
        assertEquals(1, builder.getMyOtherMessage().getMyInt());
        assertTrue(builder.hasMyOtherMessage());

        assertFalse(bar.hasOtherMessage());
        bar.getOtherMessage();
        assertFalse(bar.hasOtherMessage());

        bar.getOrCreateMyOtherMessage().setMyInt(1);
        assertEquals(1, bar.getMyOtherMessage().getMyInt());
        assertTrue(bar.hasMyOtherMessage());
    }

    /**
     * Test getOrCreate on a oneOf field.
     */
    @Test
    public void testGetVsGetOrCreateOneOfField() {
        final BarClassDTO.Bar.Builder builder = BarClassDTO.Bar.newBuilder();
        builder.setIntVariant(2);
        assertFalse(builder.hasOtherMessage());
        assertFalse(builder.getOtherMessage().hasMyInt());
        assertFalse(builder.hasOtherMessage());
        assertEquals(MyOneOfCase.INT_VARIANT, builder.getMyOneOfCase());

        builder.getOtherMessageBuilder().setMyInt(1);
        assertEquals(1, builder.getOtherMessage().getMyInt());
        assertTrue(builder.hasOtherMessage());
        assertEquals(MyOneOfCase.OTHER_MESSAGE, builder.getMyOneOfCase());
        assertFalse(builder.hasIntVariant());

        bar.setIntVariant(2);
        assertFalse(bar.hasOtherMessage());
        bar.getOtherMessage();
        assertFalse(bar.hasOtherMessage());
        assertEquals(MyOneOfCase.INT_VARIANT, bar.getMyOneOfCase());

        bar.getOrCreateOtherMessage().setMyInt(1);
        assertEquals(1, bar.getOtherMessage().getMyInt());
        assertTrue(bar.hasOtherMessage());
        assertEquals(MyOneOfCase.OTHER_MESSAGE, bar.getMyOneOfCase());
        assertFalse(bar.hasIntVariant());
    }

    /**
     * testClearEnum.
     */
    @Test
    public void testClearEnum() {
        bar.setGardenPlant(Plant.CHRISTMAS_CACTUS);
        assertTrue(bar.hasGardenPlant());
        bar.clearGardenPlant();
        assertFalse(bar.hasGardenPlant());
    }

    /**
     * testClearString.
     */
    @Test
    public void testClearString() {
        bar.setSomeString("foo");
        assertTrue(bar.hasSomeString());
        bar.clearSomeString();
        assertFalse(bar.hasSomeString());
    }

    /**
     * testClearBoolean.
     */
    @Test
    public void testClearBoolean() {
        bar.setMyBoolean(false);
        assertTrue(bar.hasMyBoolean());
        bar.clearMyBoolean();
        assertFalse(bar.hasMyBoolean());
    }

    /**
     * testClearSubMessage.
     */
    @Test
    public void testClearSubMessage() {
        bar.setMyOtherMessage(new OtherMessageImpl());
        assertTrue(bar.hasMyOtherMessage());
        bar.clearMyOtherMessage();
        assertFalse(bar.hasMyOtherMessage());
    }

    /**
     * testGetSetPrimitiveFieldWithDefault.
     */
    @Test
    public void testGetSetPrimitiveFieldWithDefault() {
        assertEquals(defaultProto.getMyDoubleWithDefault(),
            bar.getMyDoubleWithDefault(), 0);
        bar.setMyDoubleWithDefault(0.5);
        assertEquals(0.5, bar.getMyDoubleWithDefault(), 0);
        bar.clearMyDoubleWithDefault();
        assertEquals(defaultProto.getMyDoubleWithDefault(),
            bar.getMyDoubleWithDefault(), 0);
    }

    /**
     * testGetSetBytesFieldWithDefault.
     */
    @Test
    public void testGetSetBytesFieldWithDefault() {
        assertEquals(defaultProto.getBytesWithDefault(),
            bar.getBytesWithDefault());
        bar.setBytesWithDefault(fooBytes);
        assertEquals(fooBytes, bar.getBytesWithDefault());
        bar.clearBytesWithDefault();
        assertEquals(defaultProto.getBytesWithDefault(),
            bar.getBytesWithDefault());
    }

    /**
     * testOneOfMessageVariant.
     */
    @Test
    public void testOneOfMessageVariant() {
        assertEquals(MyOneOfCase.MYONEOF_NOT_SET, bar.getMyOneOfCase());
        assertEquals(defaultProto.getBoolVariant(), bar.getBoolVariant());
        assertEquals(defaultProto.getStringVariant(), bar.getStringVariant());
        assertEquals(defaultProto.getIntVariant(), bar.getIntVariant());
        assertEquals(defaultProto.getNoDefaultString(), bar.getNoDefaultString());
        assertEquals(defaultProto.getOtherMessage(), bar.getOtherMessage().toProto());
        assertEquals(defaultProto.getBlah(), bar.getBlah().toProto());
        assertEquals(defaultProto.getBytesVariant(), bar.getBytesVariant());
        assertEquals(defaultProto.getBytesVariantWithDefualt(), bar.getBytesVariantWithDefualt());

        bar.setStringVariant("foo");
        assertEquals(MyOneOfCase.STRING_VARIANT, bar.getMyOneOfCase());
        bar.setIntVariant(2);
        assertEquals(MyOneOfCase.INT_VARIANT, bar.getMyOneOfCase());
        bar.setBoolVariant(false);
        assertEquals(MyOneOfCase.BOOL_VARIANT, bar.getMyOneOfCase());

        // Clearing string and int variants when the bool variant is set does nothing
        bar.clearStringVariant();
        bar.clearIntVariant();
        assertEquals(MyOneOfCase.BOOL_VARIANT, bar.getMyOneOfCase());

        bar.clearBoolVariant();
        assertEquals(MyOneOfCase.MYONEOF_NOT_SET, bar.getMyOneOfCase());

        bar.setOtherMessage(new OtherMessageImpl().setMyInt(1));
        assertEquals(1, bar.getOtherMessage().getMyInt());
        assertEquals(MyOneOfCase.OTHER_MESSAGE, bar.getMyOneOfCase());

        bar.setBlah(new BlahImpl().setStr("blah"));
        assertEquals("blah", bar.getBlah().getStr());
        assertEquals(MyOneOfCase.BLAH, bar.getMyOneOfCase());

        bar.setBytesVariant(barBytes);
        assertEquals(barBytes, bar.getBytesVariant());
        assertEquals(MyOneOfCase.BYTES_VARIANT, bar.getMyOneOfCase());
    }

    /**
     * testOneOfIntVariant.
     */
    @Test
    public void testOneOfIntVariant() {
        assertFalse(bar.hasIntVariant());
        assertEquals(0, bar.getIntVariant());
        bar.setIntVariant(3);
        assertTrue(bar.hasIntVariant());
        assertEquals(3, bar.getIntVariant());
    }

    /**
     * testOneOfBoolVariant.
     */
    @Test
    public void testOneOfBoolVariant() {
        assertFalse(bar.hasBoolVariant());
        assertEquals(defaultProto.getBoolVariant(), bar.getBoolVariant());
        bar.setBoolVariant(false);
        assertTrue(bar.hasBoolVariant());
        assertFalse(bar.getBoolVariant());
    }

    /**
     * testOneOfStringVariant.
     */
    @Test
    public void testOneOfStringVariant() {
        assertFalse(bar.hasStringVariant());
        assertFalse(bar.hasNoDefaultString());

        assertEquals(defaultProto.getStringVariant(), bar.getStringVariant());
        assertEquals(defaultProto.getNoDefaultString(), bar.getNoDefaultString());

        bar.setStringVariant("bar");
        assertTrue(bar.hasStringVariant());
        assertEquals("bar", bar.getStringVariant());
    }

    /**
     * testOneOfEnumVariant.
     */
    @Test
    public void testOneOfEnumVariant() {
        assertFalse(bar.hasYourPlant());
        assertFalse(bar.hasThirdPlant());

        assertEquals(defaultProto.getYourPlant(), bar.getYourPlant());
        assertEquals(defaultProto.getThirdPlant(), bar.getThirdPlant());

        bar.setYourPlant(Plant.OAK);
        assertTrue(bar.hasYourPlant());
        assertFalse(bar.hasThirdPlant());

        assertEquals(Plant.OAK, bar.getYourPlant());
        assertEquals(defaultProto.getThirdPlant(), bar.getThirdPlant());

        bar.setThirdPlant(Plant.OAK);
        assertFalse(bar.hasYourPlant());
        assertTrue(bar.hasThirdPlant());

        assertEquals(Plant.OAK, bar.getThirdPlant());
        assertEquals(defaultProto.getYourPlant(), bar.getYourPlant());
    }

    /**
     * We should not be able to modify the lists returned by the list getter when it is empty.
     */
    @Test
    public void testModifyEmptyRepeatedList() {
        expectedException.expect(UnsupportedOperationException.class);
        bar.getRepeatedIntList()
            .add(1);
    }

    /**
     * We should not be able to modify the lists returned by the list getter when it is not empty.
     */
    @Test
    public void testModifyNonEmptyRepeatedList() {
        expectedException.expect(UnsupportedOperationException.class);
        bar.addRepeatedInt(1)
            .getRepeatedIntList()
            .add(1);
    }

    /**
     * We should not be able to add a null element to a list for a repeated field.
     */
    @Test
    public void testAddNull() {
        expectedException.expect(NullPointerException.class);
        bar.addRepeatedBlah(null);
    }

    /**
     * We should not be able to add a null element to a list for a repeated field
     * even when adding a collection.
     */
    @Test
    public void testAddAllNull() {
        expectedException.expect(NullPointerException.class);
        bar.addAllRepeatedInt(Arrays.asList(1, 2, 3, null, 5));
    }

    /**
     * Test that we get the correct exception when trying to set a value at an index out of bounds.
     */
    @Test
    public void testSetAtIndexOutOfBoundsOnEmpty() {
        expectedException.expect(IndexOutOfBoundsException.class);
        expectedException.expectMessage("Index: 3, Size: 0");
        bar.setRepeatedInt(3, 5);
    }

    /**
     * Test that we get the correct exception when trying to set a value at an index out of bounds.
     */
    @Test
    public void testSetAtIndexOutOfBoundsOnNonEmpty() {
        expectedException.expect(IndexOutOfBoundsException.class);
        expectedException.expectMessage("Index: 3, Size: 2");
        bar.addRepeatedInt(1);
        bar.addRepeatedInt(2);
        bar.setRepeatedInt(3, 5);
    }

    /**
     * testRepeatedInt.
     */
    @Test
    public void testRepeatedInt() {
        assertEquals(0, bar.getRepeatedIntList().size());
        assertEquals(1234, bar.addRepeatedInt(1234).getRepeatedInt(0).intValue());
        assertEquals(1234, bar.getRepeatedIntList().get(0).intValue());
        assertEquals(1, bar.getRepeatedIntCount());
        assertEquals(12345, bar.setRepeatedInt(0, 12345).getRepeatedInt(0).intValue());
        assertEquals(0, bar.clearRepeatedInt().getRepeatedIntCount());
    }

    /**
     * testRepeatedBool.
     */
    @Test
    public void testRepeatedBool() {
        assertEquals(0, bar.getRepeatedBoolList().size());
        assertTrue(bar.addRepeatedBool(true).getRepeatedBool(0));
        assertTrue(bar.getRepeatedBoolList().get(0));
        assertEquals(1, bar.getRepeatedBoolCount());
        assertFalse(bar.setRepeatedBool(0, false).getRepeatedBool(0));
        assertEquals(0, bar.clearRepeatedBool().getRepeatedBoolCount());
    }

    /**
     * testRepeatedString.
     */
    @Test
    public void testRepeatedString() {
        assertEquals(0, bar.getRepeatedStringList().size());
        assertEquals("1234", bar.addRepeatedString("1234").getRepeatedString(0));
        assertEquals("1234", bar.getRepeatedStringList().get(0));
        assertEquals(1, bar.getRepeatedStringCount());
        assertEquals("foo", bar.setRepeatedString(0, "foo").getRepeatedString(0));
        assertEquals(0, bar.clearRepeatedString().getRepeatedStringCount());
    }

    /**
     * testRepeatedMessage.
     */
    @Test
    public void testRepeatedMessage() {
        assertEquals(0, bar.getRepeatedBlahList().size());
        assertEquals(new BlahImpl().setStr("foo"),
            bar.addRepeatedBlah(new BlahImpl().setStr("foo")).getRepeatedBlah(0));
        assertEquals(new BlahImpl().setStr("foo"), bar.getRepeatedBlahList().get(0));
        assertEquals(1, bar.getRepeatedBlahCount());
        assertEquals(new BlahImpl().setStr("bar"),
            bar.setRepeatedBlah(0, new BlahImpl().setStr("bar")).getRepeatedBlah(0));
        assertEquals(0, bar.clearRepeatedBlah().getRepeatedBlahCount());
    }

    /**
     * testIntEquals.
     */
    @Test
    public void testIntEquals() {
        final BarImpl other = new BarImpl();

        bar.setBar(1234L);
        bar.setIntVariant(3);
        assertNotEquals(other, bar);

        other.setBar(1234L);
        other.setIntVariant(3);
        assertEquals(other, bar);
    }

    /**
     * testBooleanEquals.
     */
    @Test
    public void testBooleanEquals() {
        final BarImpl other = new BarImpl();

        bar.setMyBoolean(true);
        bar.setBoolVariant(false);
        assertNotEquals(other, bar);

        other.setMyBoolean(true);
        other.setBoolVariant(false);
        assertEquals(other, bar);
    }

    /**
     * testStringEquals.
     */
    @Test
    public void testStringEquals() {
        final BarImpl other = new BarImpl();

        bar.setSomeString("foo");
        bar.setStringVariant("bar");
        assertNotEquals(other, bar);

        other.setSomeString("foo");
        other.setStringVariant("bar");
        assertEquals(other, bar);
    }

    /**
     * testByteStringEquals.
     */
    @Test
    public void testByteStringEquals() {
        final BarImpl other = new BarImpl();

        bar.setMyBytes(fooBytes);
        bar.setBytesWithDefault(barBytes);
        assertNotEquals(other, bar);

        other.setMyBytes(fooBytes);
        other.setBytesWithDefault(barBytes);
        assertEquals(other, bar);
    }

    /**
     * testEnumEquals.
     */
    @Test
    public void testEnumEquals() {
        final BarImpl other = new BarImpl();

        bar.setGardenPlant(Plant.POTATO);
        bar.setYourPlant(Plant.BUSH);
        assertNotEquals(other, bar);

        other.setGardenPlant(Plant.POTATO);
        other.setYourPlant(Plant.BUSH);
        assertEquals(other, bar);
    }

    /**
     * testMessageEquals.
     */
    @Test
    public void testMessageEquals() {
        final BarImpl other = new BarImpl();

        bar.setMyOtherMessage(new OtherMessageImpl().setMyInt(2));
        bar.setBlah(new BlahImpl().setStr("foo"));
        assertNotEquals(other, bar);

        other.setMyOtherMessage(new OtherMessageImpl().setMyInt(2));
        other.setBlah(new BlahImpl().setStr("foo"));
        assertEquals(other, bar);
    }

    /**
     * testMapContainsOnEmpty.
     */
    @Test
    public void testMapContainsOnEmpty() {
        assertFalse(bar.containsMyMap("foo"));
    }

    /**
     * testNullKeyMapContains.
     */
    @Test
    public void testNullKeyMapContains() {
        expectedException.expect(NullPointerException.class);
        bar.containsMyMap(null);
    }

    /**
     * testContains.
     */
    @Test
    public void testContains() {
        assertFalse(bar.containsMyMap("foo"));
        bar.putMyMap("foo", new OtherMessageImpl());
        assertTrue(bar.containsMyMap("foo"));
    }

    /**
     * testPutNullKey.
     */
    @Test
    public void testPutNullKey() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("Key cannot be null");
        bar.putMyMap(null, new OtherMessageImpl());
    }

    /**
     * testPutNullValue.
     */
    @Test
    public void testPutNullValue() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("Value cannot be null");
        bar.putMyMap("foo", null);
    }

    /**
     * testPutAllNull.
     */
    @Test
    public void testPutAllNull() {
        expectedException.expect(NullPointerException.class);
        bar.putAllMyMap(null);
    }

    /**
     * testPutAllWithNullValue.
     */
    @Test
    public void testPutAllWithNullValue() {
        expectedException.expect(NullPointerException.class);
        final Map<String, OtherMessageImpl> map = new HashMap<>();
        map.put("foo", null);

        bar.putAllMyMap(map);
    }

    /**
     * testPutAllWithNullKey.
     */
    @Test
    public void testPutAllWithNullKey() {
        expectedException.expect(NullPointerException.class);
        final Map<String, OtherMessageImpl> map = new HashMap<>();
        map.put(null, new OtherMessageImpl().setMyInt(1));

        bar.putAllMyMap(map);
    }

    /**
     * testPutAll.
     */
    @Test
    public void testPutAll() {
        assertEquals(0, bar.getMyMapCount());
        final Map<String, OtherMessageImpl> map1 = ImmutableMap.of(
            "foo", new OtherMessageImpl().setMyInt(1),
            "bar", new OtherMessageImpl().setMyInt(2)
        );

        bar.putAllMyMap(map1);
        assertTrue(bar.containsMyMap("foo"));
        assertTrue(bar.containsMyMap("bar"));
        assertEquals(2, bar.getMyMapCount());

        final Map<String, OtherMessageImpl> map2 = ImmutableMap.of(
            "foo", new OtherMessageImpl().setMyInt(3),
            "baz", new OtherMessageImpl().setMyInt(4),
            "quux", new OtherMessageImpl().setMyInt(5)
        );

        bar.putAllMyMap(map2);
        assertTrue(bar.containsMyMap("foo"));
        assertTrue(bar.containsMyMap("bar"));
        assertTrue(bar.containsMyMap("baz"));
        assertTrue(bar.containsMyMap("quux"));
        assertEquals(new OtherMessageImpl().setMyInt(3), bar.getMyMapOrThrow("foo"));
        assertEquals(4, bar.getMyMapCount());
    }

    /**
     * testSettingWithDefault. We should not allow modification of the default when
     * we set a field to a default.
     */
    @Test
    public void testSettingWithDefault() {
        bar.setOtherMessage(OtherMessageView.getDefaultInstance());
        bar.getOrCreateMyOtherMessage().setMyInt(1234);
        assertEquals(1234, bar.getMyOtherMessage().getMyInt());
        assertEquals(defaultProto.getMyOtherMessage().getMyInt(),
            OtherMessageView.getDefaultInstance().getMyInt());
        assertNotSame(OtherMessageView.getDefaultInstance(), bar.getOtherMessage());
    }

    /**
     * Test getOrThrow.
     */
    @Test
    public void testGetOrThrow() {
        bar.putMyMap("foo", new OtherMessageImpl().setMyInt(2));
        assertEquals(new OtherMessageImpl().setMyInt(2), bar.getMyMapOrThrow("foo"));

        expectedException.expect(IllegalArgumentException.class);
        bar.getMyMapOrThrow("bar");
    }

    /**
     * testGetOrDefault.
     */
    @Test
    public void testGetOrDefault() {
        bar.putMyMap("foo", new OtherMessageImpl().setMyInt(2));
        assertEquals(new OtherMessageImpl().setMyInt(2), bar.getMyMapOrDefault("foo", null));

        assertEquals(new OtherMessageImpl().setMyInt(5),
            bar.getMyMapOrDefault("bar", new OtherMessageImpl().setMyInt(5)));
    }

    /**
     * testClearMap.
     */
    @Test
    public void testClearMap() {
        bar.clearPrimitivesMap();
        assertEquals(0, bar.getPrimitivesMapCount());

        bar.putPrimitivesMap(1, true);
        bar.putPrimitivesMap(2, false);
        assertEquals(2, bar.getPrimitivesMapCount());

        bar.clearPrimitivesMap();
        assertEquals(0, bar.getPrimitivesMapCount());
    }

    /**
     * Test removing from a map.
     */
    @Test
    public void testRemoveMap() {
        bar.removeMyMap("foo"); // Nothing happens when you remove a key that is not present.
        bar.putMyMap("bar", new OtherMessageImpl().setMyInt(1));

        assertEquals(1, bar.getMyMapCount());
        bar.removeMyMap("bar");
        assertEquals(0, bar.getMyMapCount());
    }

    /**
     * Test removing from a repeated field.
     */
    @Test
    public void testRemoveRepeated() {
        bar.addRepeatedInt(1);
        bar.addRepeatedInt(2);
        bar.addRepeatedInt(3);

        assertEquals(3, bar.getRepeatedIntCount());
        bar.removeRepeatedInt(0);
        assertEquals(2, bar.getRepeatedIntCount());

        assertEquals(2, (int)bar.getRepeatedInt(0));
        assertEquals(3, (int)bar.getRepeatedInt(1));

        bar.removeRepeatedInt(1);
        assertEquals(1, bar.getRepeatedIntCount());
        assertEquals(2, (int)bar.getRepeatedInt(0));
    }

    /**
     * Test removing at invalid index.
     */
    @Test public void testRemoveRepeatedInvalid() {
        expectedException.expect(IndexOutOfBoundsException.class);
        bar.removeRepeatedInt(0);
    }

    /**
     * testCannotModifyEmptyMapDirectly.
     */
    @Test
    public void testCannotModifyEmptyMapDirectly() {
        expectedException.expect(UnsupportedOperationException.class);
        bar.getMyMapMap().put("foo", new OtherMessageImpl().setMyInt(1));
    }

    /**
     * testCannotModifyNonEmptyMapDirectly.
     */
    @Test
    public void testCannotModifyNonEmptyMapDirectly() {
        bar.putMyMap("foo", new OtherMessageImpl().setMyInt(1));

        expectedException.expect(UnsupportedOperationException.class);
        bar.getMyMapMap().clear();
    }

    /**
     * testMapEquals.
     */
    @Test
    public void testMapEquals() {
        final BarImpl other = new BarImpl();
        assertEquals(bar, other);

        bar.putMyMap("foo", new OtherMessageImpl().setMyInt(1));
        assertNotEquals(bar, other);

        other.putMyMap("foo", new OtherMessageImpl().setMyInt(1));
        assertEquals(bar, other);

        bar.putPrimitivesMap(1, false);
        bar.putPrimitivesMap(2, true);
        assertNotEquals(bar, other);

        other.putPrimitivesMap(1, false);
        other.putPrimitivesMap(2, true);
        assertEquals(bar, other);
    }

    /**
     * testRepeatedEquals.
     */
    @Test
    public void testRepeatedEquals() {
        final BarImpl other = new BarImpl();

        bar.addAllRepeatedInt(Arrays.asList(1, 2, 3, 4));
        assertNotEquals(bar, other);
        other.addAllRepeatedInt(Arrays.asList(1, 2, 3, 4));
        assertEquals(bar, other);

        bar.addRepeatedBlah(new BlahImpl().setStr("foo"));
        assertNotEquals(bar, other);
        other.addRepeatedBlah(new BlahImpl().setStr("foo"));
        assertEquals(bar, other);

        bar.addAllRepeatedBool(Arrays.asList(true, false, true));
        assertNotEquals(bar, other);
        other.addAllRepeatedBool(Arrays.asList(true, false, true));
        assertEquals(bar, other);

        bar.addAllRepeatedString(Arrays.asList("foo", "bar", "baz"));
        assertNotEquals(bar, other);
        other.addAllRepeatedString(Arrays.asList("foo", "bar", "baz"));
        assertEquals(bar, other);
    }

    /**
     * testDefaultInstance.
     */
    @Test
    public void testDefaultInstance() {
        assertEquals(BarView.getDefaultInstance(), BarView.getDefaultInstance());
        assertSame(BarView.getDefaultInstance(), BarView.getDefaultInstance());
    }

    /**
     * testToFromProtoRepeatedAndMapFields.
     */
    @Test
    public void testToFromProtoRepeatedAndMapFields() {
        final BarClassDTO.Bar proto = defaultProto.toBuilder()
            .setRequiredFloat(1.23f)
            .addAllRepeatedInt(Arrays.asList(1, 2, 3, 4))
            .addRepeatedBlah(BarClassDTO.Bar.BlahDTO.newBuilder().setStr("foo").build())
            .addAllRepeatedBool(Arrays.asList(true, false, true))
            .addAllRepeatedString(Arrays.asList("foo", "bar", "baz"))
            .putMyMap("foo", new OtherMessageImpl().setMyInt(1).toProto())
            .putMyMap("bar", new OtherMessageImpl().setMyInt(2).toProto())
            .putPrimitivesMap(3, false)
            .putPrimitivesMap(4, true)
            .build();

        final BarImpl bar = BarImpl.fromProto(proto);
        assertEquals(Arrays.asList(1, 2, 3, 4), bar.getRepeatedIntList());
        assertEquals(new BlahImpl().setStr("foo"), bar.getRepeatedBlah(0));
        assertEquals(Arrays.asList(true, false, true), bar.getRepeatedBoolList());
        assertEquals(Arrays.asList("foo", "bar", "baz"), bar.getRepeatedStringList());
        assertEquals(ImmutableMap.of("foo", new OtherMessageImpl().setMyInt(1),
            "bar", new OtherMessageImpl().setMyInt(2)), bar.getMyMapMap());
        assertEquals(ImmutableMap.of(3, false, 4, true), bar.getPrimitivesMapMap());
        assertEquals(proto, bar.toProto());
    }

    /**
     * testToByteArray.
     */
    @Test
    public void testToByteArray() {
        final BarClassDTO.Bar proto = defaultProto.toBuilder()
            .setRequiredFloat(1.23f)
            .addAllRepeatedInt(Arrays.asList(1, 2, 3, 4))
            .addRepeatedBlah(BarClassDTO.Bar.BlahDTO.newBuilder().setStr("foo").build())
            .addAllRepeatedBool(Arrays.asList(true, false, true))
            .addAllRepeatedString(Arrays.asList("foo", "bar", "baz"))
            .addAllRepeatedBytes(Arrays.asList(fooBytes, barBytes))
            .build();

        final BarImpl bar = new BarImpl()
            .setRequiredFloat(1.23f)
            .addAllRepeatedInt(Arrays.asList(1, 2, 3, 4))
            .addRepeatedBlah(new BlahImpl().setStr("foo"))
            .addAllRepeatedBool(Arrays.asList(true, false, true))
            .addAllRepeatedString(Arrays.asList("foo", "bar", "baz"))
            .addAllRepeatedBytes(Arrays.asList(fooBytes, barBytes));

        final byte[] protoBytes = proto.toByteArray();
        final byte[] pojoBytes = bar.toByteArray();
        assertEquals(protoBytes.length, pojoBytes.length);
        for (int i = 0; i < protoBytes.length; i++) {
            assertEquals(protoBytes[i], pojoBytes[i]);
        }
    }

    /**
     * testHashing.
     */
    @Test
    public void testHashing() {
        final BarImpl bar = new BarImpl()
            .setRequiredFloat(1.23f)
            .addAllRepeatedInt(Arrays.asList(1, 2, 3, 4))
            .addRepeatedBlah(new BlahImpl().setStr("foo"))
            .addAllRepeatedBool(Arrays.asList(true, false, true))
            .addAllRepeatedString(Arrays.asList("foo", "bar", "baz"))
            .setDoubleVariant(3.0)
            .putMyMap("foo", new OtherMessageImpl().setMyInt(2))
            .putMyMap("bar", new OtherMessageImpl().setMyInt(3))
            .addAllRepeatedBytes(Arrays.asList(fooBytes, barBytes))
            .putPrimitivesMap(1, false);
        BarImpl barCopy = bar.copy();
        assertEquals(bar, barCopy);
        assertEquals(bar.hashCode(), barCopy.hashCode());

        final Map<BarImpl, Integer> barMap = new HashMap<>();
        barMap.put(bar, 3);
        assertEquals(1, barMap.size());
        assertTrue(barMap.containsKey(barCopy));
        assertEquals(3, (int)barMap.get(bar));
        assertEquals(3, (int)barMap.get(barCopy));

        bar.setIntVariant(1);
        assertNotEquals(barCopy.hashCode(), bar.hashCode());
        barCopy = bar.copy();

        bar.setBlah(new BlahImpl().setStr("foo"));
        assertNotEquals(barCopy.hashCode(), bar.hashCode());
        barCopy = bar.copy();

        bar.setStringVariant("foo");
        assertNotEquals(barCopy.hashCode(), bar.hashCode());
        barCopy = bar.copy();

        bar.setBoolVariant(false);
        assertNotEquals(barCopy.hashCode(), bar.hashCode());
        barCopy = bar.copy();

        bar.setFloatVariant(4.2f);
        assertNotEquals(barCopy.hashCode(), bar.hashCode());
        barCopy = bar.copy();

        bar.setLongVariant(123456L);
        assertNotEquals(barCopy.hashCode(), bar.hashCode());
        barCopy = bar.copy();

        bar.clearMyMap();
        assertNotEquals(barCopy.hashCode(), bar.hashCode());
        barCopy = bar.copy();

        bar.clearRepeatedBlah();
        assertNotEquals(barCopy.hashCode(), bar.hashCode());
        barCopy = bar.copy();

        bar.clearRepeatedBool();
        assertNotEquals(barCopy.hashCode(), bar.hashCode());
        barCopy = bar.copy();

        bar.clearRepeatedInt();
        assertNotEquals(barCopy.hashCode(), bar.hashCode());
        barCopy = bar.copy();

        bar.clearRepeatedBytes();
        assertNotEquals(barCopy.hashCode(), bar.hashCode());
    }

    /**
     * testEquals.
     */
    @Test
    public void testEquals() {
        final BarImpl bar = new BarImpl()
            .setRequiredFloat(1.23f)
            .addAllRepeatedInt(Arrays.asList(1, 2, 3, 4))
            .addRepeatedBlah(new BlahImpl().setStr("foo"))
            .addAllRepeatedBool(Arrays.asList(true, false, true))
            .addAllRepeatedString(Arrays.asList("foo", "bar", "baz"))
            .setDoubleVariant(3.0)
            .addAllRepeatedBytes(Arrays.asList(fooBytes, barBytes))
            .putMyMap("foo", new OtherMessageImpl().setMyInt(2))
            .putMyMap("bar", new OtherMessageImpl().setMyInt(3))
            .putPrimitivesMap(1, false);
        BarImpl barCopy = bar.copy();
        assertEquals(bar, barCopy);

        bar.setIntVariant(1);
        assertNotEquals(barCopy, bar);
        barCopy = bar.copy();
        assertEquals(bar, barCopy);

        bar.setBlah(new BlahImpl().setStr("foo"));
        assertNotEquals(barCopy, bar);
        barCopy = bar.copy();
        assertEquals(bar, barCopy);

        bar.setStringVariant("foo");
        assertNotEquals(barCopy, bar);
        barCopy = bar.copy();
        assertEquals(bar, barCopy);

        bar.setBoolVariant(false);
        assertNotEquals(barCopy, bar);
        barCopy = bar.copy();
        assertEquals(bar, barCopy);

        bar.setFloatVariant(4.2f);
        assertNotEquals(barCopy, bar);
        barCopy = bar.copy();
        assertEquals(bar, barCopy);

        bar.setLongVariant(123456L);
        assertNotEquals(barCopy, bar);
        barCopy = bar.copy();
        assertEquals(bar, barCopy);

        bar.clearMyMap();
        assertNotEquals(barCopy, bar);
        barCopy = bar.copy();
        assertEquals(bar, barCopy);

        bar.clearRepeatedBlah();
        assertNotEquals(barCopy, bar);
        barCopy = bar.copy();
        assertEquals(bar, barCopy);

        bar.clearRepeatedBool();
        assertNotEquals(barCopy, bar);
        barCopy = bar.copy();
        assertEquals(bar, barCopy);

        bar.clearRepeatedInt();
        assertNotEquals(barCopy, bar);
        barCopy = bar.copy();
        assertEquals(bar, barCopy);

        bar.clearRepeatedBytes();
        assertNotEquals(barCopy, bar);
        barCopy = bar.copy();
        assertEquals(bar, barCopy);
    }

    /**
     * testClear.
     */
    @Test
    public void testClear() {
        final BarImpl bar = new BarImpl()
            .setRequiredFloat(1.23f)
            .addAllRepeatedInt(Arrays.asList(1, 2, 3, 4))
            .addRepeatedBlah(new BlahImpl().setStr("foo"))
            .addAllRepeatedBool(Arrays.asList(true, false, true))
            .addAllRepeatedString(Arrays.asList("foo", "bar", "baz"))
            .setDoubleVariant(3.0)
            .setGardenPlant(Plant.MAPLE)
            .addAllRepeatedBytes(Arrays.asList(fooBytes, barBytes))
            .putMyMap("foo", new OtherMessageImpl().setMyInt(2))
            .putMyMap("bar", new OtherMessageImpl().setMyInt(3))
            .putPrimitivesMap(1, false);
        bar.clear();
        assertEquals(new BarImpl(), bar);
    }

    /**
     * testCopy.
     */
    @Test
    public void testCopy() {
        final BarImpl bar = new BarImpl()
            .setRequiredFloat(1.23f)
            .addAllRepeatedInt(Arrays.asList(1, 2, 3, 4))
            .addRepeatedBlah(new BlahImpl().setStr("foo"))
            .addAllRepeatedBool(Arrays.asList(true, false, true))
            .addAllRepeatedString(Arrays.asList("foo", "bar", "baz"))
            .addAllRepeatedBytes(Arrays.asList(fooBytes, barBytes))
            .putMyMap("foo", new OtherMessageImpl().setMyInt(2))
            .putMyMap("bar", new OtherMessageImpl().setMyInt(3))
            .addAllRepeatedBytes(Arrays.asList(fooBytes, barBytes))
            .putPrimitivesMap(1, false);

        assertEquals(bar, bar.copy());

        bar.setBoolVariant(false);
        assertEquals(bar, bar.copy());

        bar.setGardenPlant(Plant.MAPLE);
        assertEquals(bar, bar.copy());

        bar.setMyBoolean(true);
        assertEquals(bar, bar.copy());

        bar.clearMyBoolean();
        assertEquals(bar, bar.copy());

        bar.setBlah(new BlahImpl().setStr("blah"));
        assertEquals(bar, bar.copy());

        bar.clearBlah();
        assertEquals(bar, bar.copy());

        bar.clearRepeatedBool();
        assertEquals(bar, bar.copy());

        bar.clearRepeatedBytes();
        assertEquals(bar, bar.copy());

        final BarImpl empty = new BarImpl();
        assertEquals(empty, empty.copy());
    }

    /**
     * Since parent sets a bit field, we want to ensure that we get equality whether we have a parent or not.
     */
    @Test
    public void testEqualityParentNoParent() {
        final OtherMessageImpl a = new OtherMessageImpl().setMyInt(34934);
        final OtherMessageImpl b = a.copy();

        bar.setMyOtherMessage(a);
        assertEquals(a, b); // a and b should still be equal even though a has a parent and b does not.
    }

    /**
     * Parent setting should always be unset after a copy even if we are copying
     * from something without a parent.
     */
    @Test
    public void testParentValueNotCopied() {
        final OtherMessageImpl a = new OtherMessageImpl().setMyInt(34934);
        bar.setMyOtherMessage(a);

        assertTrue(a.hasParent());
        assertFalse(a.copy().hasParent());
        assertNotSame(a, a.copy());
    }

    /**
     * When copying a POJO with a child, parent should be set on the child on
     * both the original child and the copied child. The copied child should
     * not be reference-equal to the original.
     */
    @Test
    public void testCopiedChildrenHaveParentSet() {
        bar.setMyOtherMessage(new OtherMessageImpl().setMyInt(34934));
        bar.setOtherMessage(new OtherMessageImpl().setMyInt(1234));
        bar.addRepeatedBlah(new BlahImpl().setStr("foo"));
        bar.putMyMap("foo", new OtherMessageImpl().setMyInt(9999));

        final BarImpl copy = bar.copy();

        assertTrue(bar.getOrCreateMyOtherMessage().hasParent());
        assertTrue(copy.getOrCreateMyOtherMessage().hasParent());
        assertNotSame(bar.getMyOtherMessage(), copy.getMyOtherMessage());

        assertTrue(bar.getOrCreateOtherMessage().hasParent());
        assertTrue(copy.getOrCreateOtherMessage().hasParent());
        assertNotSame(bar.getOtherMessage(), copy.getOtherMessage());

        assertTrue(bar.getRepeatedBlahImpl(0).hasParent());
        assertTrue(copy.getRepeatedBlahImpl(0).hasParent());
        assertNotSame(bar.getRepeatedBlah(0), copy.getRepeatedBlah(0));

        assertTrue(bar.getMyMapImplOrThrow("foo").hasParent());
        assertTrue(copy.getMyMapImplOrThrow("foo").hasParent());
        assertNotSame(bar.getMyMapOrThrow("foo"), copy.getMyMapOrThrow("foo"));
    }

    /**
     * When converting a proto with a child, the parent should be set on the
     * converted pojo child.
     */
    @Test
    public void testFromProtoChildrenHaveParentSet() {
        final BarClassDTO.Bar.Builder proto = defaultProto.toBuilder();
        proto.setMyOtherMessage(OtherMessage.newBuilder().setMyInt(34934));
        proto.setOtherMessage(OtherMessage.newBuilder().setMyInt(1234));
        proto.addRepeatedOtherMessage(OtherMessage.newBuilder().setMyInt(5678));
        proto.putMyMap("foo", OtherMessage.newBuilder().setMyInt(9999).build());
        final BarImpl copy = BarImpl.fromProto(proto);

        assertTrue(copy.getOrCreateMyOtherMessage().hasParent());
        assertTrue(copy.getOrCreateOtherMessage().hasParent());
        assertTrue(copy.getRepeatedOtherMessageImpl(0).hasParent());
        assertTrue(copy.getMyMapImplOrThrow("foo").hasParent());
    }

    /**
     * testParentForSingleField.
     */
    @Test
    public void testParentForSingleField() {
        assertTrue(bar.getOrCreateMyOtherMessage().hasParent());
        final BarImpl bar2 = new BarImpl();

        final OtherMessageImpl other = new OtherMessageImpl().setMyInt(1234);
        assertFalse(other.hasParent());
        bar.setMyOtherMessage(other);
        bar2.setMyOtherMessage(other);
        assertTrue(other.hasParent());

        assertEquals(1234, bar.getMyOtherMessage().getMyInt());
        assertEquals(1234, bar2.getMyOtherMessage().getMyInt());

        // Changing the original should change the original parent, but not the second one.
        other.setMyInt(2345);
        assertEquals(2345, bar.getMyOtherMessage().getMyInt());
        assertEquals(1234, bar2.getMyOtherMessage().getMyInt());
        assertNotSame(bar.getMyOtherMessage(), bar2.getMyOtherMessage());
    }

    /**
     * testParentForOneOfVariant.
     */
    @Test
    public void testParentForOneOfVariant() {
        assertTrue(bar.getOrCreateOtherMessage().hasParent());
        final BarImpl bar2 = new BarImpl();

        final OtherMessageImpl other = new OtherMessageImpl().setMyInt(1234);
        assertFalse(other.hasParent());
        bar.setOtherMessage(other);
        bar2.setOtherMessage(other);
        assertTrue(other.hasParent());

        assertEquals(1234, bar.getOtherMessage().getMyInt());
        assertEquals(1234, bar2.getOtherMessage().getMyInt());

        // Changing the original should change the original parent, but not the second one.
        other.setMyInt(2345);
        assertEquals(2345, bar.getOtherMessage().getMyInt());
        assertEquals(1234, bar2.getOtherMessage().getMyInt());
        assertNotSame(bar.getOtherMessage(), bar2.getOtherMessage());
    }

    /**
     * testParentForRepeatedField.
     */
    @Test
    public void testParentForRepeatedField() {
        bar.addRepeatedOtherMessage(new OtherMessageImpl().setMyInt(1234));
        final BarImpl bar2 = new BarImpl();

        bar2.addRepeatedOtherMessage(bar.getRepeatedOtherMessage(0));

        assertEquals(bar.getRepeatedOtherMessage(0), bar2.getRepeatedOtherMessage(0));
        assertNotSame(bar.getRepeatedOtherMessage(0), bar2.getRepeatedOtherMessage(0));
        assertTrue(bar.getRepeatedOtherMessageImpl(0).hasParent());
        assertTrue(bar2.getRepeatedOtherMessageImpl(0).hasParent());

        bar.getRepeatedOtherMessageImpl(0).setMyInt(5678);
        assertEquals(5678, bar.getRepeatedOtherMessage(0).getMyInt());
        assertEquals(1234, bar2.getRepeatedOtherMessage(0).getMyInt());
    }

    /**
     * testParentForMapField.
     */
    @Test
    public void testParentForMapField() {
        bar.putMyMap("foo", new OtherMessageImpl().setMyInt(1234));

        final BarImpl bar2 = new BarImpl();
        bar2.putAllMyMap(bar.getMyMapMap());

        assertEquals(bar.getMyMapOrThrow("foo"), bar2.getMyMapOrThrow("foo"));
        assertNotSame(bar.getMyMapOrThrow("foo"), bar2.getMyMapOrThrow("foo"));
        assertTrue(bar.getMyMapImplOrThrow("foo").hasParent());
        assertTrue(bar2.getMyMapImplOrThrow("foo").hasParent());

        bar.getMyMapImplOrThrow("foo").setMyInt(2345);
        assertEquals(2345, bar.getMyMapOrThrow("foo").getMyInt());
        assertEquals(1234, bar2.getMyMapOrThrow("foo").getMyInt());
    }

    /**
     * testSetRepeatedFieldViaView.
     */
    @Test
    public void testSetRepeatedFieldViaView() {
        final BlahImpl blah = new BlahImpl().setStr("foo");
        final BlahView blahView = blah;
        bar.addRepeatedBlah(blahView);
        bar.addAllRepeatedBlah(Arrays.asList(blahView, blahView, blahView, blahView));

        assertEquals(5, bar.getRepeatedBlahCount());
        assertEquals("foo", bar.getRepeatedBlah(0).getStr());
        assertEquals("foo", bar.getRepeatedBlah(1).getStr());

        blah.setStr("bar");
        assertEquals("bar", bar.getRepeatedBlah(0).getStr());
        assertEquals("foo", bar.getRepeatedBlah(1).getStr());
    }

    /**
     * testClearDoesNotClearParent.
     */
    @Test
    public void testClearDoesNotClearParent() {
        final OtherMessageImpl o = bar.getOrCreateOtherMessage();
        assertTrue(o.hasParent());

        o.clear();
        assertTrue(o.hasParent());
    }

    /**
     * Test serialization and deserialization.
     *
     * @throws Exception If an exception occurs.
     */
    @Test
    public void testSerializeDeserialize() throws Exception {
        final BarImpl bar = new BarImpl()
            .setRequiredFloat(1.23f)
            .addAllRepeatedInt(Arrays.asList(1, 2, 3, 4))
            .addRepeatedBlah(new BlahImpl().setStr("foo"))
            .addAllRepeatedBool(Arrays.asList(true, false, true))
            .addAllRepeatedString(Arrays.asList("foo", "bar", "baz"))
            .addAllRepeatedBytes(Arrays.asList(fooBytes, barBytes))
            .putMyMap("foo", new OtherMessageImpl().setMyInt(2))
            .putMyMap("bar", new OtherMessageImpl().setMyInt(3))
            .addAllRepeatedBytes(Arrays.asList(fooBytes, barBytes))
            .putPrimitivesMap(1, false);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(bar);
        oos.close();

        final String serializedBar = Base64.getEncoder().encodeToString(baos.toByteArray());

        byte[] data = Base64.getDecoder().decode(serializedBar);
        ObjectInputStream ois = new ObjectInputStream(
            new ByteArrayInputStream(data));
        Object o = ois.readObject();
        ois.close();

        // The deserialized object should be equal to the original
        assertEquals(bar, o);
    }
}
