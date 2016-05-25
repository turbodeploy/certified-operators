package com.vmturbo.platform.analysis.translators;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;

import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.EndDiscoveredTopology.CommodityResizeDependencyEntry;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.EndDiscoveredTopology.CommodityResizeDependency;

/**
 * A test case for the {@link ProtobufToAnalysis} class.
 */
@RunWith(JUnitParamsRunner.class)
public class ProtobufToAnalysisTest {
    // Fields

    // Methods for converting PriceFunctionDTOs.

    @Test
    @Ignore
    public final void testPriceFunction() {
        fail("Not yet implemented"); // TODO
    }

    // Methods for converting QuantityUpdatingFunctionDTOs.

    @Test
    @Ignore
    public final void testQuantityUpdatingFunction() {
        fail("Not yet implemented"); // TODO
    }

    // Methods for converting EconomyDTOs.

    @Test
    @Ignore
    public final void testCommoditySpecification() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    @Ignore
    public final void testBasket_ListOfCommoditySpecificationTO() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    @Ignore
    public final void testBasket_ShoppingListTO() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    @Ignore
    public final void testBasket_TraderTO() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    @Ignore
    public final void testAddShoppingList() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    @Ignore
    public final void testPopulateCommoditySoldSettings() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    @Ignore
    public final void testPopulateCommoditySold() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    @Ignore
    public final void testPopulateTraderSettings() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    @Ignore
    public final void testTraderState() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    @Ignore
    public final void testAddTrader() {
        fail("Not yet implemented"); // TODO
    }

    // Methods for converting ActionDTOs.

    @Test
    @Ignore
    public final void testAction() {
        fail("Not yet implemented"); // TODO
    }

    // Methods for converting CommunicationDTOs.

    @Test
    @Ignore
    public final void testPopulateQuantityUpdatingFunctions() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public final void testCommodityResizeDependencyMap() {
        CommodityResizeDependencyEntry.Builder entryBuilder = CommodityResizeDependencyEntry.newBuilder();
        List<CommodityResizeDependency> listValues = new ArrayList<>(2);

        // VMem(0)  <--  Mem(1), MemProvisioned(2)
        CommodityResizeDependency.Builder memDependencyBuilder = CommodityResizeDependency.newBuilder();
        memDependencyBuilder.setDependentCommodityType(1);    // Mem
        UpdatingFunctionTO.Builder minFunctionBuilder = UpdatingFunctionTO.newBuilder();
        UpdatingFunctionTO.Min.Builder minBuilder = UpdatingFunctionTO.Min.newBuilder();
        UpdatingFunctionTO.Min minUpdateFunction = minBuilder.build();
        minFunctionBuilder.setMin(minUpdateFunction);
        UpdatingFunctionTO minFunction = minFunctionBuilder.build();
        memDependencyBuilder.setUpdateFunction(minFunction);
        CommodityResizeDependency dependencyMem = memDependencyBuilder.build();
        listValues.add(dependencyMem);

        CommodityResizeDependency.Builder memProvDependencyBuilder = CommodityResizeDependency.newBuilder();
        memProvDependencyBuilder.setDependentCommodityType(2);    // MemProvisioned
        UpdatingFunctionTO.Builder projectFunctionBuilder = UpdatingFunctionTO.newBuilder();
        UpdatingFunctionTO.ProjectSecond.Builder projectBuilder = UpdatingFunctionTO.ProjectSecond.newBuilder();
        UpdatingFunctionTO.ProjectSecond projectUpdateFunction = projectBuilder.build();
        projectFunctionBuilder.setProjectSecond(projectUpdateFunction);
        UpdatingFunctionTO projectFunction = projectFunctionBuilder.build();
        memProvDependencyBuilder.setUpdateFunction(projectFunction);
        CommodityResizeDependency dependencyMemProvisioned = memProvDependencyBuilder.build();
        listValues.add(dependencyMemProvisioned);

        entryBuilder.setCommodityType(0); // VMem
        entryBuilder.addAllCommodityResizeDependency(listValues);
        CommodityResizeDependencyEntry mapEntry = entryBuilder.build();

        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
        try {
            mapEntry.writeTo(byteOutputStream);
        } catch (Exception ioe) {

        }

        // read back
        InputStream inputStream = new ByteArrayInputStream(byteOutputStream.toByteArray());
        CommodityResizeDependencyEntry.Builder reconstittutedEntryBuilder = CommodityResizeDependencyEntry.newBuilder();
        try {
            reconstittutedEntryBuilder.mergeFrom(inputStream);
        } catch (Exception ioe) {

        }

        CommodityResizeDependencyEntry reconstittutedEntry = reconstittutedEntryBuilder.build();
        // Resize Dependency List for VMem (0)
        assertEquals(reconstittutedEntry.getCommodityType(), 0);
        assertEquals(reconstittutedEntry.getCommodityResizeDependencyCount(), 2);
        List<CommodityResizeDependency> vMemDependencyList = reconstittutedEntry.getCommodityResizeDependencyList();

        CommodityResizeDependency memDependency = vMemDependencyList.get(0);
        assertEquals(memDependency.getDependentCommodityType(), 1);
        assertEquals(memDependency.getUpdateFunction(), minFunction);

        CommodityResizeDependency memProvDependency = vMemDependencyList.get(1);
        assertEquals(memProvDependency.getDependentCommodityType(), 2);
        assertEquals(memProvDependency.getUpdateFunction(), projectFunction);

    }

} // end ProtobufToAnalysisTest class
