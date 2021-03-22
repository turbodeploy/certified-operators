package com.vmturbo.market.diagnostics;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xddf.usermodel.chart.AxisCrosses;
import org.apache.poi.xddf.usermodel.chart.AxisPosition;
import org.apache.poi.xddf.usermodel.chart.ChartTypes;
import org.apache.poi.xddf.usermodel.chart.LegendPosition;
import org.apache.poi.xddf.usermodel.chart.MarkerStyle;
import org.apache.poi.xddf.usermodel.chart.ScatterStyle;
import org.apache.poi.xddf.usermodel.chart.XDDFChartData;
import org.apache.poi.xddf.usermodel.chart.XDDFChartLegend;
import org.apache.poi.xddf.usermodel.chart.XDDFDataSourcesFactory;
import org.apache.poi.xddf.usermodel.chart.XDDFNumericalDataSource;
import org.apache.poi.xddf.usermodel.chart.XDDFScatterChartData;
import org.apache.poi.xddf.usermodel.chart.XDDFValueAxis;
import org.apache.poi.xssf.usermodel.XSSFChart;
import org.apache.poi.xssf.usermodel.XSSFClientAnchor;
import org.apache.poi.xssf.usermodel.XSSFDrawing;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * This class contains methods that help create a spreadsheet with utilization distributions.
 */
public class WorkbookHelper {

    /**
     * This method Creates a spreadsheet at {@code outputPath} with the source and projected
     * utilization distribution.
     * @param sourceDistribution source topology utilization distribution
     * @param projectedDistribution projected topology utilization distribution
     * @param outputPath path to create workbook
     */
    public void createWorkbookWithUtilizationDistributions(
        Map<Integer, Map<String, UtilizationDistribution>> sourceDistribution,
        Map<Integer, Map<String, UtilizationDistribution>> projectedDistribution,
        String outputPath) {

        XSSFWorkbook workbook = new XSSFWorkbook();
        Set<String> sheetNames = new HashSet<>();
        sheetNames.addAll(getSheetNames(sourceDistribution));
        sheetNames.addAll(getSheetNames(projectedDistribution));

        Map<String, DistributionSheet> distributionSheets = createEmptySheets(workbook, sheetNames);
        populateData(sourceDistribution, distributionSheets, true);
        populateData(projectedDistribution, distributionSheets, false);
        distributionSheets.forEach((sheetName, distSheet) -> distSheet.chart.plot(distSheet.data));

        try {
            FileOutputStream out = new FileOutputStream(new File(outputPath));
            workbook.write(out);
            out.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * We create a sheet for each entity type and commodity type combination.
     * @param distribution the utilization distribution
     * @return the sheet names
     */
    private Set<String> getSheetNames(Map<Integer, Map<String, UtilizationDistribution>> distribution) {
        Set<String> sheetNames = Sets.newHashSet();
        distribution.forEach((entityType, commUtilDistributions) -> {
            commUtilDistributions.forEach((commName, utilDistribution) -> {
                sheetNames.add(getSheetName(entityType, commName));
            });
        });
        return sheetNames;
    }

    private String getSheetName(int entityType, String commName) {
        return EntityType.forNumber(entityType).name() + "_" + commName;
    }

    private void populateData(Map<Integer, Map<String, UtilizationDistribution>> distribution,
                              Map<String, DistributionSheet> distributionSheets,
                              boolean isSource) {
        int firstCol = isSource ? 0 : 3;
        int secondCol = isSource ? 1 : 4;
        String prefix = isSource ? "Source " : "Projected ";
        MarkerStyle markerStyle = isSource ? MarkerStyle.TRIANGLE : MarkerStyle.CIRCLE;
        distribution.forEach((entityType, commUtilDistributions) -> {
            commUtilDistributions.forEach((commName, utilDistribution) -> {
                int rowNum = 1;
                String sheetName = getSheetName(entityType, commName);
                XSSFSheet spreadsheet = distributionSheets.get(sheetName).spreadsheet;
                XSSFRow row = spreadsheet.getRow(0);
                if (row == null) {
                    row = spreadsheet.createRow(0);
                }

                row.createCell(firstCol).setCellValue(prefix + " Utilization");
                row.createCell(secondCol).setCellValue(prefix + " Number of Entities");
                Map<Integer, Integer> utils = utilDistribution.getUnmodifiableUtilDistribution();
                for (Map.Entry<Integer, Integer> utilEntry : utils.entrySet()) {
                    row = spreadsheet.getRow(rowNum);
                    if (row == null) {
                        row = spreadsheet.createRow(rowNum);
                    }
                    rowNum++;
                    row.createCell(firstCol).setCellValue(utilEntry.getKey());
                    row.createCell(secondCol).setCellValue(utilEntry.getValue());
                }

                XDDFNumericalDataSource<Double> xAxisData = XDDFDataSourcesFactory.fromNumericCellRange(spreadsheet,
                    new CellRangeAddress(1, utils.size(), firstCol, firstCol));

                XDDFNumericalDataSource<Double> yAxisData = XDDFDataSourcesFactory.fromNumericCellRange(spreadsheet,
                    new CellRangeAddress(1, utils.size(), secondCol, secondCol));

                XDDFChartData data = distributionSheets.get(sheetName).data;
                XDDFScatterChartData.Series series1 = (XDDFScatterChartData.Series)data.addSeries(xAxisData, yAxisData);
                series1.setTitle(prefix, null);
                series1.setMarkerStyle(markerStyle);
            });
        });
    }

    private Map<String, DistributionSheet> createEmptySheets(XSSFWorkbook workbook, Set<String> sheetNames)  {
        // We will need reference to the the spreadhseet object, the data and the chart later when
        // plotting the data points. Keep a reference to them.
        Map<String, DistributionSheet> sheets = Maps.newHashMap();
        for (String sheetName : sheetNames) {
            XSSFSheet spreadsheet = workbook.createSheet(sheetName);
            XSSFDrawing drawing = spreadsheet.createDrawingPatriarch();
            XSSFClientAnchor anchor = drawing.createAnchor(0, 0, 0, 0, 6, 4, 27, 38);

            XSSFChart chart = drawing.createChart(anchor);
            chart.setTitleText("Utilization distribution");
            chart.setTitleOverlay(false);
            XDDFChartLegend legend = chart.getOrAddLegend();
            legend.setPosition(LegendPosition.TOP_RIGHT);

            XDDFValueAxis bottomAxis = chart.createValueAxis(org.apache.poi.xddf.usermodel.chart.AxisPosition.BOTTOM);
            bottomAxis.setTitle("Utilization");
            XDDFValueAxis leftAxis = chart.createValueAxis(AxisPosition.LEFT);
            leftAxis.setTitle("Num of Entities");
            leftAxis.setCrosses(AxisCrosses.AUTO_ZERO);

            XDDFChartData data = chart.createData(ChartTypes.SCATTER, bottomAxis, leftAxis);

            XDDFScatterChartData scatter = (XDDFScatterChartData)data;
            scatter.setStyle(ScatterStyle.MARKER);
            scatter.setVaryColors(false);
            sheets.put(sheetName, new DistributionSheet(spreadsheet, data, chart));
        }
        return sheets;
    }

    /**
     * A container class which holds a reference to the sheet, the data and the chart object for
     * each sheet.
     */
    private static class DistributionSheet {
        private final XSSFSheet spreadsheet;
        private final XDDFChartData data;
        private final XSSFChart chart;

        private DistributionSheet(XSSFSheet spreadsheet, XDDFChartData data, XSSFChart chart) {
            this.spreadsheet = spreadsheet;
            this.data = data;
            this.chart = chart;
        }
    }
}
