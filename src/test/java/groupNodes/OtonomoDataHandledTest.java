package groupNodes;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.Test;

public class OtonomoDataHandledTest {

    @Test
    public void filterOtonomoDataByCityNameFirstBatch() {
        long startId = 35000000000L;
        int slot = 10;
        String inputPath = "file:///d:/ev_data/otonomo_raw_data/*.csv";
        String outputPath = "file:///d:/ev_data/";
        String[] mountainCities = new String[]{"Portland;Oregon", "San Diego;California", "Kansas City;Missouri"
                , "Seattle;Washington", "San Francisco;California", "Honolulu;Hawaii", "Atlanta;Georgia", "Los Angeles;California"};
        String[] flatLandsCities = new String[]{"Miami;Florida", "Chicago;Illinois", "Fresno;California"
                , "Sacramento;California", "Jacksonville;Florida", "Virginia Beach;Virginia", "Long Beach;California",
                "Detroit;Michigan", "Wichita;Kansas", "Houston;Texas", "Dallas;Pennsylvania"};

        long idOffset = 0;
        for (String cityName : mountainCities) {
            long offset = filterNodesByCityName(cityName, idOffset + startId, inputPath, outputPath);
            idOffset += (offset + slot);
        }
        for (String cityName : flatLandsCities) {
            long offset = filterNodesByCityName(cityName, idOffset + startId, inputPath, outputPath);
            idOffset += (offset + slot);
        }

    }

    @Test
    public void filterOtonomoDataByCityNameSecondBatch() {
        long startId = 35007000000L;
        int slot = 10;
        String inputPath = "file:///d:/ev_data/otonomo_raw_data/*.csv";
        String outputPath = "file:///d:/ev_data/terrain_otonomo_nodes/";
        String[] mountainCities = new String[]{"San José;California", "Phoenix;Arizona", "Oakland;California",
                "Austin;Texas", "Bellevue;Washington", "Nashville;Tennessee", "Colorado Springs;Colorado"};
        String[] flatLandsCities = new String[]{"New York City;New York", "Denver;Colorado", "Boulder;Colorado", "Aurora;Colorado"};

        long idOffset = 0;
        for (String cityName : mountainCities) {
            long offset = filterNodesByCityName(cityName, idOffset + startId, inputPath, outputPath);
            idOffset += (offset + slot);
        }
        for (String cityName : flatLandsCities) {
            long offset = filterNodesByCityName(cityName, idOffset + startId, inputPath, outputPath);
            idOffset += (offset + slot);
        }

    }

    private long filterNodesByCityName(String cityStateName, long StartID, String inputPath, String outputPath) {
        String[] cityState = cityStateName.split(";");
        OtonomoDataHandled grouped = new OtonomoDataHandled(inputPath, outputPath + StringUtils.strip(cityState[0]));
        return grouped.filterOtonomoDataByCityName(StartID, 1, StringUtils.strip(cityState[0]), StringUtils.strip(cityState[1]));
    }

    @Test
    public void groupedDataWithNodeIDTest() {
        String inputPath = "file:///d:/ev_data/otonomo_raw_data/*.csv";
        String outputPath = "file:///d:/ev_data/otnomo_data_grouped_by_id/grouped_data/split10";
        OtonomoDataHandled grouped = new OtonomoDataHandled(inputPath, outputPath);
        grouped.groupedDataWithNodeID(2, 10);

    }

    @Test
    public void cityDistributionInOtonomoTest() {
        String inputPath = "file:///d:/ev_data/otonomo_raw_data/*.csv";
        String outputPath = "file:///d:/ev_data/city_distribution/output";
        OtonomoDataHandled grouped = new OtonomoDataHandled(inputPath, outputPath);
        grouped.cityDistributionInOtonomo();
    }

    @Test
    public void vehicleDistinctInOtonomoTest() {
        String inputPath = "file:///d:/ev_data/otonomo_raw_data/*.csv";
        String outputPath = "";
        OtonomoDataHandled grouped = new OtonomoDataHandled(inputPath, outputPath);
        long vehicles = grouped.vehicleDistinctInOtonomo();
        System.out.println("distinct vehicle size is " + vehicles);
    }


    @Test
    public void doColumnsCoverageTest() {
        String inputPath = "file:///d:/ev_data/otonomo_raw_data/*.csv";
        String outputPath = "";
        OtonomoDataHandled grouped = new OtonomoDataHandled(inputPath, outputPath);
        grouped.columnsCoverageInOtonomo();
    }

    @Test
    public void batteryTemperatureRangeInOtonomoTest(){
//        String inputPath = "file:///d:/ev_data/otonomo_raw_sample_data/part_data/*.csv";
        String inputPath = "file:///d:/ev_data/otonomo_raw_data/*.csv";
        String outputPath = "";
        OtonomoDataHandled grouped = new OtonomoDataHandled(inputPath, outputPath);
        grouped.batteryTemperatureRangeInOtonomo();
    }

    @Test
    public void otonomoTripSimplify(){
//        String inputPath = "file:///d:/ev_data/otonomo_trip/sample/trip/*.csv";
        String inputPath = "file:///d:/ev_data/otonomo_trip/entire/trip/*.csv";
        String outputPath = "file:///d:/ev_data/otonomo_trip/entire/out";
        OtonomoDataHandled dataHandled = new OtonomoDataHandled(inputPath, outputPath);
        dataHandled.otonomoTripSimplify();
    }

}