package groupNodes;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

public class OtonomoGroupedTest {

    @Test
    public void filterOtonomoDataByCityName() {
        long startId = 35000000000L;
        int slot = 10;
        String inputPath = "file:///d:/ev_data/otonomo_raw_data/*.csv";
        String outputPath = "file:///d:/ev_data/";
        String[] mountainCities = new String[]{"Portland;Oregon", "San Diego;California", "Kansas City;Missouri"
                , "Seattle;Washington", "San Francisco;California", "Honolulu;Hawaii", "Atlanta;Georgia", "Los Angeles;California"};
        String[] flatLandsCities = new String[]{"Miami;Florida", "Chicago;Illinois", "Fresno;California"
                , "Sacramento;California", "Jacksonville;Florida", "Virginia Beach;Virginia", "Long Beach;California",
                "Detroit;Michigan", "Wichita;Kansas", "Houston;Texas", "Dallas;Pennsylvania", "New York; "};

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
        OtonomoGrouped grouped = new OtonomoGrouped(inputPath, outputPath + StringUtils.strip(cityState[0]));
        return grouped.filterOtonomoDataByCityName(StartID, 1, StringUtils.strip(cityState[0]), StringUtils.strip(cityState[1]));
    }
}