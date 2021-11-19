package groupNodes;

import org.junit.Test;

import java.util.stream.Stream;

public class OtonomoGroupedTest {

    @Test
    public void handleOtonomoData() {
        int slot = 10;
        String[] mountainCities = new String[]{"Portland", "San Diego", "Kansas City"
                , "Seattle", "San Francisco", "Honolulu", "Atlanta", "Los Angeles"};
        long idOffset = 0;
        for (String cityName : mountainCities) {
            long offset = filterNodesByCityName(cityName, idOffset + 35000000000L);
            idOffset += (offset + slot);
        }

    }

    private long filterNodesByCityName(String cityName, long StartID) {
        OtonomoGrouped grouped = new OtonomoGrouped(
                "D:\\ev_data\\otonomo_raw_sample_data\\part-00000-6453e5b1-0955-4c6d-895a-e75456da2d98-c000.csv",
                "file:///d:/ev_data/" + cityName);
        return grouped.handleOtonomoData(35000000000L, 1, cityName);
    }
}