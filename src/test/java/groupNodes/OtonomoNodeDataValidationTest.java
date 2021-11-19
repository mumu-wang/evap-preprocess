package groupNodes;

import filterNodes.CityNodeDataValidation;
import org.junit.Test;

import java.io.*;

public class OtonomoNodeDataValidationTest implements Serializable {

    private static String[] terrainType = new String[]{"flatlands","moutains"};

    private static String[] moutains = new String[]{ "Atlanta","Honolulu", "San Diego", "Kansas City","Los Angeles","Portland"
            , "Seattle", "San Francisco"  };

//    private static String[] flatlands = new String[]{ "Miami","Chicago", "Fresno", "Sacramento","Jacksonville","Virginia Beach"
//            , "Long Beach", "Detroit","Wichita","Houston", "New York","Dallas"};

    private static String[] flatlands = new String[]{ "New York","Dallas"};

    @Test
    public void handPortlandData() throws IOException {

        long idOffset = 0;
        String output = "";
        for (String cityName : flatlands) {
            double offset = filterNodesByCityPolygon(cityName, idOffset + 35000000000L);
            System.out.printf("%s post-process data percent:%.2f " +
                    "",cityName, offset*100);
            System.out.println();
        }
    }


    private double filterNodesByCityPolygon(String cityName, long StartID) {
        CityNodeDataValidation grouped = new CityNodeDataValidation(
                "D:\\ev_data\\Terrain Data\\flatlands\\"+cityName+"\\*.csv",
                "file:///d:/ev_data/" + cityName);
        return grouped.handleCityData(35000000000L, 1, cityName);
    }
}
