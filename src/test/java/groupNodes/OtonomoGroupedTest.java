package groupNodes;

import org.junit.Test;

public class OtonomoGroupedTest {

    @Test
    public void handleOtonomoData() {
        OtonomoGrouped grouped = new OtonomoGrouped(
                "file:///d:/ev_data/otonomo_raw_sample_data/*.csv",
                "file:///d:/ev_data/otonomo_raw_sample_data_split");
        grouped.handleOtonomoData(100, 3, "california");

    }
}