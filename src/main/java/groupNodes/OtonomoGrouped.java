package groupNodes;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.monotonically_increasing_id;

/**
 * @program: evap-preprocess
 * @description:
 * @author: Lin.Wang
 * @create: 2021-11-10 15:42
 **/
public class OtonomoGrouped {

    private static final String VEHICLE_ID = "vehicle__identification__otonomo_id";

    public void handleOtonomoData(String inputPath, String outputPath, long startID, int splitSize) {
        // 1.active spark environment
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("handle data").getOrCreate();
        // 2.read otonomo csv file
        Dataset<Row> csvData = sparkSession.read().format("csv").option("header", "true").load(inputPath);
        // 3.add node id
        Dataset<Row> nodeDataset = addNodeID(csvData, startID);
        // 4.group node by vehicle id
        nodeDataset = groupDataByVehicle(nodeDataset, splitSize);
        // 5.write result file
        nodeDataset.write().mode(SaveMode.Overwrite).option("header", "true").csv(outputPath);
        // 6.deactive spark environment
        sparkSession.close();
    }

    /**
     * @param nodeDataset
     * @param startID,    node start ID
     * @return
     */
    private Dataset<Row> addNodeID(Dataset<Row> nodeDataset, long startID) {
        // add increasing id, but don't consecutive
        nodeDataset = nodeDataset.withColumn("monotonically_increasing_id", monotonically_increasing_id());
        // generation consecutive id from 1
        WindowSpec window = Window.orderBy(col("monotonically_increasing_id"));
        nodeDataset = nodeDataset.withColumn("increasing_id", row_number().over(window));
        // generation node id from startID
        nodeDataset = nodeDataset.withColumn("node_id", nodeDataset.col("increasing_id").$plus(startID - 1));
        // drop temporary columns
        nodeDataset = nodeDataset.drop(col("monotonically_increasing_id")).drop("increasing_id");
        return nodeDataset;
    }

    /**
     * @param nodeDataset
     * @param splitSize,  the number of split files
     * @return
     */
    private Dataset<Row> groupDataByVehicle(Dataset<Row> nodeDataset, int splitSize) {
        return nodeDataset.repartition(splitSize, col(VEHICLE_ID));
    }


    public static void main(String[] args) {

    }
}
