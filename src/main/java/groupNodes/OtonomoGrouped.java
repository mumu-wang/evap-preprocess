package groupNodes;

import boundary.BoundariesConfig;
import boundary.BoundariesProfile;
import lombok.SneakyThrows;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.storage.StorageLevel;
import org.locationtech.jts.geom.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.locationtech.jts.io.WKTReader;


import java.io.Serializable;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.monotonically_increasing_id;

/**
 * @program: evap-preprocess
 * @description:
 * @author: Lin.Wang
 * @create: 2021-11-10 15:42
 **/
public class OtonomoGrouped implements Serializable {

    private static final String VEHICLE_ID = "vehicle__identification__otonomo_id";
    private static final String LAT_FIELD = "location__latitude__value";
    private static final String LON_FIELD = "location__longitude__value";
    private static final String LOCATION_CITY_NAME_FILED = "location__city__name";
    private static final String LOCATION_STATE_NAME_FILED = "location__state__name";
    private final String inputPath;
    private final String outputPath;

    public OtonomoGrouped(String inputPath, String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public long filterOtonomoDataByCityName(long startID, int splitSize, String filterName, String stateName) {
        // 1.active spark environment
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("handle data").getOrCreate();
        // 2.read otonomo csv file
        Dataset<Row> csvData = sparkSession.read().format("csv").option("header", "true").load(inputPath);
        // 3.1 filter nodes by geometry
//        Dataset<Row> nodeDataset = filterNodesByGeometry(csvData, filterName);
        Dataset<Row> nodeDataset = filerNodesByCityName(csvData, filterName, stateName);
        // 3.2 add node id
        nodeDataset = addNodeID(nodeDataset, startID);
        // 3.3 group node by vehicle id
        nodeDataset = groupDataByVehicle(nodeDataset, splitSize);
        // 4.write result file
        nodeDataset.persist(StorageLevel.DISK_ONLY());
        nodeDataset.write().mode(SaveMode.Overwrite).option("header", "true").csv(outputPath);
        long nodeSize = nodeDataset.count();
        // 5.deactive spark environment
        sparkSession.close();
        return nodeSize;
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

    /**
     * @param nodeDataset
     * @param filterName, filter name in config file
     * @return
     */
    private Dataset<Row> filterNodesByGeometry(Dataset<Row> nodeDataset, String filterName) {
        if (StringUtils.isNotEmpty(filterName)) {
            Geometry geometry = getGeometryFromConfigByName(filterName);
            return filterDataByGeom(nodeDataset, geometry);
        }
        return nodeDataset;
    }

    private Dataset<Row> filerNodesByCityName(Dataset<Row> nodeDataset, String cityName, String stateName) {
        nodeDataset = nodeDataset.filter((FilterFunction<Row>) x -> {
            String locationCity = StringUtils.strip((String) x.get(x.fieldIndex(LOCATION_CITY_NAME_FILED)));
            String locationState = StringUtils.strip((String) x.get(x.fieldIndex(LOCATION_STATE_NAME_FILED)));
            return StringUtils.endsWithIgnoreCase(locationCity, cityName) && StringUtils.endsWithIgnoreCase(locationState, stateName);
        });
        return nodeDataset;
    }

    @SneakyThrows
    private Geometry getGeometryFromConfigByName(String name) {
        BoundariesProfile boundariesProfile = BoundariesConfig.getBoundariesProfile();
        if (boundariesProfile.getBoundaryMap().containsKey(name)) {
            String boundary = boundariesProfile.getBoundaryMap().get(name);
            WKTReader wktReader = new WKTReader();
            return wktReader.read(boundary);
        }
        return new GeometryFactory().createEmpty(0);
    }

    private boolean isValidLatLon(String lat, String lon) {
        try {
            Double.parseDouble(lat);
            Double.parseDouble(lon);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private Dataset<Row> filterDataByGeom(Dataset<Row> nodeDataset, Geometry polygon) {
        nodeDataset = nodeDataset.filter((FilterFunction<Row>) x -> {
            String lat = (String) x.get(x.fieldIndex(LAT_FIELD));
            String lon = (String) x.get(x.fieldIndex(LON_FIELD));
            if (lat != null && lon != null && isValidLatLon(lat, lon)) {
                Point point = new GeometryFactory(new PrecisionModel(), 4236).createPoint(new Coordinate(Double.parseDouble(lon), Double.parseDouble(lat)));
                return polygon.contains(point);
            }
            return false;
        });
        return nodeDataset;
    }

    public static void main(String[] args) {

    }
}
