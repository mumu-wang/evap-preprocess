package boundary;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @program: evap-preprocess
 * @description: Json object
 * @author: Lin.wang
 * @create: 2021-11-11 10:15
 **/

@AllArgsConstructor
@Getter
@Setter
public class BoundariesProfile implements Serializable {
    @SerializedName(value = "boundary_list")
    Map<String, String> boundaryMap;
}
