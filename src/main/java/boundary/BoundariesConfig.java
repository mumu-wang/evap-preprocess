package boundary;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Objects;

/**
 * @program: evap-preprocess
 * @description: read config file
 * @author: Lin.wang
 * @create: 2021-11-11 10:24
 **/
@Slf4j
public class BoundariesConfig {
    private static BoundariesProfile boundariesProfile;
    private static final String configPath = "config/boundaries_filter.json";

    private BoundariesConfig() {
    }

    public static synchronized BoundariesProfile getBoundariesProfile() {
        if (boundariesProfile == null) {
            Gson gson = new Gson();
            try (Reader reader = new InputStreamReader(Objects.requireNonNull(BoundariesConfig.class.getClassLoader().getResourceAsStream(configPath)))) {
                boundariesProfile = gson.fromJson(reader, BoundariesProfile.class);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                throw new IllegalStateException("get boundaries config file error, which path is " + configPath);
            }
        }
        return boundariesProfile;
    }
}
