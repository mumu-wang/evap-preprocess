package boundary;

import org.junit.Test;

import static org.junit.Assert.*;

public class BoundariesConfigTest {

    @Test
    public void getBoundariesTest(){
        BoundariesProfile boundariesProfile = BoundariesConfig.getBoundariesProfile();
        assertNotNull(boundariesProfile);
        assertTrue(boundariesProfile.boundaryMap.size() > 0);
    }

}