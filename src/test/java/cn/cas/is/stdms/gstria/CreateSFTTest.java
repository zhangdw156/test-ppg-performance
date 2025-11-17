package cn.cas.is.stdms.gstria;

import lombok.extern.slf4j.Slf4j;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.Arrays;

@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CreateSFTTest {
    public static DataStoreConfig dataStoreConfig = DataStoreConfig.PPG;
    public static String spec = "*geom:Point:srid=4326,dtg:Date,taxi_id:Integer";
    public static String typeName = "performance";

    @Test
    @Order(1)
    public void testCreateSFT() throws Exception {
        DataStore ds = null;
        try {
            ds = DataStoreFinder.getDataStore(dataStoreConfig.toMap());
            SimpleFeatureType sft = DataUtilities.createType(typeName, spec);
            sft.getUserData().put("geomesa.index.dtg", "dtg");
            sft.getDescriptor("taxi_id").getUserData().put("index", "true");
            log.info("Creating schema: '{}'", typeName);
            ds.createSchema(sft);
            log.info("Existing schemas: {}", Arrays.toString(ds.getTypeNames()));
        } finally {
            if (ds != null) {
                ds.dispose();
            }
        }
    }

    @Test
    @Order(2)
    public void testShowSFT() throws Exception {
        DataStore ds = null;
        try {
            ds = DataStoreFinder.getDataStore(dataStoreConfig.toMap());
            log.info("sfts: {}",Arrays.toString(ds.getTypeNames()));
        } finally {
            if (ds != null) {
                ds.dispose();
            }
        }
    }

    @Test
    @Order(3)
    public void testRemoveSFT() throws Exception {
        DataStore ds = null;
        try {
            ds = DataStoreFinder.getDataStore(dataStoreConfig.toMap());
            log.info("map: {}",dataStoreConfig.toMap());
            log.info("ds: {}",ds);
            if (Arrays.asList(ds.getTypeNames()).contains(typeName)) {
                log.info("Removing schema: '{}'", typeName);
                ds.removeSchema(typeName);
            }
        } finally {
            if (ds != null) {
                ds.dispose();
            }
        }
    }

}