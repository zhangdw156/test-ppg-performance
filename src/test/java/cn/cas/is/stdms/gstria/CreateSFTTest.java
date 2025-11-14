package cn.cas.is.stdms.gstria;

import lombok.extern.slf4j.Slf4j;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.junit.jupiter.api.Test;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.Arrays;

@Slf4j
public class CreateSFTTest {
    public static DataStoreConfig dataStoreConfig = DataStoreConfig.PPG;
    public static String spec = "*geom:Point:srid=4326,dtg:Date,taxi_id:Integer";
    public static String typeName = "test1";

    @Test
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