package cn.cas.is.stdms.gstria;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.io.IOException;

@Slf4j
public class ShowTest {

    @Test
    public void testSetup() throws IOException {
        GeomesaTestUtils.prepareData("beijing_subway", DataStoreConfig.HBASE);
        GeomesaTestUtils.prepareData("beijing_subway_station", DataStoreConfig.HBASE);
        GeomesaTestUtils.prepareData("Hogwarts", DataStoreConfig.HBASE);
        GeomesaTestUtils.prepareData("mutants", DataStoreConfig.HBASE);

        GeomesaTestUtils.prepareData("beijing_subway", DataStoreConfig.PG);
        GeomesaTestUtils.prepareData("beijing_subway_station", DataStoreConfig.PG);

        GeomesaTestUtils.prepareData("Hogwarts", DataStoreConfig.PPG);
        GeomesaTestUtils.prepareData("mutants", DataStoreConfig.PPG);
    }

    @Test
    public void testCleanup() throws IOException {
        GeomesaTestUtils.dropData("beijing_subway", DataStoreConfig.HBASE);
        GeomesaTestUtils.dropData("beijing_subway_station", DataStoreConfig.HBASE);
        GeomesaTestUtils.dropData("Hogwarts", DataStoreConfig.HBASE);
        GeomesaTestUtils.dropData("mutants", DataStoreConfig.HBASE);

        GeomesaTestUtils.dropData("beijing_subway", DataStoreConfig.PG);
        GeomesaTestUtils.dropData("beijing_subway_station", DataStoreConfig.PG);

        GeomesaTestUtils.dropData("Hogwarts", DataStoreConfig.PPG);
        GeomesaTestUtils.dropData("mutants", DataStoreConfig.PPG);
    }
}
