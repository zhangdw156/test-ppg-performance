package cn.cas.is.stdms.gstria;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class IngestDataTest {
    public static DataStoreConfig dataStoreConfig = DataStoreConfig.PPG;
    public static String spec = "*geom:Point:srid=4326,dtg:Date,taxi_id:Integer";
    public static String typeName = "performance";
    public static String datasetPath = "D:\\datasets\\beijingshi";

    @BeforeAll
    public static void beforeAll() throws Exception {
//        cleanup();
//        setup();
    }

    @AfterAll
    public static void afterAll() throws Exception {
//        cleanup();
    }

    public static void setup() throws Exception {
        DataStore ds = null;
        try {
            ds = DataStoreFinder.getDataStore(dataStoreConfig.toMap());
            SimpleFeatureType sft = DataUtilities.createType(typeName, spec);
            log.info("Creating schema: '{}'", typeName);
            ds.createSchema(sft);
            log.info("Existing schemas: {}", Arrays.toString(ds.getTypeNames()));
        } finally {
            if (ds != null) {
                ds.dispose();
            }
        }
    }

    public static void cleanup() throws Exception {
        DataStore ds = null;
        try {
            ds = DataStoreFinder.getDataStore(dataStoreConfig.toMap());
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

    @Test
    public void importAllCsvFiles() throws Exception {
        List<String> csvFiles = getFilesFromLocalFolder("beijingshi");
        if (csvFiles.isEmpty()) {
            log.warn("No CSV files found in the 'beijingshi' resource folder.");
            return;
        }
        log.info("Found {} CSV files to import: {}", csvFiles.size(), csvFiles);

        DataStore ds = null;
        try {
            ds = DataStoreFinder.getDataStore(dataStoreConfig.toMap());
            int totalFiles = csvFiles.size();
            int currentFileIndex = 1; // 用于显示 "第 X 个 / 共 Y 个"
            Instant totalStartTime = Instant.now(); // 记录总开始时间

            log.info("==================================================");
            log.info("开始导入数据，共 {} 个文件需要处理。", totalFiles);
            log.info("==================================================");

            for (String fileName : csvFiles) {
                // --- 在循环内部，为每个文件计时并显示进度 ---
                log.info("[进度: {}/{}] 开始处理文件: {}", currentFileIndex, totalFiles, fileName);
                Instant fileStartTime = Instant.now(); // 记录单个文件的开始时间

                try {
                    String fullPath = Paths.get(datasetPath, fileName).toString();
                    importCsvData(ds, fullPath);

                    Instant fileEndTime = Instant.now(); // 记录单个文件的结束时间
                    Duration fileDuration = Duration.between(fileStartTime, fileEndTime);

                    // 使用 String.format 来格式化秒数，保留两位小数
                    String formattedSeconds = String.format("%.2f", fileDuration.toMillis() / 1000.0);
                    log.info("[进度: {}/{}] 文件 '{}' 处理完毕。耗时: {} 秒。",
                            currentFileIndex,
                            totalFiles,
                            fileName,
                            formattedSeconds);

                } catch (Exception e) {
                    log.error("[进度: {}/{}] 处理文件 '{}' 时发生严重错误，已跳过。错误: {}",
                            currentFileIndex,
                            totalFiles,
                            fileName,
                            e.getMessage());
                }

                currentFileIndex++; // 递增文件计数器
            }

            // --- 在循环结束后，打印总结信息 ---
            Instant totalEndTime = Instant.now(); // 记录总结束时间
            Duration totalDuration = Duration.between(totalStartTime, totalEndTime);
            String formattedTotalSeconds = String.format("%.2f", totalDuration.toMillis() / 1000.0);

            log.info("==================================================");
            log.info("所有文件处理完毕！");
            log.info("总计处理文件数: {}", totalFiles);
            log.info("总耗时: {} 秒。", formattedTotalSeconds);
            log.info("==================================================");
        } finally {
            if (ds != null) {
                ds.dispose();
            }
        }
    }

    private void importCsvData(DataStore ds, String resourcePath) throws Exception {
        SimpleFeatureType sft = ds.getSchema(typeName);
        SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(sft);
        GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        List<SimpleFeature> features = new ArrayList<>();

        Path filePath = Paths.get(resourcePath);

        String fileName = filePath.getFileName().toString();

        String taxiIdStr = fileName.substring(0, fileName.lastIndexOf('.'));

        Integer taxiId = Integer.parseInt(taxiIdStr);
        try (Reader reader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8);
             CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).setTrim(true).build())
        ) {
            for (CSVRecord record : parser) {
                try {
                    double lon = Double.parseDouble(record.get("lng"));
                    double lat = Double.parseDouble(record.get("lat"));
                    Point point = geometryFactory.createPoint(new Coordinate(lon, lat));
                    Date date = dateFormat.parse(record.get("dtg_str"));
                    featureBuilder.set("geom", point);
                    featureBuilder.set("dtg", date);
                    featureBuilder.set("taxi_id", taxiId);

                    String featureId = UUID.randomUUID().toString();
                    SimpleFeature feature = featureBuilder.buildFeature(featureId);
                    features.add(feature);
                } catch (Exception e) {
                    log.error("Failed to process row {} in file '{}': {}", record.getRecordNumber(), resourcePath, record, e);
                }
            }
        }


        if (features.isEmpty()) {
            log.warn("No valid features were created from file '{}'.", resourcePath);
            return;
        }

        SimpleFeatureSource featureSource = ds.getFeatureSource(typeName);
        if (featureSource instanceof SimpleFeatureStore) {
            SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;
            SimpleFeatureCollection collection = DataUtilities.collection(features);
            featureStore.addFeatures(collection);
            log.info("Successfully wrote {} features from file '{}'.", features.size(), resourcePath);
        } else {
            throw new IOException("DataStore does not support writing for type: " + typeName);
        }
    }

    private List<String> getFilesFromLocalFolder(String folderPath) throws IOException {
        List<String> filenames = new ArrayList<>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(datasetPath))) {
            // 3. 遍历这个流
            for (Path entry : stream) {
                filenames.add(entry.getFileName().toString());
            }
        }

        return filenames.stream()
                .filter(f -> f.toLowerCase().endsWith(".csv"))
                .collect(Collectors.toList());
    }
}