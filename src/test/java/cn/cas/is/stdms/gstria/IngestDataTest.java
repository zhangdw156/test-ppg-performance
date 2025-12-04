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
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
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

    public static DataStoreConfig dataStoreConfig = DataStoreConfig.HBASE;
    public static String typeName = "beijing_taxi";
    public static String datasetPath = "/datas/zhangdw/datasets/beijingshi_tbl_5000k";

    @Test
    public void importAllTblFiles() throws Exception {
        List<String> tblFiles = getFilesFromLocalFolder();
        if (tblFiles.isEmpty()) {
            log.warn("No TBL files found in folder: {}", datasetPath);
            return;
        }
        log.info("Found {} TBL files to import.", tblFiles.size());

        DataStore ds = null;
        try {
            ds = DataStoreFinder.getDataStore(dataStoreConfig.toMap());
            if (ds == null) {
                log.error("Could not create DataStore.");
                return;
            }

            int totalFiles = tblFiles.size();
            int currentFileIndex = 1;

            // --- 统计总开始时间 ---
            Instant totalStart = Instant.now();

            log.info("========================================");
            log.info("Start processing {} files...", totalFiles);
            log.info("========================================");

            for (String fileName : tblFiles) {
                log.info("[{}/{}] Start processing: {}", currentFileIndex, totalFiles, fileName);

                // --- 统计单个文件开始时间 ---
                Instant fileStart = Instant.now();

                try {
                    String fullPath = Paths.get(datasetPath, fileName).toString();
                    importTblData(ds, fullPath);

                    // --- 统计单个文件结束时间 & 计算耗时 ---
                    Instant fileEnd = Instant.now();
                    Duration fileDuration = Duration.between(fileStart, fileEnd);

                    log.info("[{}/{}] Finished '{}'. Time taken: {} ms ({} s)",
                            currentFileIndex, totalFiles, fileName,
                            fileDuration.toMillis(),
                            String.format("%.2f", fileDuration.toMillis() / 1000.0));

                } catch (Exception e) {
                    log.error("[{}/{}] Failed to process '{}': {}", currentFileIndex, totalFiles, fileName, e.getMessage());
                }
                currentFileIndex++;
            }

            // --- 统计总结束时间 ---
            Instant totalEnd = Instant.now();
            Duration totalDuration = Duration.between(totalStart, totalEnd);

            log.info("========================================");
            log.info("All files processed.");
            log.info("Total files: {}", totalFiles);
            log.info("Total time: {} s ({} ms)",
                    String.format("%.2f", totalDuration.toMillis() / 1000.0),
                    totalDuration.toMillis());
            log.info("========================================");

        } finally {
            if (ds != null) ds.dispose();
        }
    }

    private void importTblData(DataStore ds, String resourcePath) throws Exception {
        SimpleFeatureType sft = ds.getSchema(typeName);
        SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(sft);

        // 专门用于解析 "POINT(116.3 39.8)" 这种字符串
        WKTReader wktReader = new WKTReader(JTSFactoryFinder.getGeometryFactory());

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));

        List<SimpleFeature> features = new ArrayList<>();
        Path filePath = Paths.get(resourcePath);
        String fileName = filePath.getFileName().toString();

        // 解析文件名获取 taxi_id (如果文件名是 id.tbl)
        String taxiIdStr = fileName.contains(".") ? fileName.substring(0, fileName.lastIndexOf('.')) : fileName;
        Integer fileTaxiId = 0;
        try {
            fileTaxiId = Integer.parseInt(taxiIdStr);
        } catch (NumberFormatException e) {
            // 如果文件名不是数字，保持 0
        }

        CSVFormat tblFormat = CSVFormat.Builder.create()
                .setDelimiter('|')         // 分隔符
                .setQuote(null)            // 禁用引号解析
                .setIgnoreEmptyLines(true)
                .setTrim(true)
                .build();

        try (Reader reader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8);
             CSVParser parser = new CSVParser(reader, tblFormat)
        ) {
            for (CSVRecord record : parser) {
                try {
                    // 数据格式: UUID | SRID=4326;POINT(x y) | Time | ID?

                    // 1. 获取 ID (第0列)
                    String originalId = record.get(0);

                    // 2. 解析 Geometry (第1列)
                    String rawWkt = record.get(1);
                    String cleanWkt = rawWkt;

                    // JTS WKTReader 不认识 "SRID=4326;" 前缀，需要切掉
                    if (rawWkt.contains(";")) {
                        cleanWkt = rawWkt.split(";")[1];
                    }
                    Geometry geometry = wktReader.read(cleanWkt);

                    // 3. 解析时间 (第2列)
                    Date date = dateFormat.parse(record.get(2));

                    // 4. 设置属性
                    featureBuilder.set("geom", geometry);
                    featureBuilder.set("dtg", date);
                    featureBuilder.set("taxi_id", fileTaxiId);

                    // 5. 构建 Feature
                    SimpleFeature feature = featureBuilder.buildFeature(originalId);
                    features.add(feature);

                } catch (Exception e) {
                    // 仅打印少量错误日志，避免刷屏
                    if (record.getRecordNumber() < 5) {
                        log.error("Row {} error: {}", record.getRecordNumber(), e.getMessage());
                    }
                }
            }
        }

        if (!features.isEmpty()) {
            SimpleFeatureSource featureSource = ds.getFeatureSource(typeName);
            if (featureSource instanceof SimpleFeatureStore) {
                SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;
                featureStore.addFeatures(DataUtilities.collection(features));
                // 这里可以保留，显示导入了多少条
                log.info("  -> Wrote {} features from file '{}'", features.size(), fileName);
            }
        }
    }

    private List<String> getFilesFromLocalFolder() throws IOException {
        List<String> filenames = new ArrayList<>();
        Path folder = Paths.get(datasetPath);
        if (!Files.exists(folder)) return filenames;

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(folder)) {
            for (Path entry : stream) {
                filenames.add(entry.getFileName().toString());
            }
        }
        return filenames.stream()
                .filter(f -> f.endsWith(".tbl"))
                .collect(Collectors.toList());
    }
}