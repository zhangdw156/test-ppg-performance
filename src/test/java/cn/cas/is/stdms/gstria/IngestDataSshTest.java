package cn.cas.is.stdms.gstria;

import com.jcraft.jsch.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.Vector;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class IngestDataSshTest {

    public static DataStoreConfig dataStoreConfig = DataStoreConfig.HBASE;
    public static String typeName = "beijing_taxi";

//    public static String datasetPath = "zhangdw@192.168.1.232:/data6/zhangdw/datasets/beijingshi_tbl_100k";
    public static String datasetPath = "/datas/zhangdw/datasets/beijingshi_tbl_100k";
    public static String SSH_PASSWORD = "ds123456"; // 替换密码

    // 线程数（同时也是 SSH 连接数，建议不要超过 20，以免触发服务器防火墙）
    private static final int THREAD_COUNT = 16;

    @Test
    public void importAllTblFiles() throws Exception {
        DataStore ds = null;
        ExecutorService executor = null;

        try {
            // 1. 初始化 DataStore (这是线程安全的，可以共享)
            ds = DataStoreFinder.getDataStore(dataStoreConfig.toMap());
            if (ds == null) {
                log.error("Could not create DataStore.");
                return;
            }
            SimpleFeatureType sft = ds.getSchema(typeName);
            if (sft == null) throw new RuntimeException("Schema " + typeName + " not found!");

            // 2. 获取文件列表（只需要建立一次临时连接）
            List<String> allFiles = getAllFileNames(datasetPath);
            if (allFiles.isEmpty()) {
                log.warn("No files found.");
                return;
            }

            int totalFiles = allFiles.size();
            log.info("Found {} files. Splitting into {} batches...", totalFiles, THREAD_COUNT);

            // 3. 将文件列表切分成 THREAD_COUNT 份
            List<List<String>> batches = partitionList(allFiles, THREAD_COUNT);

            executor = Executors.newFixedThreadPool(THREAD_COUNT);
            List<Future<?>> futures = new ArrayList<>();
            AtomicInteger processedCounter = new AtomicInteger(0);
            Instant start = Instant.now();

            // 4. 提交任务：每个 Batch 一个任务，每个任务建立自己的 Session
            for (List<String> batch : batches) {
                if (batch.isEmpty()) continue;

                final DataStore finalDs = ds;
                // 提交任务
                futures.add(executor.submit(() -> {
                    processBatch(batch, finalDs, processedCounter, totalFiles);
                }));
            }

            // 5. 等待完成
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    log.error("Batch execution failed", e);
                }
            }

            Instant end = Instant.now();
            Duration duration = Duration.between(start, end);
            log.info("Done! Total: {} files, Time: {}s, Speed: {} files/s",
                    totalFiles, duration.getSeconds(), totalFiles / (double) duration.getSeconds());

        } finally {
            if (executor != null) executor.shutdownNow();
            if (ds != null) ds.dispose();
        }
    }

    // --- 核心改动：以批次为单位，每个线程建立独立的 Session ---
    private void processBatch(List<String> fileNames, DataStore ds, AtomicInteger counter, int total) {
        Session session = null;
        String threadName = Thread.currentThread().getName();

        try {
            boolean isRemote = datasetPath.contains("@");
            String remoteDir = null;

            if (isRemote) {
                SshInfo sshInfo = parseSshPath(datasetPath);
                remoteDir = sshInfo.path;
                // 每个线程建立独立的 SSH 连接
                session = createSession(sshInfo);
                log.info("[{}] SSH Connected. Processing {} files...", threadName, fileNames.size());
            }

            for (String fileName : fileNames) {
                try {
                    if (isRemote) {
                        // 在当前 Session 下处理单个文件
                        // 此时每个 Session 同一时间只有一个 Channel 打开，不会报错
                        processRemoteFile(session, remoteDir, fileName, ds);
                    } else {
                        String fullPath = Paths.get(datasetPath, fileName).toString();
                        processLocalFile(fullPath, fileName, ds);
                    }

                    int current = counter.incrementAndGet();
                    if (current % 50 == 0) {
                        log.info("Progress: {}/{}", current, total);
                    }
                } catch (Exception e) {
                    log.error("[{}] Failed to process '{}': {}", threadName, fileName, e.getMessage());
                }
            }
        } catch (Exception e) {
            log.error("[{}] Critical batch error: {}", threadName, e.getMessage());
        } finally {
            // 任务结束，断开连接
            if (session != null && session.isConnected()) {
                session.disconnect();
                log.debug("[{}] SSH Disconnected.", threadName);
            }
        }
    }


    private Session createSession(SshInfo info) throws JSchException {
        JSch jsch = new JSch();
        Session session = jsch.getSession(info.user, info.host, 22);
        session.setPassword(SSH_PASSWORD); // 确保这里使用了正确的密码
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);
        session.connect(10000); // 10s timeout
        return session;
    }

    private void processRemoteFile(Session session, String dir, String fileName, DataStore ds) throws Exception {
        ChannelSftp channel = null;
        try {
            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect();
            String remoteFilePath = dir + "/" + fileName;
            try (InputStream stream = channel.get(remoteFilePath)) {
                processStream(stream, fileName, ds);
            }
        } finally {
            if (channel != null) channel.disconnect();
        }
    }

    private void processLocalFile(String fullPath, String fileName, DataStore ds) throws Exception {
        try (InputStream stream = Files.newInputStream(Paths.get(fullPath))) {
            processStream(stream, fileName, ds);
        }
    }

    private void processStream(InputStream inputStream, String fileName, DataStore ds) throws Exception {
        SimpleFeatureType sft = ds.getSchema(typeName);
        SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(sft);

        // 几何对象解析器
        WKTReader wktReader = new WKTReader(JTSFactoryFinder.getGeometryFactory());

        // 时间格式解析器
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));

        List<SimpleFeature> features = new ArrayList<>();

        String nameWithoutExt = fileName.contains(".") ? fileName.substring(0, fileName.lastIndexOf('.')) : fileName;
        Integer fileTaxiId = 0;
        try {
            if (nameWithoutExt.contains("_")) {
                String numPart = nameWithoutExt.substring(nameWithoutExt.lastIndexOf('_') + 1);
                fileTaxiId = Integer.parseInt(numPart);
            } else {
                fileTaxiId = Integer.parseInt(nameWithoutExt);
            }
        } catch (NumberFormatException e) {
            // log.warn("Filename '{}' ID parse error, default to 0", fileName);
            fileTaxiId = 0;
        }

        // --- 2. 配置 CSV 格式 (竖线分隔) ---
        CSVFormat tblFormat = CSVFormat.Builder.create()
                .setDelimiter('|')
                .setQuote(null)
                .setIgnoreEmptyLines(true)
                .setTrim(true)
                .build();

        // --- 3. 流式读取与解析 ---
        try (InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
             CSVParser parser = new CSVParser(reader, tblFormat)) {

            for (CSVRecord record : parser) {
                try {
                    // 数据格式: UUID | SRID=4326;POINT(x y) | Time | ...

                    // A. 获取 UUID (第0列)
                    String originalId = record.get(0);

                    // B. 解析 Geometry (第1列)
                    String rawWkt = record.get(1);
                    String cleanWkt = rawWkt;
                    // 去除 SRID=4326; 前缀
                    if (rawWkt.contains(";")) {
                        cleanWkt = rawWkt.split(";")[1];
                    }
                    Geometry geometry = wktReader.read(cleanWkt);

                    // C. 解析时间 (第2列)
                    Date date = dateFormat.parse(record.get(2));

                    // D. 设置属性
                    featureBuilder.set("geom", geometry);
                    featureBuilder.set("dtg", date);
                    featureBuilder.set("taxi_id", fileTaxiId);

                    // E. 构建 Feature
                    features.add(featureBuilder.buildFeature(originalId));

                } catch (Exception e) {
                    // 忽略单行解析错误，避免中断整个文件
                }
            }
        }

        // --- 4. 批量写入 HBase ---
        if (!features.isEmpty()) {
            SimpleFeatureStore featureStore = (SimpleFeatureStore) ds.getFeatureSource(typeName);
            featureStore.addFeatures(DataUtilities.collection(features));
            // 这里的日志在多线程下可能会有点乱，可以根据需要注释掉
            // log.info("Imported {} features from {}", features.size(), fileName);
        }
    }

    // --- 工具方法 ---

    private List<String> getAllFileNames(String path) throws Exception {
        if (path.contains("@")) {
            SshInfo info = parseSshPath(path);
            Session session = createSession(info);
            try {
                return listRemoteFiles(session, info.path);
            } finally {
                session.disconnect();
            }
        } else {
            return listLocalFiles(path);
        }
    }

    private List<String> listRemoteFiles(Session session, String directory) throws Exception {
        ChannelSftp channel = null;
        List<String> files = new ArrayList<>();
        try {
            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect();
            Vector<ChannelSftp.LsEntry> entries = channel.ls(directory);
            for (ChannelSftp.LsEntry entry : entries) {
                String name = entry.getFilename();
                if (!entry.getAttrs().isDir() && name.endsWith(".tbl")) {
                    files.add(name);
                }
            }
        } finally {
            if (channel != null) channel.disconnect();
        }
        return files;
    }

    private List<String> listLocalFiles(String pathStr) throws IOException {
        List<String> filenames = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(pathStr))) {
            for (Path entry : stream) {
                if (entry.toString().endsWith(".tbl")) filenames.add(entry.getFileName().toString());
            }
        }
        return filenames;
    }

    private <T> List<List<T>> partitionList(List<T> list, int chunks) {
        List<List<T>> parts = new ArrayList<>();
        int size = list.size();
        int chunkSize = (int) Math.ceil((double) size / chunks);
        for (int i = 0; i < size; i += chunkSize) {
            parts.add(new ArrayList<>(list.subList(i, Math.min(size, i + chunkSize))));
        }
        return parts;
    }

    private SshInfo parseSshPath(String path) {
        String[] parts = path.split(":");
        String remotePath = parts[1];
        String[] loginParts = parts[0].split("@");
        return new SshInfo(loginParts[0], loginParts[1], remotePath);
    }

    static class SshInfo {
        String user, host, path;

        public SshInfo(String u, String h, String p) {
            this.user = u;
            this.host = h;
            this.path = p;
        }
    }
}