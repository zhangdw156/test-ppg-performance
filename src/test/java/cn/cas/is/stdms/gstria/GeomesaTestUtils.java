package cn.cas.is.stdms.gstria;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import lombok.extern.slf4j.Slf4j;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.referencing.CRS;
import org.geotools.util.factory.Hints;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

@Slf4j
public class GeomesaTestUtils {

  // 使用 Caffeine 缓存
  // Key: 配置参数 Map, Value: DataStore 实例
  private static final Cache<Map<String, Object>, DataStore> dsCache =
      Caffeine.newBuilder()
          .maximumSize(10) // 限制最大连接数，防止内存泄漏
          .expireAfterAccess(10, TimeUnit.MINUTES) // 10分钟无访问自动回收（可选）
          // 【核心】：移除监听器。当缓存被 invalidate 或过期时，自动销毁 DataStore
          .removalListener(
              (Map<String, Object> key, DataStore value, RemovalCause cause) -> {
                if (value != null) {
                  try {
                    log.info("Disposing DataStore (Reason: {}): {}", cause, key);
                    value.dispose();
                  } catch (Exception e) {
                    log.error("Error disposing DataStore", e);
                  }
                }
              })
          .build();

  /**
   * 准备ds (单例模式 - Caffeine实现)
   *
   * @return DataStore 实例
   */
  public static DataStore prepareDs(DataStoreConfig dataStoreConfig) {
    Map<String, Object> params = dataStoreConfig.toMap();

    // caffeine.get 是原子操作：如果存在则返回，不存在则创建并存入
    return dsCache.get(
        params,
        k -> {
          try {
            log.info("Creating new DataStore instance for: {}", k);
            return DataStoreFinder.getDataStore(k);
          } catch (IOException e) {
            throw new RuntimeException("Failed to create DataStore", e);
          }
        });
  }

  /**
   * 准备测试数据
   *
   * @throws IOException
   */
  public static void prepareData(String fileName, DataStoreConfig dataStoreConfig)
      throws IOException {
    DataStore ds = prepareDs(dataStoreConfig);
    String resourcePath = "/" + fileName + ".geojson";

    InputStream geoJsonStream = GeomesaTestUtils.class.getResourceAsStream(resourcePath);

    if (geoJsonStream == null) {
      throw new IOException("Cannot find resource: '" + resourcePath + "' in classpath.");
    }

    log.info("Successfully found resource: {}", resourcePath);

    SimpleFeatureCollection featureCollection;
    try (InputStream in = geoJsonStream) {
      featureCollection = (SimpleFeatureCollection) new FeatureJSON().readFeatureCollection(in);
    }

    log.info("Successfully read {} features from {}", featureCollection.size(), fileName);
    SimpleFeatureType sft = featureCollection.getSchema();
    log.info("原始sft索引信息");
    printIndexInfo(sft);
    log.info("typename: {}", sft.getTypeName());
    log.info("attributedescriptors: {}", sft.getAttributeDescriptors());
    log.info("spec: {}", getSpecString(sft));

    // 创建新的 SFT
    SimpleFeatureType newSft = SimpleFeatureTypes.createType(fileName, getSpecString(sft));
    log.info("newSft索引信息");
    printIndexInfo(newSft);

    // 1. 遍历所有字段，区分时空列与非时空列
    for (AttributeDescriptor descriptor : newSft.getAttributeDescriptors()) {
      String fieldName = descriptor.getLocalName();
      Class<?> fieldType = descriptor.getType().getBinding();

      boolean isGeometryColumn = descriptor instanceof GeometryDescriptor;

      boolean isTimeColumn =
          ("dtg".equalsIgnoreCase(fieldName)) && Date.class.isAssignableFrom(fieldType);

      if (!isGeometryColumn && !isTimeColumn) {
        descriptor.getUserData().put("index", "true");
        log.info("为非时空列 [{}]（类型：{}）默认建立索引", fieldName, fieldType.getSimpleName());
      }

      // 4. 时间列：配置DTG时空索引
      if (isTimeColumn) {
        newSft.getUserData().put("geomesa.index.dtg", fieldName);
        log.info("为时间列 [{}] 配置DTG时空索引", fieldName);
      }

      // 5. 几何列：配置空间索引
      if (isGeometryColumn) {
        newSft.getUserData().put("geomesa.index.geom", fieldName);
        log.info("为几何列 [{}] 配置空间索引", fieldName);
      }
    }

    log.info("创建索引后，newSft索引信息");
    printIndexInfo(newSft);
    log.info("sft: {}", sft);
    log.info("newSft: {}", newSft);

    // 尝试创建 Schema
    try {
      ds.createSchema(newSft);
    } catch (Exception e) {
      log.warn("Schema might already exist: {}", e.getMessage());
    }

    log.info("开始将要素写入数据存储...");

    try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
        ds.getFeatureWriterAppend(newSft.getTypeName(), Transaction.AUTO_COMMIT)) {

      try (SimpleFeatureIterator iterator = featureCollection.features()) {
        while (iterator.hasNext()) {
          SimpleFeature feature = iterator.next();
          SimpleFeature toWrite = writer.next();
          toWrite.setAttributes(feature.getAttributes());
          toWrite.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.FALSE);
          writer.write();
        }
      }
    }
    log.info("成功写入 {} 个要素。", featureCollection.size());
  }

  /** 从一个 SimpleFeatureType 对象动态生成其 spec 字符串。 */
  public static String getSpecString(SimpleFeatureType sft) {
    if (sft == null) {
      return "";
    }

    StringJoiner spec = new StringJoiner(",");
    GeometryDescriptor defaultGeom = sft.getGeometryDescriptor();

    for (AttributeDescriptor descriptor : sft.getAttributeDescriptors()) {
      StringBuilder part = new StringBuilder();

      if (defaultGeom != null && defaultGeom.equals(descriptor)) {
        part.append("*geom");
      } else {
        part.append(descriptor.getLocalName());
      }

      part.append(":");
      String name = descriptor.getLocalName();
      Class<?> binding = descriptor.getType().getBinding();

      if ("dtg".equalsIgnoreCase(name)) {
        part.append("Date");
      } else if (binding.equals(byte[].class)) {
        part.append("Bytes");
      } else {
        part.append(binding.getSimpleName());
      }

      if (descriptor instanceof GeometryDescriptor) {
        GeometryDescriptor geomDescriptor = (GeometryDescriptor) descriptor;
        CoordinateReferenceSystem crs = geomDescriptor.getCoordinateReferenceSystem();
        if (crs != null) {
          try {
            Integer srid = CRS.lookupEpsgCode(crs, true);
            if (srid != null) {
              part.append(":srid=").append(srid);
            }
          } catch (Exception e) {
            // ignore
          }
        }
      }
      spec.add(part.toString());
    }

    return spec.toString();
  }

  public static void printIndexInfo(SimpleFeatureType sft) {
    if (sft == null) {
      log.info("SimpleFeatureType为null，无索引信息");
      return;
    }

    log.info("===== 要素类型 [{}] 的索引信息 =====", sft.getTypeName());

    String geomIndexField = (String) sft.getUserData().get("geomesa.index.geom");
    if (geomIndexField != null) {
      AttributeDescriptor geomDescriptor = sft.getDescriptor(geomIndexField);
      String geomType =
          geomDescriptor != null ? geomDescriptor.getType().getBinding().getSimpleName() : "未知类型";
      log.info("空间索引字段: {} (类型: {})", geomIndexField, geomType);
    } else {
      log.info("空间索引字段: 无");
    }

    String dtgIndexField = (String) sft.getUserData().get("geomesa.index.dtg");
    if (dtgIndexField != null) {
      AttributeDescriptor dtgDescriptor = sft.getDescriptor(dtgIndexField);
      String dtgType =
          dtgDescriptor != null ? dtgDescriptor.getType().getBinding().getSimpleName() : "未知类型";
      log.info("时间索引字段: {} (类型: {})", dtgIndexField, dtgType);
    } else {
      log.info("时间索引字段: 无");
    }

    List<String> attributeIndexes = new ArrayList<>();
    for (AttributeDescriptor descriptor : sft.getAttributeDescriptors()) {
      String fieldName = descriptor.getLocalName();
      if ((geomIndexField != null && geomIndexField.equals(fieldName))
          || (dtgIndexField != null && dtgIndexField.equals(fieldName))) {
        continue;
      }

      String indexValue = (String) descriptor.getUserData().get("index");
      if ("true".equals(indexValue)) {
        String fieldType = descriptor.getType().getBinding().getSimpleName();
        attributeIndexes.add(fieldName + " (" + fieldType + ")");
      }
    }

    if (attributeIndexes.isEmpty()) {
      log.info("普通属性索引: 无");
    } else {
      log.info("普通属性索引: {}", String.join(", ", attributeIndexes));
    }

    log.info("======================================");
  }

  /**
   * 用于测试完成清理数据
   *
   * @throws IOException
   */
  public static void dropData(String tableName, DataStoreConfig dataStoreConfig)
      throws IOException {
    DataStore ds = prepareDs(dataStoreConfig);

    if (ds != null) {
      try {
        // 只删除指定的 Schema
        log.info("Removing schema: {}", tableName);
        try {
          ds.removeSchema(tableName);
        } catch (IllegalArgumentException e) {
          log.warn("Schema {} might not exist or already removed.", tableName);
        }
      } catch (Exception e) {
        log.error("Error removing schema " + tableName, e);
        throw e;
      }
    }
  }
}
