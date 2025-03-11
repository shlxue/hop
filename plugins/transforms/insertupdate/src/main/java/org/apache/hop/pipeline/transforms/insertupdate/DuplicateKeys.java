package org.apache.hop.pipeline.transforms.insertupdate;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.StringUtil;

class DuplicateKeys {
  private static final String[] USE_SQL_STATE_DATABASES;
  private static final String[] SUCCESS_NO_INFO;
  private static final Map<String, String[]> ERROR_CODES_MAP;
  private static final Map<String, String> INDEX_MATCH_PATTERNS;
  private static final String DEFAULT_INDEX_MATCH_PATTERN;

  private final boolean noInfo;
  private final boolean useSqlState;
  private final String[] errorCodes;
  private final Pattern indexNamePattern;
  private Map<String, String[]> uniqIndexes;
  private Map<String, String[]> normalIndexes;
  private String primaryKey;
  private String[] ignoreIndexes;

  static {
    Properties props = new Properties();
    String path = DuplicateKeys.class.getPackageName().replace(".", "/") + "/";
    try (InputStream is =
        DuplicateKeys.class.getResourceAsStream(
            String.format("/%s/sql-err-codes.properties", path))) {
      props.load(is);
    } catch (IOException ignored) {
    }
    DEFAULT_INDEX_MATCH_PATTERN = props.getProperty("indexNameMatch");
    USE_SQL_STATE_DATABASES = props.getProperty("useSqlState.databases").split(",");
    SUCCESS_NO_INFO = props.getProperty("successNoInfo.databases").split(",");
    ERROR_CODES_MAP = convertByPrefix(props, "duplicateKeyCodes.", s -> s.split(","));
    INDEX_MATCH_PATTERNS = convertByPrefix(props, "indexNameMatch.", s -> s);
  }

  static DuplicateKeys build(String databaseType) {
    String indexMatchPattern = INDEX_MATCH_PATTERNS.get(databaseType);
    if (StringUtil.isEmpty(indexMatchPattern)) {
      indexMatchPattern = DEFAULT_INDEX_MATCH_PATTERN;
    }
    return new DuplicateKeys(
        Arrays.binarySearch(SUCCESS_NO_INFO, databaseType) >= 0,
        Arrays.binarySearch(USE_SQL_STATE_DATABASES, databaseType) >= 0,
        ERROR_CODES_MAP.get(databaseType),
        indexMatchPattern);
  }

  private DuplicateKeys(
      boolean noInfo, boolean useSqlState, String[] errorCodes, String indexNamePattern) {
    this.noInfo = noInfo;
    this.useSqlState = useSqlState;
    this.errorCodes = errorCodes;
    this.indexNamePattern = Pattern.compile(indexNamePattern);
  }

  void initIndexes(Map<String, String[]> indexColumns, String primaryKey) {
    this.uniqIndexes = indexColumns;
    this.primaryKey = primaryKey;
  }

  String[] checkIndexCols(String[] lookupKeys, String[] updateColumns) {
    if (support()) {
      Set<String> cols =
          uniqIndexes.values().stream().flatMap(Stream::of).collect(Collectors.toSet());
      cols.removeIf(
          k ->
              Arrays.binarySearch(updateColumns, k) >= 0
                  || Arrays.binarySearch(lookupKeys, k) >= 0);
      return cols.toArray(new String[0]);
    }
    return new String[0];
  }

  boolean support() {
    return uniqIndexes != null && !uniqIndexes.isEmpty();
  }

  boolean isDuplicateKeyError(SQLException sqlError) {
    String errorCode;
    if (useSqlState) {
      errorCode = sqlError.getSQLState();
    } else {
      SQLException current = sqlError;
      while (current.getErrorCode() == 0 && current.getCause() instanceof SQLException) {
        current = (SQLException) current.getCause();
      }
      errorCode = Integer.toString(current.getErrorCode());
    }
    return !StringUtil.isEmpty(errorCode) && Arrays.binarySearch(errorCodes, errorCode) >= 0;
  }

  String[] conflictColumns(SQLException e, StringBuilder pk) {
    String maybeIndex = matchIndexName(e);
    if (maybeIndex != null) {
      for (Map.Entry<String, String[]> entry : uniqIndexes.entrySet()) {
        if (maybeIndex.equalsIgnoreCase(entry.getKey())) {
          pk.append(entry.getKey());
          return entry.getValue();
        }
      }
      if (primaryKey != null && maybeIndex.equalsIgnoreCase("PRIMARY")) {
        pk.append(primaryKey);
        return uniqIndexes.get(primaryKey);
      }
    }
    return new String[0];
  }

  private String matchIndexName(SQLException e) {
    Set<SQLException> cache = new HashSet<>(4);
    while (e != null && e.getCause() instanceof SQLException) {
      Matcher matcher = indexNamePattern.matcher(Const.nullToEmpty(e.getMessage()).trim());
      if (matcher.find()) {
        return matcher.group("name");
      }
      cache.add(e);
      e = (SQLException) e.getCause();
      if (cache.contains(e)) {
        break;
      }
    }
    return null;
  }

  private static <T> Map<String, T> convertByPrefix(
      Properties props, String prefix, Function<String, T> to) {
    return props.stringPropertyNames().stream()
        .filter(k -> k.startsWith(prefix))
        .map(
            k ->
                new AbstractMap.SimpleEntry<>(
                    k.substring(prefix.length()), to.apply(props.getProperty(k))))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
