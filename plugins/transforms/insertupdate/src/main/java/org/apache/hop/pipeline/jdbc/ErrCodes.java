package org.apache.hop.pipeline.jdbc;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.StringUtil;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ErrCodes {
  private static final String[] USE_SQL_STATE_DATABASES;
  private static final String[] SUCCESS_NO_INFO;
  private static final Map<String, String[]> ERROR_CODES_MAP;
  private static final Map<String, String> INDEX_MATCH_PATTERNS;
  private static final String DEFAULT_INDEX_MATCH_PATTERN;

  private final boolean noInfo;
  private final boolean useSqlState;
  private final String[] errCodes;
  private final Pattern indexNamePattern;
  private Map<String, String[]> uniqIndexes;
  private Map<String, String[]> normalIndexes;
  private String primaryKey;
  private String[] ignoreIndexes;

  static {
    Properties props = new Properties();
    Class<?> type = ErrCodes.class;
    String path = type.getPackageName().replace(".", "/") + "/sql-codes.properties";
    try (InputStream is = type.getResourceAsStream(path)) {
      props.load(is);
    } catch (IOException ignored) {
    }
    DEFAULT_INDEX_MATCH_PATTERN = props.getProperty("SqlErrCodes.IndexNameMatch");
    USE_SQL_STATE_DATABASES = props.getProperty("SqlErrCodes.UseSqlState.Databases").split(",");
    SUCCESS_NO_INFO = props.getProperty("SqlErrCodes.SuccessNoInfo.Databases").split(",");
    ERROR_CODES_MAP = convertByPrefix(props, "SqlErrCodes.DuplicateKey.", s -> s.split(","));
    INDEX_MATCH_PATTERNS = convertByPrefix(props, "indexNameMatch.", s -> s);
  }

  static ErrCodes build(String databaseType) {
    String indexMatchPattern = INDEX_MATCH_PATTERNS.get(databaseType);
    if (StringUtil.isEmpty(indexMatchPattern)) {
      indexMatchPattern = DEFAULT_INDEX_MATCH_PATTERN;
    }
    return new ErrCodes(
        Arrays.binarySearch(SUCCESS_NO_INFO, databaseType) >= 0,
        Arrays.binarySearch(USE_SQL_STATE_DATABASES, databaseType) >= 0,
        ERROR_CODES_MAP.get(databaseType),
        indexMatchPattern);
  }

  private ErrCodes(
      boolean noInfo, boolean useSqlState, String[] errCodes, String indexNamePattern) {
    this.noInfo = noInfo;
    this.useSqlState = useSqlState;
    this.errCodes = errCodes;
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

  boolean isConflictKeyError(SQLException sqlError) {
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
    return !StringUtil.isEmpty(errorCode) && Arrays.binarySearch(errCodes, errorCode) >= 0;
  }

  public String[] conflictColumns(SQLException e, StringBuilder pk) {
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
