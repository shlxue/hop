package org.apache.hop.pipeline.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Wrapper;

public interface JdbcRunner<T extends Wrapper, U, R> {
  R run(T target, U u) throws SQLException;

  default ResultSet query(Statement statement, StatementHandle handle) throws SQLException {
    return statement.executeQuery(handle.toString());
  }

  default R update(Statement statement, StatementHandle handle) throws SQLException {
    return
  }
}
