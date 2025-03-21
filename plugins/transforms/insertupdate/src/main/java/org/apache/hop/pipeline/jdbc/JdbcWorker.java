package org.apache.hop.pipeline.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

abstract class JdbcWorker<R> implements Callable<R> {
  protected final Connection connection;
  protected final StatementHandle handle;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  public JdbcWorker(Connection connection, StatementHandle handle) {
    this.connection = connection;
    this.handle = handle;
  }

  @Override
  public R call() throws SQLException {
    return null;
  }
}
