package org.apache.hop.pipeline.jdbc;

import java.sql.Statement;

public abstract class Bags {

  static class ResultStats {
    private final int[] counts;
    private final int failedTimes;
    private final int unknownTimes;
    private final int affectedTimes;

    static ResultStats of(int[] counts) {
      int affected = 0;
      int failed = 0;
      int unknown = 0;
      for (int item : counts) {
        if (item >= 0) {
          affected++;
        } else if (item == Statement.EXECUTE_FAILED) {
          failed++;
        } else if (item == Statement.SUCCESS_NO_INFO) {
          unknown++;
        }
      }
      return new ResultStats(counts, failed, unknown, affected);
    }

    public ResultStats(int[] counts, int failedTimes, int unknownTimes, int affectedTimes) {
      this.counts = counts;
      this.failedTimes = failedTimes;
      this.unknownTimes = unknownTimes;
      this.affectedTimes = affectedTimes;
    }
  }
}
