/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.insertupdate;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.jdbc.ErrCodes;

/** Stores data for the Insert/Update transform. */
@SuppressWarnings("java:S1104")
public class InsertUpdateData extends BaseTransformData implements ITransformData {
  public Database db;

  public int[] keynrs; // nr of keylookup -value in row...
  public int[] keynrs2; // nr of keylookup2-value in row...
  public int[] valuenrs; // Stream valuename nrs to prevent searches.

  public IRowMeta outputRowMeta;

  public String schemaTable;

  public PreparedStatement prepStatementLookup;
  public PreparedStatement prepStatementUpdate;

  public IRowMeta updateParameterRowMeta;
  public IRowMeta lookupParameterRowMeta;
  public IRowMeta lookupReturnRowMeta;
  public IRowMeta insertRowMeta;

  public ErrCodes errCodes;
  public JdbcStats.TableStats<?> tableStats;

  public StatementHandle updateHandle;
  public Map<String, StatementHandle> conflictKeyHandles;

  /** Default constructor. */
  public InsertUpdateData() {
    super();

    db = null;
  }

  static enum Bag {
    SINGLE,
    TEN,
    X100Hundred,
    Thousand,
    TenThousand,
  }

  static class StatementHandle implements AutoCloseable {
    private final PreparedStatement statement;
    private final IRowMeta parameterRowMeta;
    private final int[] valueNrs;

    StatementHandle(PreparedStatement statement, IRowMeta parameterRowMeta, int[] valueNrs) {
      this.statement = statement;
      this.parameterRowMeta = parameterRowMeta;
      this.valueNrs = valueNrs;
    }

    @Override
    public void close() throws SQLException {
      statement.close();
    }
  }
}
