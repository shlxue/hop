/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline.transforms.tableoutput;

import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.utils.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Node;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TableOutputMetaTest {

  private List<DatabaseMeta> databases;
  private IMetaStore metaStore;

  @SuppressWarnings( "unchecked" )
  @Before
  public void setUp() {
    databases = mock( List.class );
    metaStore = mock( IMetaStore.class );
  }

  @Test
  public void testIsReturningGeneratedKeys() throws Exception {
    TableOutputMeta tableOutputMeta = new TableOutputMeta(),
      tableOutputMetaSpy = spy( tableOutputMeta );

    DatabaseMeta databaseMeta = mock( DatabaseMeta.class );
    doReturn( true ).when( databaseMeta ).supportsAutoGeneratedKeys();
    doReturn( databaseMeta ).when( tableOutputMetaSpy ).getDatabaseMeta();

    tableOutputMetaSpy.setReturningGeneratedKeys( true );
    assertTrue( tableOutputMetaSpy.isReturningGeneratedKeys() );

    doReturn( false ).when( databaseMeta ).supportsAutoGeneratedKeys();
    assertFalse( tableOutputMetaSpy.isReturningGeneratedKeys() );

    tableOutputMetaSpy.setReturningGeneratedKeys( true );
    assertFalse( tableOutputMetaSpy.isReturningGeneratedKeys() );

    tableOutputMetaSpy.setReturningGeneratedKeys( false );
    assertFalse( tableOutputMetaSpy.isReturningGeneratedKeys() );
  }

  @Test
  public void testProvidesModeler() throws Exception {
    TableOutputMeta tableOutputMeta = new TableOutputMeta();
    tableOutputMeta.setFieldDatabase( new String[] { "f1", "f2", "f3" } );
    tableOutputMeta.setFieldStream( new String[] { "s4", "s5", "s6" } );

    TableOutputData tableOutputData = new TableOutputData();
    tableOutputData.insertRowMeta = mock( RowMeta.class );
    assertEquals( tableOutputData.insertRowMeta, tableOutputMeta.getRowMeta( tableOutputData ) );

    tableOutputMeta.setSpecifyFields( false );
    assertEquals( 0, tableOutputMeta.getDatabaseFields().size() );
    assertEquals( 0, tableOutputMeta.getStreamFields().size() );

    tableOutputMeta.setSpecifyFields( true );
    assertEquals( 3, tableOutputMeta.getDatabaseFields().size() );
    assertEquals( "f1", tableOutputMeta.getDatabaseFields().get( 0 ) );
    assertEquals( "f2", tableOutputMeta.getDatabaseFields().get( 1 ) );
    assertEquals( "f3", tableOutputMeta.getDatabaseFields().get( 2 ) );
    assertEquals( 3, tableOutputMeta.getStreamFields().size() );
    assertEquals( "s4", tableOutputMeta.getStreamFields().get( 0 ) );
    assertEquals( "s5", tableOutputMeta.getStreamFields().get( 1 ) );
    assertEquals( "s6", tableOutputMeta.getStreamFields().get( 2 ) );
  }

  @Test
  public void testLoadXml() throws Exception {

    TableOutputMeta tableOutputMeta = new TableOutputMeta();
    tableOutputMeta.loadXML( getTestNode(), metaStore );
    assertEquals( "1000", tableOutputMeta.getCommitSize() );
    assertEquals( null, tableOutputMeta.getGeneratedKeyField() );
    assertEquals( "public", tableOutputMeta.getSchemaName() );
    assertEquals( "sales_csv", tableOutputMeta.getTableName() );
    assertEquals( null, tableOutputMeta.getPartitioningField() );
    assertTrue( tableOutputMeta.truncateTable() );
    assertTrue( tableOutputMeta.specifyFields() );
    assertFalse( tableOutputMeta.ignoreErrors() );
    assertFalse( tableOutputMeta.isPartitioningEnabled() );
    assertTrue( tableOutputMeta.useBatchUpdate() );
    assertFalse( tableOutputMeta.isTableNameInField() );
    assertTrue( tableOutputMeta.isTableNameInTable() );
    assertFalse( tableOutputMeta.isReturningGeneratedKeys() );
    String expectedXml = ""
      + "    <connection/>\n"
      + "    <schema>public</schema>\n"
      + "    <table>sales_csv</table>\n"
      + "    <commit>1000</commit>\n"
      + "    <truncate>Y</truncate>\n"
      + "    <ignore_errors>N</ignore_errors>\n"
      + "    <use_batch>Y</use_batch>\n"
      + "    <specify_fields>Y</specify_fields>\n"
      + "    <partitioning_enabled>N</partitioning_enabled>\n"
      + "    <partitioning_field/>\n"
      + "    <partitioning_daily>N</partitioning_daily>\n"
      + "    <partitioning_monthly>Y</partitioning_monthly>\n"
      + "    <tablename_in_field>N</tablename_in_field>\n"
      + "    <tablename_field/>\n"
      + "    <tablename_in_table>Y</tablename_in_table>\n"
      + "    <return_keys>N</return_keys>\n"
      + "    <return_field/>\n"
      + "    <fields>\n"
      + "        <field>\n"
      + "          <column_name>ORDERNUMBER</column_name>\n"
      + "          <stream_name>ORDERNUMBER</stream_name>\n"
      + "        </field>\n"
      + "        <field>\n"
      + "          <column_name>QUANTITYORDERED</column_name>\n"
      + "          <stream_name>QUANTITYORDERED</stream_name>\n"
      + "        </field>\n"
      + "        <field>\n"
      + "          <column_name>PRICEEACH</column_name>\n"
      + "          <stream_name>PRICEEACH</stream_name>\n"
      + "        </field>\n"
      + "    </fields>\n";
    String actualXml = TestUtils.toUnixLineSeparators( tableOutputMeta.getXML() );
    assertEquals( expectedXml, actualXml );
  }

  @Test
  public void testSetupDefault() throws Exception {
    TableOutputMeta tableOutputMeta = new TableOutputMeta();
    tableOutputMeta.setDefault();
    assertEquals( "", tableOutputMeta.getTableName() );
    assertEquals( "1000", tableOutputMeta.getCommitSize() );
    assertFalse( tableOutputMeta.isPartitioningEnabled() );
    assertTrue( tableOutputMeta.isPartitioningMonthly() );
    assertEquals( "", tableOutputMeta.getPartitioningField() );
    assertTrue( tableOutputMeta.isTableNameInTable() );
    assertEquals( "", tableOutputMeta.getTableNameField() );
    assertFalse( tableOutputMeta.specifyFields() );
  }

  @Test
  public void testClone() throws Exception {
    TableOutputMeta tableOutputMeta = new TableOutputMeta();
    tableOutputMeta.setDefault();
    tableOutputMeta.setFieldStream( new String[] { "1", "2", "3" } );
    tableOutputMeta.setFieldDatabase( new String[] { "d1", "d2", "d3" } );
    TableOutputMeta clone = (TableOutputMeta) tableOutputMeta.clone();
    assertNotSame( clone, tableOutputMeta );
    assertEquals( clone.getXML(), tableOutputMeta.getXML() );
  }

  @Test
  public void testSupportsErrorHandling() throws Exception {
    TableOutputMeta tableOutputMeta = new TableOutputMeta();
    DatabaseMeta dbMeta = mock( DatabaseMeta.class );
    tableOutputMeta.setDatabaseMeta( dbMeta );
    IDatabase iDatabase = mock( IDatabase.class );
    when( dbMeta.getIDatabase() ).thenReturn( iDatabase );
    when( iDatabase.supportsErrorHandling() ).thenReturn( true, false );
    assertTrue( tableOutputMeta.supportsErrorHandling() );
    assertFalse( tableOutputMeta.supportsErrorHandling() );
    tableOutputMeta.setDatabaseMeta( null );
    assertTrue( tableOutputMeta.supportsErrorHandling() );
  }

  private Node getTestNode() throws HopXMLException {
    String xml =
      "  <transform>\n"
        + "    <name>Table output</name>\n"
        + "    <type>TableOutput</type>\n"
        + "    <description/>\n"
        + "    <distribute>Y</distribute>\n"
        + "    <custom_distribution/>\n"
        + "    <copies>1</copies>\n"
        + "         <partitioning>\n"
        + "           <method>none</method>\n"
        + "           <schema_name/>\n"
        + "           </partitioning>\n"
        + "    <connection>local postgres</connection>\n"
        + "    <schema>public</schema>\n"
        + "    <table>sales_csv</table>\n"
        + "    <commit>1000</commit>\n"
        + "    <truncate>Y</truncate>\n"
        + "    <ignore_errors>N</ignore_errors>\n"
        + "    <use_batch>Y</use_batch>\n"
        + "    <specify_fields>Y</specify_fields>\n"
        + "    <partitioning_enabled>N</partitioning_enabled>\n"
        + "    <partitioning_field/>\n"
        + "    <partitioning_daily>N</partitioning_daily>\n"
        + "    <partitioning_monthly>Y</partitioning_monthly>\n"
        + "    <tablename_in_field>N</tablename_in_field>\n"
        + "    <tablename_field/>\n"
        + "    <tablename_in_table>Y</tablename_in_table>\n"
        + "    <return_keys>N</return_keys>\n"
        + "    <return_field/>\n"
        + "    <fields>\n"
        + "        <field>\n"
        + "          <column_name>ORDERNUMBER</column_name>\n"
        + "          <stream_name>ORDERNUMBER</stream_name>\n"
        + "        </field>\n"
        + "        <field>\n"
        + "          <column_name>QUANTITYORDERED</column_name>\n"
        + "          <stream_name>QUANTITYORDERED</stream_name>\n"
        + "        </field>\n"
        + "        <field>\n"
        + "          <column_name>PRICEEACH</column_name>\n"
        + "          <stream_name>PRICEEACH</stream_name>\n"
        + "        </field>\n"
        + "    </fields>\n"
        + "     <cluster_schema/>\n"
        + "    <GUI>\n"
        + "      <xloc>368</xloc>\n"
        + "      <yloc>64</yloc>\n"
        + "      <draw>Y</draw>\n"
        + "      </GUI>\n"
        + "    </transform>\n";
    return XMLHandler.loadXMLString( xml, "transform" );
  }
}
