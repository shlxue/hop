/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.textfileoutput;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.compress.CompressionOutputStream;
import org.apache.hop.core.compress.CompressionPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.hop.utils.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * User: Dzmitry Stsiapanau Date: 10/18/13 Time: 2:23 PM
 */
public class TextFileOutputTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private static final String EMPTY_FILE_NAME = "Empty File";
  private static final String EMPTY_STRING = "";
  private static final Boolean[] BOOL_VALUE_LIST = new Boolean[] { false, true };
  private static final String TEXT_FILE_OUTPUT_PREFIX = "textFileOutput";
  private static final String TEXT_FILE_OUTPUT_EXTENSION = ".txt";
  private static final String END_LINE = " endLine ";
  private static final String RESULT_ROWS = "\"some data\" \"another data\"\n" + "\"some data2\" \"another data2\"\n";
  private static final String TEST_PREVIOUS_DATA = "testPreviousData\n";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    PluginRegistry.addPluginType( CompressionPluginType.getInstance() );
    PluginRegistry.init( false );
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    FileUtils.deleteQuietly( Paths.get( TEXT_FILE_OUTPUT_PREFIX + TEXT_FILE_OUTPUT_EXTENSION ).toFile() );
  }

  public class TextFileOutputTestHandler extends TextFileOutput {
    public List<Throwable> errors = new ArrayList<>();
    private Object[] row;

    TextFileOutputTestHandler( TransformMeta transformMeta, ITransformData iTransformData, int copyNr,
                               PipelineMeta pipelineMeta, Pipeline pipeline ) {
      super( transformMeta, iTransformData, copyNr, pipelineMeta, pipeline );
    }

    public void setRow( Object[] row ) {
      this.row = row;
    }

    @Override public String buildFilename( String filename, boolean ziparchive ) {
      return filename;
    }

    @Override
    public Object[] getRow() throws HopException {
      return row;
    }

    @Override
    public void putRow( IRowMeta rowMeta, Object[] row ) throws HopTransformException {

    }

    @Override
    public void logError( String message ) {
      errors.add( new HopException( message ) );
    }

    @Override
    public void logError( String message, Throwable thr ) {
      errors.add( thr );
    }

    @Override
    public void logError( String message, Object... arguments ) {
      errors.add( new HopException( message ) );
    }
  }

  private TransformMockHelper<TextFileOutputMeta, TextFileOutputData> transformMockHelper;
  private TextFileField textFileField =
    new TextFileField( "Name", 2, EMPTY_STRING, 10, 20, EMPTY_STRING, EMPTY_STRING, EMPTY_STRING, EMPTY_STRING );
  private TextFileField textFileField2 =
    new TextFileField( "Surname", 2, EMPTY_STRING, 10, 20, EMPTY_STRING, EMPTY_STRING, EMPTY_STRING, EMPTY_STRING );
  private TextFileField[] textFileFields = new TextFileField[] { textFileField, textFileField2 };
  private Object[] row = new Object[] { "some data", "another data" };
  private Object[] row2 = new Object[] { "some data2", "another data2" };
  private List<Object[]> emptyRows = new ArrayList<>();
  private List<Object[]> rows = new ArrayList<>();
  private List<String> contents = new ArrayList<>();
  private TextFileOutput textFileOutput;

  {
    rows.add( row );
    rows.add( row2 );

    contents.add( EMPTY_STRING );
    contents.add( EMPTY_STRING );
    contents.add( END_LINE );
    contents.add( END_LINE );
    contents.add( null );
    contents.add( null );
    contents.add( END_LINE );
    contents.add( END_LINE );
    contents.add( RESULT_ROWS );
    contents.add( RESULT_ROWS );
    contents.add( RESULT_ROWS + END_LINE );
    contents.add( RESULT_ROWS + END_LINE );
    contents.add( RESULT_ROWS );
    contents.add( RESULT_ROWS );
    contents.add( RESULT_ROWS + END_LINE );
    contents.add( RESULT_ROWS + END_LINE );
    contents.add( EMPTY_STRING );
    contents.add( TEST_PREVIOUS_DATA );
    contents.add( END_LINE );
    contents.add( TEST_PREVIOUS_DATA + END_LINE );
    contents.add( TEST_PREVIOUS_DATA );
    contents.add( TEST_PREVIOUS_DATA );
    contents.add( END_LINE );
    contents.add( TEST_PREVIOUS_DATA + END_LINE );
    contents.add( RESULT_ROWS );
    contents.add( TEST_PREVIOUS_DATA + RESULT_ROWS );
    contents.add( RESULT_ROWS + END_LINE );
    contents.add( TEST_PREVIOUS_DATA + RESULT_ROWS + END_LINE );
    contents.add( RESULT_ROWS );
    contents.add( TEST_PREVIOUS_DATA + RESULT_ROWS );
    contents.add( RESULT_ROWS + END_LINE );
    contents.add( TEST_PREVIOUS_DATA + RESULT_ROWS + END_LINE );
  }

  @Before
  public void setUp() throws Exception {
    transformMockHelper =
      new TransformMockHelper<>( "TEXT FILE OUTPUT TEST", TextFileOutputMeta.class, TextFileOutputData.class );
    Mockito.when( transformMockHelper.logChannelFactory.create( Mockito.any(), Mockito.any( ILoggingObject.class ) ) ).thenReturn(
      transformMockHelper.logChannelInterface );
    Mockito.verify( transformMockHelper.logChannelInterface, Mockito.never() ).logError( Mockito.anyString() );
    Mockito.verify( transformMockHelper.logChannelInterface, Mockito.never() ).logError( Mockito.anyString(), Mockito.any( Object[].class ) );
    Mockito.verify( transformMockHelper.logChannelInterface, Mockito.never() ).logError( Mockito.anyString(), Mockito.any( Throwable.class ) );
    Mockito.when( transformMockHelper.pipeline.isRunning() ).thenReturn( true );
    Mockito.verify( transformMockHelper.pipeline, Mockito.never() ).stopAll();
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.getSeparator() ).thenReturn( " " );
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.getEnclosure() ).thenReturn( "\"" );
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.getNewline() ).thenReturn( "\n" );
    Mockito.when( transformMockHelper.pipelineMeta.listVariables() ).thenReturn( new String[ 0 ] );
  }

  @After
  public void tearDown() throws Exception {
    transformMockHelper.cleanUp();
  }

  @Test
  public void testCloseFileDataOutIsNullCase() {
    textFileOutput =
      new TextFileOutput( transformMockHelper.transformMeta, transformMockHelper.iTransformData, 0, transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline );
    textFileOutput.data = Mockito.mock( TextFileOutputData.class );

    Assert.assertNull( textFileOutput.data.out );
    textFileOutput.closeFile();
  }

  @Test
  public void testCloseFileDataOutIsNotNullCase() {
    textFileOutput =
      new TextFileOutput( transformMockHelper.transformMeta, transformMockHelper.iTransformData, 0, transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline );
    textFileOutput.data = Mockito.mock( TextFileOutputData.class );
    textFileOutput.data.out = Mockito.mock( CompressionOutputStream.class );

    textFileOutput.closeFile();
    Assert.assertNull( textFileOutput.data.out );
  }

  private FileObject createTemplateFile() {
    String path =
      TestUtils.createRamFile( getClass().getSimpleName() + "/" + TEXT_FILE_OUTPUT_PREFIX + new Random().nextLong()
        + TEXT_FILE_OUTPUT_EXTENSION, transformMockHelper.pipelineMeta );
    return TestUtils.getFileObject( path, transformMockHelper.pipelineMeta );
  }

  private FileObject createTemplateFile( String content ) throws IOException {
    FileObject f2 = createTemplateFile();
    if ( content == null ) {
      f2.delete();
    } else {
      try ( OutputStreamWriter fw = new OutputStreamWriter( f2.getContent().getOutputStream() ) ) {
        fw.write( content );
      }
    }
    return f2;
  }

  @Test
  public void testsIterate() {
    FileObject resultFile = null;
    FileObject contentFile;
    String content = null;
    int i = 0;
    for ( Boolean fileExists : BOOL_VALUE_LIST ) {
      for ( Boolean dataReceived : BOOL_VALUE_LIST ) {
        for ( Boolean isDoNotOpenNewFileInit : BOOL_VALUE_LIST ) {
          for ( Boolean endLineExists : BOOL_VALUE_LIST ) {
            for ( Boolean append : BOOL_VALUE_LIST ) {
              try {
                resultFile = helpTestInit( fileExists, dataReceived, isDoNotOpenNewFileInit, endLineExists, append );
                content = (String) contents.toArray()[ i++ ];
                contentFile = createTemplateFile( content );
                if ( resultFile.exists() ) {
                  Assert.assertTrue( IOUtils.contentEquals( resultFile.getContent().getInputStream(), contentFile.getContent()
                    .getInputStream() ) );
                } else {
                  Assert.assertFalse( contentFile.exists() );
                }
              } catch ( Exception e ) {
                Assert.fail( e.getMessage() + "\n FileExists = " + fileExists + "\n DataReceived = " + dataReceived
                  + "\n isDoNotOpenNewFileInit = " + isDoNotOpenNewFileInit + "\n EndLineExists = " + endLineExists
                  + "\n Append = " + append + "\n Content = " + content + "\n resultFile = " + resultFile );
              }
            }
          }
        }
      }
    }
  }

  /**
   * Tests the RULE#1: If 'Do not create file at start' checkbox is cheked AND 'Add landing line of file' is NOT set AND
   * pipeline does not pass any rows to the file input transform, then NO output file should be created.
   */
  @Test
  public void testNoOpenFileCall_IfRule_1() throws Exception {

    TextFileField tfFieldMock = Mockito.mock( TextFileField.class );
    TextFileField[] textFileFields = { tfFieldMock };

    Mockito.when( transformMockHelper.initTransformMetaInterface.getEndedLine() ).thenReturn( EMPTY_STRING );
    Mockito.when( transformMockHelper.initTransformMetaInterface.getOutputFields() ).thenReturn( textFileFields );
    Mockito.when( transformMockHelper.initTransformMetaInterface.isDoNotOpenNewFileInit() ).thenReturn( true );

    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.getEndedLine() ).thenReturn( EMPTY_STRING );
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.getFileName() ).thenReturn( EMPTY_FILE_NAME );
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.isDoNotOpenNewFileInit() ).thenReturn( true );
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.getOutputFields() ).thenReturn( textFileFields );

    textFileOutput =
      new TextFileOutput( transformMockHelper.transformMeta, transformMockHelper.iTransformData, 0, transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline );
    TextFileOutput textFileOutputSpy = Mockito.spy( textFileOutput );
    Mockito.doReturn( false ).when( textFileOutputSpy ).isWriteHeader( TEXT_FILE_OUTPUT_PREFIX + TEXT_FILE_OUTPUT_EXTENSION );
    Mockito.doCallRealMethod().when( textFileOutputSpy ).initFileStreamWriter( EMPTY_FILE_NAME );
    Mockito.doNothing().when( textFileOutputSpy ).flushOpenFiles( true );

    textFileOutputSpy.init( transformMockHelper.initTransformMetaInterface, transformMockHelper.initTransformDataInterface );

    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.buildFilename( Mockito.anyString(), Mockito.anyString(),
      Mockito.any( IVariables.class ), Mockito.anyInt(), Mockito.anyString(), Mockito.anyInt(), Mockito.anyBoolean(),
      Mockito.any( TextFileOutputMeta.class ) ) ).
      thenReturn( TEXT_FILE_OUTPUT_PREFIX + TEXT_FILE_OUTPUT_EXTENSION );

    textFileOutputSpy.processRow( transformMockHelper.processRowsTransformMetaInterface, transformMockHelper.initTransformDataInterface );
    Mockito.verify( textFileOutputSpy, Mockito.never() ).initFileStreamWriter( EMPTY_FILE_NAME );
    Mockito.verify( textFileOutputSpy, Mockito.never() ).writeEndedLine();
    Mockito.verify( textFileOutputSpy ).setOutputDone();
  }

  private FileObject helpTestInit( Boolean fileExists, Boolean dataReceived, Boolean isDoNotOpenNewFileInit,
                                   Boolean endLineExists, Boolean append ) throws Exception {
    FileObject f;
    String endLine = null;
    List<Object[]> rows;

    if ( fileExists ) {
      f = createTemplateFile( TEST_PREVIOUS_DATA );
    } else {
      f = createTemplateFile( null );
    }

    if ( dataReceived ) {
      rows = this.rows;
    } else {
      rows = this.emptyRows;
    }

    if ( endLineExists ) {
      endLine = END_LINE;
    }

    List<Throwable> errors =
      doOutput( textFileFields, rows, f.getName().getURI(), endLine, false, isDoNotOpenNewFileInit, append );
    if ( !errors.isEmpty() ) {
      StringBuilder str = new StringBuilder();
      for ( Throwable thr : errors ) {
        str.append( thr );
      }
      Assert.fail( str.toString() );
    }

    return f;

  }

  private List<Throwable> doOutput( TextFileField[] textFileField, List<Object[]> rows, String pathToFile,
                                    String endedLine, Boolean isHeaderEnabled, Boolean isDoNotOpenNewFileInit, Boolean append )
    throws HopException {
    TextFileOutputData textFileOutputData = new TextFileOutputData();
    TextFileOutputTestHandler textFileOutput =
      new TextFileOutputTestHandler( transformMockHelper.transformMeta, textFileOutputData, 0, transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline );

    // init transform meta and process transform meta should be the same in this case
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.isDoNotOpenNewFileInit() ).thenReturn( isDoNotOpenNewFileInit );
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.isFileAppended() ).thenReturn( append );

    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.isHeaderEnabled() ).thenReturn( isHeaderEnabled );
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.getFileName() ).thenReturn( pathToFile );
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.buildFilename( Mockito.anyString(), Mockito.anyString(),
      Mockito.any( IVariables.class ), Mockito.anyInt(), Mockito.anyString(), Mockito.anyInt(), Mockito.anyBoolean(),
      Mockito.any( TextFileOutputMeta.class ) ) ).thenReturn( pathToFile );

    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.getOutputFields() ).thenReturn( textFileField );

    textFileOutput.init( transformMockHelper.processRowsTransformMetaInterface, textFileOutputData );

    // Process rows

    IRowSet rowSet = transformMockHelper.getMockInputRowSet( rows );
    IRowMeta inputRowMeta = Mockito.mock( IRowMeta.class );
    textFileOutput.setInputRowMeta( inputRowMeta );

    Mockito.when( rowSet.getRowWait( Mockito.anyInt(), Mockito.any( TimeUnit.class ) ) )
      .thenReturn( rows.isEmpty() ? null : rows.iterator().next() );
    Mockito.when( rowSet.getRowMeta() ).thenReturn( inputRowMeta );
    Mockito.when( inputRowMeta.clone() ).thenReturn( inputRowMeta );

    for ( int i = 0; i < textFileField.length; i++ ) {
      String name = textFileField[ i ].getName();
      ValueMetaString valueMetaString = new ValueMetaString( name );
      Mockito.when( inputRowMeta.getValueMeta( i ) ).thenReturn( valueMetaString );
      Mockito.when( inputRowMeta.indexOfValue( name ) ).thenReturn( i );
    }

    textFileOutput.addRowSetToInputRowSets( rowSet );
    textFileOutput.addRowSetToOutputRowSets( rowSet );

    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.getEndedLine() ).thenReturn( endedLine );
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.isFastDump() ).thenReturn( true );

    for ( int i = 0; i < rows.size(); i++ ) {
      textFileOutput.setRow( rows.get( i ) );
      textFileOutput.processRow( transformMockHelper.processRowsTransformMetaInterface, textFileOutputData );
    }
    textFileOutput.setRow( null );
    textFileOutput.processRow( transformMockHelper.processRowsTransformMetaInterface, textFileOutputData );
    textFileOutput.dispose( transformMockHelper.processRowsTransformMetaInterface, textFileOutputData );
    return textFileOutput.errors;
  }

  @Test
  public void containsSeparatorOrEnclosureIsNotUnnecessaryInvoked_SomeFieldsFromMeta() {
    TextFileField field = new TextFileField();
    field.setName( "name" );
    assertNotInvokedTwice( field );
  }

  @Test
  public void containsSeparatorOrEnclosureIsNotUnnecessaryInvoked_AllFieldsFromMeta() {
    assertNotInvokedTwice( null );
  }

  @Test
  public void testEndedLineVar() throws Exception {
    TextFileOutputData data = new TextFileOutputData();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    data.writer = baos;
    TextFileOutputMeta meta = new TextFileOutputMeta();
    meta.setEndedLine( "${endvar}" );
    meta.setDefault();
    meta.setEncoding( "UTF-8" );
    transformMockHelper.transformMeta.setTransformMetaInterface( meta );
    TextFileOutput textFileOutput =
      new TextFileOutputTestHandler( transformMockHelper.transformMeta, data, 0, transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline );
    textFileOutput.meta = meta;
    textFileOutput.data = data;
    textFileOutput.setVariable( "endvar", "this is the end" );
    textFileOutput.writeEndedLine();
    assertEquals( "this is the end", baos.toString( "UTF-8" ) );
  }

  private void assertNotInvokedTwice( TextFileField field ) {
    TextFileOutput transform =
      new TextFileOutput( transformMockHelper.transformMeta, transformMockHelper.iTransformData, 1, transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline );

    TextFileOutputMeta meta = new TextFileOutputMeta();
    meta.setEnclosureForced( false );
    meta.setEnclosureFixDisabled( false );
    transform.meta = meta;

    TextFileOutputData data = new TextFileOutputData();
    data.binarySeparator = " ".getBytes();
    data.binaryEnclosure = "\"".getBytes();
    data.binaryNewline = "\n".getBytes();
    transform.data = data;

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "name" ) );
    data.outputRowMeta = rowMeta;

    data.writer = new ByteArrayOutputStream();

    if ( field != null ) {
      meta.setOutputFields( new TextFileField[] { field } );
    }

    transform = Mockito.spy( transform );
    transform.writeHeader();
    Mockito.verify( transform ).containsSeparatorOrEnclosure( Mockito.any( byte[].class ), Mockito.any( byte[].class ),
      Mockito.any( byte[].class ) );
  }

  /**
   * PDI-15650
   * File Exists=N Flag Set=N Add Header=Y Append=Y
   * Result = File is created, header is written at top of file (this changed by the fix)
   */
  @Test
  public void testProcessRule_2() throws Exception {

    TextFileField tfFieldMock = Mockito.mock( TextFileField.class );
    TextFileField[] textFileFields = { tfFieldMock };

    Mockito.when( transformMockHelper.initTransformMetaInterface.getEndedLine() ).thenReturn( EMPTY_STRING );
    Mockito.when( transformMockHelper.initTransformMetaInterface.getOutputFields() ).thenReturn( textFileFields );
    Mockito.when( transformMockHelper.initTransformMetaInterface.isDoNotOpenNewFileInit() ).thenReturn( true );

    Mockito.when( transformMockHelper.initTransformDataInterface.getFileStreamsCollection() ).thenCallRealMethod();

    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.getEndedLine() ).thenReturn( EMPTY_STRING );
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.getFileName() ).thenReturn( TEXT_FILE_OUTPUT_PREFIX + TEXT_FILE_OUTPUT_EXTENSION );
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.isFileAppended() ).thenReturn( true );
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.isHeaderEnabled() ).thenReturn( true );
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.getOutputFields() ).thenReturn( textFileFields );
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.isDoNotOpenNewFileInit() ).thenReturn( true );
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.isFileNameInField() ).thenReturn( false );
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.isAddToResultFiles() ).thenReturn( true );

    Object[] rowData = new Object[] { "data text" };
    textFileOutput =
      new TextFileOutputTestHandler( transformMockHelper.transformMeta, transformMockHelper.iTransformData, 0, transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline );
    ( (TextFileOutputTestHandler) textFileOutput ).setRow( rowData );
    IRowMeta inputRowMeta = Mockito.mock( IRowMeta.class );

    IValueMeta iValueMeta = Mockito.mock( IValueMeta.class );
    Mockito.when( iValueMeta.getString( Mockito.anyObject() ) ).thenReturn( TEXT_FILE_OUTPUT_PREFIX + TEXT_FILE_OUTPUT_EXTENSION );
    Mockito.when( inputRowMeta.getValueMeta( Mockito.anyInt() ) ).thenReturn( iValueMeta );
    Mockito.when( inputRowMeta.clone() ).thenReturn( inputRowMeta );

    textFileOutput.setInputRowMeta( inputRowMeta );

    TextFileOutput textFileOutputSpy = Mockito.spy( textFileOutput );
    Mockito.doCallRealMethod().when( textFileOutputSpy ).initFileStreamWriter( TEXT_FILE_OUTPUT_PREFIX + TEXT_FILE_OUTPUT_EXTENSION );
    Mockito.doNothing().when( textFileOutputSpy ).writeRow( inputRowMeta, rowData );
    Mockito.doReturn( false ).when( textFileOutputSpy ).isFileExists( TEXT_FILE_OUTPUT_PREFIX + TEXT_FILE_OUTPUT_EXTENSION );
    Mockito.doReturn( true ).when( textFileOutputSpy ).isWriteHeader( TEXT_FILE_OUTPUT_PREFIX + TEXT_FILE_OUTPUT_EXTENSION );
    textFileOutputSpy.init( transformMockHelper.processRowsTransformMetaInterface, transformMockHelper.initTransformDataInterface );
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.buildFilename( TEXT_FILE_OUTPUT_PREFIX + TEXT_FILE_OUTPUT_EXTENSION, null,
      textFileOutputSpy, 0, null, 0, true, transformMockHelper.processRowsTransformMetaInterface ) ).
      thenReturn( TEXT_FILE_OUTPUT_PREFIX + TEXT_FILE_OUTPUT_EXTENSION );

    textFileOutputSpy.processRow( transformMockHelper.processRowsTransformMetaInterface, transformMockHelper.initTransformDataInterface );
    Mockito.verify( textFileOutputSpy, Mockito.times( 1 ) ).writeHeader();
    assertNotNull( textFileOutputSpy.getResultFiles() );
    assertEquals( 1, textFileOutputSpy.getResultFiles().size() );
  }

  /**
   * PDI-15650
   * File Exists=N Flag Set=N Add Header=Y Append=Y
   * Result = File is created, header is written at top of file (this changed by the fix)
   * with file name in stream
   */
  @Test
  public void testProcessRule_2FileNameInField() throws Exception {

    TextFileField tfFieldMock = Mockito.mock( TextFileField.class );
    TextFileField[] textFileFields = { tfFieldMock };

    Mockito.when( transformMockHelper.initTransformMetaInterface.getEndedLine() ).thenReturn( EMPTY_STRING );
    Mockito.when( transformMockHelper.initTransformMetaInterface.getOutputFields() ).thenReturn( textFileFields );
    Mockito.when( transformMockHelper.initTransformMetaInterface.isDoNotOpenNewFileInit() ).thenReturn( true );

    Mockito.when( transformMockHelper.initTransformDataInterface.getFileStreamsCollection() ).thenCallRealMethod();

    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.getEndedLine() ).thenReturn( EMPTY_STRING );
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.getFileName() ).thenReturn( TEXT_FILE_OUTPUT_PREFIX + TEXT_FILE_OUTPUT_EXTENSION );
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.isFileAppended() ).thenReturn( true );
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.isHeaderEnabled() ).thenReturn( true );
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.getOutputFields() ).thenReturn( textFileFields );
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.isDoNotOpenNewFileInit() ).thenReturn( true );
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.isAddToResultFiles() ).thenReturn( true );
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.isFileNameInField() ).thenReturn( true );

    Object[] rowData = new Object[] { "data text" };
    textFileOutput =
      new TextFileOutputTestHandler( transformMockHelper.transformMeta, transformMockHelper.iTransformData, 0, transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline );
    ( (TextFileOutputTestHandler) textFileOutput ).setRow( rowData );
    IRowMeta inputRowMeta = Mockito.mock( IRowMeta.class );

    IValueMeta iValueMeta = Mockito.mock( IValueMeta.class );
    Mockito.when( iValueMeta.getString( Mockito.anyObject() ) ).thenReturn( TEXT_FILE_OUTPUT_PREFIX + TEXT_FILE_OUTPUT_EXTENSION );
    Mockito.when( inputRowMeta.getValueMeta( Mockito.anyInt() ) ).thenReturn( iValueMeta );
    Mockito.when( inputRowMeta.clone() ).thenReturn( inputRowMeta );

    textFileOutput.setInputRowMeta( inputRowMeta );

    TextFileOutput textFileOutputSpy = Mockito.spy( textFileOutput );
    Mockito.doCallRealMethod().when( textFileOutputSpy ).initFileStreamWriter( TEXT_FILE_OUTPUT_PREFIX + TEXT_FILE_OUTPUT_EXTENSION );
    Mockito.doReturn( false ).when( textFileOutputSpy ).isFileExists( TEXT_FILE_OUTPUT_PREFIX + TEXT_FILE_OUTPUT_EXTENSION );
    Mockito.doReturn( true ).when( textFileOutputSpy ).isWriteHeader( TEXT_FILE_OUTPUT_PREFIX + TEXT_FILE_OUTPUT_EXTENSION );
    Mockito.doNothing().when( textFileOutputSpy ).writeRow( inputRowMeta, rowData );
    textFileOutputSpy.init( transformMockHelper.processRowsTransformMetaInterface, transformMockHelper.initTransformDataInterface );
    Mockito.when( transformMockHelper.processRowsTransformMetaInterface.buildFilename( TEXT_FILE_OUTPUT_PREFIX + TEXT_FILE_OUTPUT_EXTENSION, null,
      textFileOutputSpy, 0, null, 0, true, transformMockHelper.processRowsTransformMetaInterface ) ).
      thenReturn( TEXT_FILE_OUTPUT_PREFIX + TEXT_FILE_OUTPUT_EXTENSION );

    textFileOutputSpy.processRow( transformMockHelper.processRowsTransformMetaInterface, transformMockHelper.initTransformDataInterface );
    Mockito.verify( textFileOutputSpy, Mockito.times( 1 ) ).writeHeader();
    assertNotNull( textFileOutputSpy.getResultFiles() );
    assertEquals( 1, textFileOutputSpy.getResultFiles().size() );
  }

  /**
   * Test for PDI-13987
   */
  @Test
  public void testFastDumpDisableStreamEncodeTest() throws Exception {

    textFileOutput =
      new TextFileOutputTestHandler( transformMockHelper.transformMeta, transformMockHelper.iTransformData, 0, transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline );
    textFileOutput.meta = transformMockHelper.processRowsTransformMetaInterface;

    String testString = "ÖÜä";
    String inputEncode = "UTF-8";
    String outputEncode = "Windows-1252";
    Object[] rows = { testString.getBytes( inputEncode ) };

    ValueMetaBase iValueMeta = new ValueMetaBase( "test", IValueMeta.TYPE_STRING );
    iValueMeta.setStringEncoding( inputEncode );
    iValueMeta.setStorageType( IValueMeta.STORAGE_TYPE_BINARY_STRING );
    iValueMeta.setStorageMetadata( new ValueMetaString() );

    TextFileOutputData data = new TextFileOutputData();
    data.binarySeparator = " ".getBytes();
    data.binaryEnclosure = "\"".getBytes();
    data.binaryNewline = "\n".getBytes();
    textFileOutput.data = data;

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( iValueMeta );

    Mockito.doReturn( outputEncode ).when( transformMockHelper.processRowsTransformMetaInterface ).getEncoding();
    textFileOutput.data.writer = Mockito.mock( BufferedOutputStream.class );

    textFileOutput.writeRow( rowMeta, rows );
    Mockito.verify( textFileOutput.data.writer ).write( testString.getBytes( outputEncode ) );
  }
}
