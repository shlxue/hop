/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.ui.workflow.actions.workflow;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.laf.BasePropertyHandler;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.ui.workflow.actions.pipeline.ActionBaseDialog;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IActionDialog;
import org.apache.hop.workflow.actions.workflow.ActionWorkflow;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.workflow.HopWorkflowFileType;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * This dialog allows you to edit the workflow action (ActionWorkflow)
 *
 * @author Matt
 * @since 19-06-2003
 */
public class ActionWorkflowDialog extends ActionBaseDialog implements IActionDialog {
  private static Class<?> PKG = ActionWorkflow.class; // for i18n purposes, needed by Translator!!

  protected ActionWorkflow action;

  protected Button wPassExport;

  protected Button wExpandRemote;

  private static final String[] FILE_FILTERLOGNAMES = new String[] {
    BaseMessages.getString( PKG, "ActionWorkflow.Fileformat.TXT" ),
    BaseMessages.getString( PKG, "ActionWorkflow.Fileformat.LOG" ),
    BaseMessages.getString( PKG, "ActionWorkflow.Fileformat.All" ) };

  public ActionWorkflowDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, action, workflowMeta );
    this.action = (ActionWorkflow) action;
  }

  public IAction open() {
    Shell parent = getParent();
    display = parent.getDisplay();

    shell = new Shell( parent, props.getWorkflowsDialogStyle() );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, action );

    backupChanged = action.hasChanged();

    createElements();

    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    setActive();

    BaseTransformDialog.setSize( shell );

    int width = 750;
    int height = Const.isWindows() ? 730 : 718;

    shell.setSize( width, height );
    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return action;
  }

  protected void createElements() {
    super.createElements();
    shell.setText( BaseMessages.getString( PKG, "ActionWorkflow.Header" ) );

    wlPath.setText( BaseMessages.getString( PKG, "ActionWorkflow.Filename.Label" ) );
    wPassParams.setText( BaseMessages.getString( PKG, "ActionWorkflow.PassAllParameters.Label" ) );

    // Start Server Section
    wPassExport = new Button( gExecution, SWT.CHECK );
    wPassExport.setText( BaseMessages.getString( PKG, "ActionWorkflow.PassExportToSlave.Label" ) );
    props.setLook( wPassExport );
    FormData fdPassExport = new FormData();
    fdPassExport.left = new FormAttachment( 0, 0 );
    fdPassExport.top = new FormAttachment( wEveryRow, 10 );
    fdPassExport.right = new FormAttachment( 100, 0 );
    wPassExport.setLayoutData( fdPassExport );

    wExpandRemote = new Button( gExecution, SWT.CHECK );
    wExpandRemote.setText( BaseMessages.getString( PKG, "ActionWorkflowDialog.ExpandRemoteOnSlave.Label" ) );
    props.setLook( wExpandRemote );
    FormData fdExpandRemote = new FormData();
    fdExpandRemote.top = new FormAttachment( wPassExport, 10 );
    fdExpandRemote.left = new FormAttachment( 0, 0 );
    wExpandRemote.setLayoutData( fdExpandRemote );

    wWaitingToFinish = new Button( gExecution, SWT.CHECK );
    props.setLook( wWaitingToFinish );
    wWaitingToFinish.setText( BaseMessages.getString( PKG, "ActionWorkflow.WaitToFinish.Label" ) );
    FormData fdWait = new FormData();
    fdWait.top = new FormAttachment( wExpandRemote, 10 );
    fdWait.left = new FormAttachment( 0, 0 );
    wWaitingToFinish.setLayoutData( fdWait );

    wFollowingAbortRemotely = new Button( gExecution, SWT.CHECK );
    props.setLook( wFollowingAbortRemotely );
    wFollowingAbortRemotely.setText( BaseMessages.getString( PKG, "ActionWorkflow.AbortRemote.Label" ) );
    FormData fdFollow = new FormData();
    fdFollow.top = new FormAttachment( wWaitingToFinish, 10 );
    fdFollow.left = new FormAttachment( 0, 0 );
    wFollowingAbortRemotely.setLayoutData( fdFollow );
    // End Server Section

    Composite cRunConfiguration = new Composite( wOptions, SWT.NONE );
    cRunConfiguration.setLayout( new FormLayout() );
    props.setLook( cRunConfiguration );
    FormData fdLocal = new FormData();
    fdLocal.top = new FormAttachment( 0 );
    fdLocal.right = new FormAttachment( 100 );
    fdLocal.left = new FormAttachment( 0 );

    wbGetParams.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        getParameters( null ); // force reload from file specification
      }
    } );

    wbBrowse.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        pickFileVFS();
      }
    } );

    wbLogFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        selectLogFile( FILE_FILTERLOGNAMES );
      }
    } );
  }

  protected ActionBase getAction() {
    return action;
  }

  protected Image getImage() {
    return SwtSvgImageUtil.getImage( shell.getDisplay(), getClass().getClassLoader(), BasePropertyHandler.getProperty( "Workflow_image" ), ConstUi.LARGE_ICON_SIZE, ConstUi.LARGE_ICON_SIZE );
  }

  protected String[] getParameters() {
    return action.parameters;
  }

  protected void getParameters( WorkflowMeta inputWorkflowMeta ) {
    try {
      if ( inputWorkflowMeta == null ) {
        ActionWorkflow jej = new ActionWorkflow();
        getInfo( jej );
        inputWorkflowMeta = jej.getWorkflowMeta( metaStore, workflowMeta );
      }
      String[] parameters = inputWorkflowMeta.listParameters();

      String[] existing = wParameters.getItems( 1 );

      for ( int i = 0; i < parameters.length; i++ ) {
        if ( Const.indexOfString( parameters[ i ], existing ) < 0 ) {
          TableItem item = new TableItem( wParameters.table, SWT.NONE );
          item.setText( 1, parameters[ i ] );
        }
      }
      wParameters.removeEmptyRows();
      wParameters.setRowNums();
      wParameters.optWidth( true );
    } catch ( Exception e ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "ActionWorkflowDialog.Exception.UnableToLoadJob.Title" ), BaseMessages
        .getString( PKG, "ActionWorkflowDialog.Exception.UnableToLoadJob.Message" ), e );
    }
  }

  protected void pickFileVFS() {

    HopWorkflowFileType<WorkflowMeta> jobFileType = HopDataOrchestrationPerspective.getInstance().getWorkflowFileType();

    FileDialog dialog = new FileDialog( shell, SWT.OPEN );
    dialog.setFilterExtensions( jobFileType.getFilterExtensions() );
    dialog.setFilterNames( jobFileType.getFilterNames() );
    String prevName = workflowMeta.environmentSubstitute( getPath() );
    String parentFolder = null;
    try {
      parentFolder =
        HopVfs.getFilename( HopVfs
          .getFileObject( workflowMeta.environmentSubstitute( workflowMeta.getFilename() ) ).getParent() );
    } catch ( Exception e ) {
      // not that important
    }
    if ( !Utils.isEmpty( prevName ) ) {
      try {
        if ( HopVfs.fileExists( prevName ) ) {
          dialog.setFilterPath( HopVfs.getFilename( HopVfs.getFileObject( prevName ).getParent() ) );
        } else {

          if ( !prevName.endsWith( ".hwf" ) ) {
            prevName = getEntryName( Const.trim( getPath() ) + ".hwf" );
          }
          if ( HopVfs.fileExists( prevName ) ) {
            wPath.setText( prevName );
            return;
          } else {
            // File specified doesn't exist. Ask if we should create the file...
            //
            MessageBox mb = new MessageBox( shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION );
            mb.setMessage( BaseMessages.getString( PKG, "ActionWorkflow.Dialog.CreateJobQuestion.Message" ) );
            mb.setText( BaseMessages.getString( PKG, "ActionWorkflow.Dialog.CreateJobQuestion.Title" ) ); // Sorry!
            int answer = mb.open();
            if ( answer == SWT.YES ) {

              HopGui hopGui = HopGui.getInstance();
              IHopFileTypeHandler typeHandler = HopDataOrchestrationPerspective.getInstance().getWorkflowFileType().newFile( hopGui, hopGui.getVariables() );
              typeHandler.setFilename( workflowMeta.environmentSubstitute( prevName ) );
              wPath.setText( prevName );
              hopGui.fileDelegate.fileSave();
              return;
            }
          }
        }
      } catch ( Exception e ) {
        dialog.setFilterPath( parentFolder );
      }
    } else if ( !Utils.isEmpty( parentFolder ) ) {
      dialog.setFilterPath( parentFolder );
    }

    String fname = dialog.open();
    if ( fname != null ) {
      File file = new File( fname );
      String name = file.getName();
      String parentFolderSelection = file.getParentFile().toString();

      if ( !Utils.isEmpty( parentFolder ) && parentFolder.equals( parentFolderSelection ) ) {
        wPath.setText( getEntryName( name ) );
      } else {
        wPath.setText( fname );
      }

    }
  }

  String getEntryName( String name ) {
    return "${"
      + Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY + "}/" + name;
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  public void setActive() {
    super.setActive();
  }

  public void getData() {
    wName.setText( Const.NVL( action.getName(), "" ) );
    wPath.setText( Const.NVL( action.getFilename(), "" ) );

    // Parameters
    if ( action.parameters != null ) {
      for ( int i = 0; i < action.parameters.length; i++ ) {
        TableItem ti = wParameters.table.getItem( i );
        if ( !Utils.isEmpty( action.parameters[ i ] ) ) {
          ti.setText( 1, Const.NVL( action.parameters[ i ], "" ) );
          ti.setText( 2, Const.NVL( action.parameterFieldNames[ i ], "" ) );
          ti.setText( 3, Const.NVL( action.parameterValues[ i ], "" ) );
        }
      }
      wParameters.setRowNums();
      wParameters.optWidth( true );
    }

    wPassParams.setSelection( action.isPassingAllParameters() );

    wPrevToParams.setSelection( action.paramsFromPrevious );
    wEveryRow.setSelection( action.execPerRow );
    wSetLogfile.setSelection( action.setLogfile );
    if ( action.logfile != null ) {
      wLogfile.setText( action.logfile );
    }
    if ( action.logext != null ) {
      wLogext.setText( action.logext );
    }
    wAddDate.setSelection( action.addDate );
    wAddTime.setSelection( action.addTime );
    wPassExport.setSelection( action.isPassingExport() );

    if ( action.logFileLevel != null ) {
      wLoglevel.select( action.logFileLevel.getLevel() );
    } else {
      // Set the default log level
      wLoglevel.select( ActionWorkflow.DEFAULT_LOG_LEVEL.getLevel() );
    }
    wAppendLogfile.setSelection( action.setAppendLogfile );
    wCreateParentFolder.setSelection( action.createParentFolder );
    wWaitingToFinish.setSelection( action.isWaitingToFinish() );
    wFollowingAbortRemotely.setSelection( action.isFollowingAbortRemotely() );
    wExpandRemote.setSelection( action.isExpandingRemoteWorkflow() );

    try {
      List<String> runConfigurations = WorkflowRunConfiguration.createFactory( metaStore).getElementNames();

      try {
        ExtensionPointHandler.callExtensionPoint( HopGui.getInstance().getLog(), HopExtensionPoint.HopUiRunConfiguration.id, new Object[] { runConfigurations, WorkflowMeta.XML_TAG } );
      } catch ( HopException e ) {
        // Ignore errors
      }

      wRunConfiguration.setItems(runConfigurations.toArray( new String[0] ));
      wRunConfiguration.setText( Const.NVL(action.getRunConfiguration(), "") );

      if ( Utils.isEmpty( action.getRunConfiguration() ) ) {
        wRunConfiguration.select( 0 );
      } else {
        wRunConfiguration.setText( action.getRunConfiguration() );
      }
    } catch(Exception e) {
      LogChannel.UI.logError( "Error getting workflow run configurations", e );
    }

    wName.selectAll();
    wName.setFocus();
  }

  protected void cancel() {
    action.setChanged( backupChanged );

    action = null;
    dispose();
  }

  @VisibleForTesting
  protected void getInfo( ActionWorkflow aw ) {
    String jobPath = getPath();
    aw.setName( getName() );
    aw.setFileName( jobPath );
    aw.setRunConfiguration( wRunConfiguration.getText() );

    // Do the parameters
    int nrItems = wParameters.nrNonEmpty();
    int nr = 0;
    for ( int i = 0; i < nrItems; i++ ) {
      String param = wParameters.getNonEmpty( i ).getText( 1 );
      if ( param != null && param.length() != 0 ) {
        nr++;
      }
    }
    aw.parameters = new String[ nr ];
    aw.parameterFieldNames = new String[ nr ];
    aw.parameterValues = new String[ nr ];
    nr = 0;
    for ( int i = 0; i < nrItems; i++ ) {
      String param = wParameters.getNonEmpty( i ).getText( 1 );
      String fieldName = wParameters.getNonEmpty( i ).getText( 2 );
      String value = wParameters.getNonEmpty( i ).getText( 3 );

      aw.parameters[ nr ] = param;

      if ( !Utils.isEmpty( Const.trim( fieldName ) ) ) {
        aw.parameterFieldNames[ nr ] = fieldName;
      } else {
        aw.parameterFieldNames[ nr ] = "";
      }

      if ( !Utils.isEmpty( Const.trim( value ) ) ) {
        aw.parameterValues[ nr ] = value;
      } else {
        aw.parameterValues[ nr ] = "";
      }

      nr++;
    }
    aw.setPassingAllParameters( wPassParams.getSelection() );

    aw.setLogfile = wSetLogfile.getSelection();
    aw.addDate = wAddDate.getSelection();
    aw.addTime = wAddTime.getSelection();
    aw.logfile = wLogfile.getText();
    aw.logext = wLogext.getText();
    if ( wLoglevel.getSelectionIndex() >= 0 ) {
      aw.logFileLevel = LogLevel.values()[ wLoglevel.getSelectionIndex() ];
    } else {
      aw.logFileLevel = LogLevel.BASIC;
    }
    aw.paramsFromPrevious = wPrevToParams.getSelection();
    aw.execPerRow = wEveryRow.getSelection();
    aw.setPassingExport( wPassExport.getSelection() );
    aw.setAppendLogfile = wAppendLogfile.getSelection();
    aw.setWaitingToFinish( wWaitingToFinish.getSelection() );
    aw.createParentFolder = wCreateParentFolder.getSelection();
    aw.setFollowingAbortRemotely( wFollowingAbortRemotely.getSelection() );
    aw.setExpandingRemoteWorkflow( wExpandRemote.getSelection() );
    aw.setRunConfiguration( wRunConfiguration.getText() );

    WorkflowExecutionConfiguration executionConfiguration = new WorkflowExecutionConfiguration();
    executionConfiguration.setRunConfiguration( aw.getRunConfiguration() );
    try {
      ExtensionPointHandler.callExtensionPoint( action.getLogChannel(), HopExtensionPoint.HopUiPipelineBeforeStart.id,
        new Object[] { executionConfiguration, workflowMeta, workflowMeta, null } );
    } catch ( HopException e ) {
      // Ignore errors
    }

    try {
      ExtensionPointHandler.callExtensionPoint( action.getLogChannel(), HopExtensionPoint.JobEntryPipelineSave.id,
        new Object[] { workflowMeta, aw.getRunConfiguration() } );
    } catch ( HopException e ) {
      // Ignore errors
    }

    if ( executionConfiguration.getRemoteServer() != null ) {
      aw.setRemoteSlaveServerName( executionConfiguration.getRemoteServer().getName() );
    }
  }

  public void ok() {
    if ( Utils.isEmpty( getName() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "System.TransformActionNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.ActionNameMissing.Msg" ) );
      mb.open();
      return;
    }
    getInfo( action );
    action.setChanged();
    dispose();
  }

  @VisibleForTesting
  protected String getName() {
    return wName.getText();
  }

  @VisibleForTesting
  protected String getPath() {
    return wPath.getText();
  }

}
