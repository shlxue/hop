/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.workflow.actions.movefiles;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/** This dialog allows you to edit the Move Files action settings. */
public class ActionMoveFilesDialog extends ActionDialog {
  private static final Class<?> PKG = ActionMoveFiles.class;

  private static final String[] FILETYPES =
      new String[] {BaseMessages.getString(PKG, "System.FileType.AllFiles")};
  public static final String CONST_OVERWRITE_FILE = "overwrite_file";
  public static final String CONST_UNIQUE_NAME = "unique_name";

  private Text wName;

  private Label wlMoveEmptyFolders;
  private Button wMoveEmptyFolders;

  private Button wIncludeSubfolders;

  private ActionMoveFiles action;

  private boolean changed;

  private Button wPrevious;

  private Label wlFields;

  private TableView wFields;

  private Label wlCreateMoveToFolder;
  private Button wCreateMoveToFolder;

  // Add File to result
  private Button wAddFileToResult;

  private Button wCreateDestinationFolder;

  private Button wDestinationIsAFile;

  private CCombo wSuccessCondition;

  private Label wlNrErrorsLessThan;
  private TextVar wNrErrorsLessThan;

  private Label wlAddDate;
  private Button wAddDate;

  private Label wlAddTime;
  private Button wAddTime;

  private Button wSpecifyFormat;

  private Label wlDateTimeFormat;
  private CCombo wDateTimeFormat;

  private Label wlMovedDateTimeFormat;
  private CCombo wMovedDateTimeFormat;

  private Label wlAddDateBeforeExtension;
  private Button wAddDateBeforeExtension;

  private Label wlAddMovedDateBeforeExtension;
  private Button wAddMovedDateBeforeExtension;

  private Label wlDoNotKeepFolderStructure;
  private Button wDoNotKeepFolderStructure;

  private CCombo wIfFileExists;

  private Label wlIfMovedFileExists;
  private CCombo wIfMovedFileExists;

  private Button wbDestinationFolder;
  private Label wlDestinationFolder;
  private TextVar wDestinationFolder;

  private Label wlAddMovedDate;
  private Button wAddMovedDate;

  private Label wlAddMovedTime;
  private Button wAddMovedTime;

  private Label wlSpecifyMoveFormat;
  private Button wSpecifyMoveFormat;

  private Button wSimulate;

  public ActionMoveFilesDialog(
      Shell parent, ActionMoveFiles action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;

    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionMoveFiles.Name.Default"));
    }
  }

  @Override
  public IAction open() {

    shell = new Shell(getParent(), SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    PropsUi.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);

    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ActionMoveFiles.Title"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // Buttons go at the very bottom
    //
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, event -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, event -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    // Filename line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "ActionMoveFiles.Name.Label"));
    PropsUi.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    wName.addModifyListener(lsMod);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, 0);
    fdName.top = new FormAttachment(0, margin);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setFont(GuiResource.getInstance().getFontDefault());
    wGeneralTab.setText(BaseMessages.getString(PKG, "ActionMoveFiles.Tab.General.Label"));

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout(generalLayout);

    // SETTINGS grouping?
    // ////////////////////////
    // START OF SETTINGS GROUP
    //

    Group wSettings = new Group(wGeneralComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wSettings);
    wSettings.setText(BaseMessages.getString(PKG, "ActionMoveFiles.Settings.Label"));

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;
    wSettings.setLayout(groupLayout);

    Label wlIncludeSubfolders = new Label(wSettings, SWT.RIGHT);
    wlIncludeSubfolders.setText(
        BaseMessages.getString(PKG, "ActionMoveFiles.IncludeSubfolders.Label"));
    PropsUi.setLook(wlIncludeSubfolders);
    FormData fdlIncludeSubfolders = new FormData();
    fdlIncludeSubfolders.left = new FormAttachment(0, 0);
    fdlIncludeSubfolders.top = new FormAttachment(wName, margin);
    fdlIncludeSubfolders.right = new FormAttachment(middle, -margin);
    wlIncludeSubfolders.setLayoutData(fdlIncludeSubfolders);
    wIncludeSubfolders = new Button(wSettings, SWT.CHECK);
    PropsUi.setLook(wIncludeSubfolders);
    wIncludeSubfolders.setToolTipText(
        BaseMessages.getString(PKG, "ActionMoveFiles.IncludeSubfolders.Tooltip"));
    FormData fdIncludeSubfolders = new FormData();
    fdIncludeSubfolders.left = new FormAttachment(middle, 0);
    fdIncludeSubfolders.top = new FormAttachment(wlIncludeSubfolders, 0, SWT.CENTER);
    fdIncludeSubfolders.right = new FormAttachment(100, 0);
    wIncludeSubfolders.setLayoutData(fdIncludeSubfolders);
    wIncludeSubfolders.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
            checkIncludeSubFolders();
          }
        });

    // Copy empty folders
    wlMoveEmptyFolders = new Label(wSettings, SWT.RIGHT);
    wlMoveEmptyFolders.setText(
        BaseMessages.getString(PKG, "ActionMoveFiles.MoveEmptyFolders.Label"));
    PropsUi.setLook(wlMoveEmptyFolders);
    FormData fdlMoveEmptyFolders = new FormData();
    fdlMoveEmptyFolders.left = new FormAttachment(0, 0);
    fdlMoveEmptyFolders.top = new FormAttachment(wlIncludeSubfolders, 2 * margin);
    fdlMoveEmptyFolders.right = new FormAttachment(middle, -margin);
    wlMoveEmptyFolders.setLayoutData(fdlMoveEmptyFolders);
    wMoveEmptyFolders = new Button(wSettings, SWT.CHECK);
    PropsUi.setLook(wMoveEmptyFolders);
    wMoveEmptyFolders.setToolTipText(
        BaseMessages.getString(PKG, "ActionMoveFiles.MoveEmptyFolders.Tooltip"));
    FormData fdMoveEmptyFolders = new FormData();
    fdMoveEmptyFolders.left = new FormAttachment(middle, 0);
    fdMoveEmptyFolders.top = new FormAttachment(wlMoveEmptyFolders, 0, SWT.CENTER);
    fdMoveEmptyFolders.right = new FormAttachment(100, 0);
    wMoveEmptyFolders.setLayoutData(fdMoveEmptyFolders);
    wMoveEmptyFolders.addListener(SWT.Selection, event -> action.setChanged());

    // Simulate?
    Label wlSimulate = new Label(wSettings, SWT.RIGHT);
    wlSimulate.setText(BaseMessages.getString(PKG, "ActionMoveFiles.Simulate.Label"));
    PropsUi.setLook(wlSimulate);
    FormData fdlSimulate = new FormData();
    fdlSimulate.left = new FormAttachment(0, 0);
    fdlSimulate.top = new FormAttachment(wlMoveEmptyFolders, 2 * margin);
    fdlSimulate.right = new FormAttachment(middle, -margin);
    wlSimulate.setLayoutData(fdlSimulate);
    wSimulate = new Button(wSettings, SWT.CHECK);
    PropsUi.setLook(wSimulate);
    wSimulate.setToolTipText(BaseMessages.getString(PKG, "ActionMoveFiles.Simulate.Tooltip"));
    FormData fdSimulate = new FormData();
    fdSimulate.left = new FormAttachment(middle, 0);
    fdSimulate.top = new FormAttachment(wlSimulate, 0, SWT.CENTER);
    fdSimulate.right = new FormAttachment(100, 0);
    wSimulate.setLayoutData(fdSimulate);
    wSimulate.addListener(SWT.Selection, event -> action.setChanged());

    // previous
    Label wlPrevious = new Label(wSettings, SWT.RIGHT);
    wlPrevious.setText(BaseMessages.getString(PKG, "ActionMoveFiles.Previous.Label"));
    PropsUi.setLook(wlPrevious);
    FormData fdlPrevious = new FormData();
    fdlPrevious.left = new FormAttachment(0, 0);
    fdlPrevious.top = new FormAttachment(wlSimulate, 2 * margin);
    fdlPrevious.right = new FormAttachment(middle, -margin);
    wlPrevious.setLayoutData(fdlPrevious);
    wPrevious = new Button(wSettings, SWT.CHECK);
    PropsUi.setLook(wPrevious);
    wPrevious.setSelection(action.argFromPrevious);
    wPrevious.setToolTipText(BaseMessages.getString(PKG, "ActionMoveFiles.Previous.Tooltip"));
    FormData fdPrevious = new FormData();
    fdPrevious.left = new FormAttachment(middle, 0);
    fdPrevious.top = new FormAttachment(wlPrevious, 0, SWT.CENTER);
    fdPrevious.right = new FormAttachment(100, 0);
    wPrevious.setLayoutData(fdPrevious);
    wPrevious.addListener(SWT.Selection, event -> refreshArgFromPrevious());
    FormData fdSettings = new FormData();
    fdSettings.left = new FormAttachment(0, margin);
    fdSettings.top = new FormAttachment(wName, margin);
    fdSettings.right = new FormAttachment(100, -margin);
    wSettings.setLayoutData(fdSettings);

    // ///////////////////////////////////////////////////////////
    // / END OF SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    wlFields = new Label(wGeneralComp, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "ActionMoveFiles.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.right = new FormAttachment(middle, -margin);
    fdlFields.top = new FormAttachment(wSettings, margin);
    wlFields.setLayoutData(fdlFields);

    int rows =
        action.sourceFileFolder == null
            ? 1
            : (action.sourceFileFolder.length == 0 ? 0 : action.sourceFileFolder.length);

    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionMoveFiles.Fields.SourceFileFolder.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT_BUTTON,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionMoveFiles.Fields.DestinationFileFolder.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT_BUTTON,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionMoveFiles.Fields.Wildcard.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
        };

    colinf[0].setUsingVariables(true);
    colinf[0].setToolTip(
        BaseMessages.getString(PKG, "ActionMoveFiles.Fields.SourceFileFolder.Tooltip"));
    colinf[0].setTextVarButtonSelectionListener(getFileSelectionAdapter());
    colinf[1].setUsingVariables(true);
    colinf[1].setToolTip(
        BaseMessages.getString(PKG, "ActionMoveFiles.Fields.DestinationFileFolder.Tooltip"));
    colinf[1].setTextVarButtonSelectionListener(getFileSelectionAdapter());
    colinf[2].setUsingVariables(true);
    colinf[2].setToolTip(BaseMessages.getString(PKG, "ActionMoveFiles.Fields.Wildcard.Tooltip"));

    wFields =
        new TableView(
            variables,
            wGeneralComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            rows,
            lsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(100, -margin);
    wFields.setLayoutData(fdFields);

    refreshArgFromPrevious();

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment(0, 0);
    fdGeneralComp.top = new FormAttachment(0, 0);
    fdGeneralComp.right = new FormAttachment(100, 0);
    fdGeneralComp.bottom = new FormAttachment(100, 0);
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);
    PropsUi.setLook(wGeneralComp);

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////////////////
    // START OF DESTINATION FILE TAB ///
    // ///////////////////////////////////

    CTabItem wDestinationFileTab = new CTabItem(wTabFolder, SWT.NONE);
    wDestinationFileTab.setFont(GuiResource.getInstance().getFontDefault());
    wDestinationFileTab.setText(
        BaseMessages.getString(PKG, "ActionMoveFiles.DestinationFileTab.Label"));

    FormLayout destcontentLayout = new FormLayout();
    destcontentLayout.marginWidth = 3;
    destcontentLayout.marginHeight = 3;

    Composite wDestinationFileComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wDestinationFileComp);
    wDestinationFileComp.setLayout(destcontentLayout);

    // DestinationFile grouping?
    // ////////////////////////
    // START OF DestinationFile GROUP
    //

    Group wDestinationFile = new Group(wDestinationFileComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wDestinationFile);
    wDestinationFile.setText(
        BaseMessages.getString(PKG, "ActionMoveFiles.GroupDestinationFile.Label"));

    FormLayout groupLayoutFile = new FormLayout();
    groupLayoutFile.marginWidth = 10;
    groupLayoutFile.marginHeight = 10;
    wDestinationFile.setLayout(groupLayoutFile);

    // Create destination folder/parent folder
    Label wlCreateDestinationFolder = new Label(wDestinationFile, SWT.RIGHT);
    wlCreateDestinationFolder.setText(
        BaseMessages.getString(PKG, "ActionMoveFiles.CreateDestinationFolder.Label"));
    PropsUi.setLook(wlCreateDestinationFolder);
    FormData fdlCreateDestinationFolder = new FormData();
    fdlCreateDestinationFolder.left = new FormAttachment(0, 0);
    fdlCreateDestinationFolder.top = new FormAttachment(0, margin);
    fdlCreateDestinationFolder.right = new FormAttachment(middle, -margin);
    wlCreateDestinationFolder.setLayoutData(fdlCreateDestinationFolder);
    wCreateDestinationFolder = new Button(wDestinationFile, SWT.CHECK);
    PropsUi.setLook(wCreateDestinationFolder);
    wCreateDestinationFolder.setToolTipText(
        BaseMessages.getString(PKG, "ActionMoveFiles.CreateDestinationFolder.Tooltip"));
    FormData fdCreateDestinationFolder = new FormData();
    fdCreateDestinationFolder.left = new FormAttachment(middle, 0);
    fdCreateDestinationFolder.top = new FormAttachment(wlCreateDestinationFolder, 0, SWT.CENTER);
    fdCreateDestinationFolder.right = new FormAttachment(100, 0);
    wCreateDestinationFolder.setLayoutData(fdCreateDestinationFolder);
    wCreateDestinationFolder.addListener(SWT.Selection, event -> action.setChanged());

    // Destination is a file?
    Label wlDestinationIsAFile = new Label(wDestinationFile, SWT.RIGHT);
    wlDestinationIsAFile.setText(
        BaseMessages.getString(PKG, "ActionMoveFiles.DestinationIsAFile.Label"));
    PropsUi.setLook(wlDestinationIsAFile);
    FormData fdlDestinationIsAFile = new FormData();
    fdlDestinationIsAFile.left = new FormAttachment(0, 0);
    fdlDestinationIsAFile.top = new FormAttachment(wlCreateDestinationFolder, 2 * margin);
    fdlDestinationIsAFile.right = new FormAttachment(middle, -margin);
    wlDestinationIsAFile.setLayoutData(fdlDestinationIsAFile);
    wDestinationIsAFile = new Button(wDestinationFile, SWT.CHECK);
    PropsUi.setLook(wDestinationIsAFile);
    wDestinationIsAFile.setToolTipText(
        BaseMessages.getString(PKG, "ActionMoveFiles.DestinationIsAFile.Tooltip"));
    FormData fdDestinationIsAFile = new FormData();
    fdDestinationIsAFile.left = new FormAttachment(middle, 0);
    fdDestinationIsAFile.top = new FormAttachment(wlDestinationIsAFile, 0, SWT.CENTER);
    fdDestinationIsAFile.right = new FormAttachment(100, 0);
    wDestinationIsAFile.setLayoutData(fdDestinationIsAFile);
    wDestinationIsAFile.addListener(SWT.Selection, event -> action.setChanged());

    // Do not keep folder structure?
    wlDoNotKeepFolderStructure = new Label(wDestinationFile, SWT.RIGHT);
    wlDoNotKeepFolderStructure.setText(
        BaseMessages.getString(PKG, "ActionMoveFiles.DoNotKeepFolderStructure.Label"));
    PropsUi.setLook(wlDoNotKeepFolderStructure);
    FormData fdlDoNotKeepFolderStructure = new FormData();
    fdlDoNotKeepFolderStructure.left = new FormAttachment(0, 0);
    fdlDoNotKeepFolderStructure.top = new FormAttachment(wlDestinationIsAFile, 2 * margin);
    fdlDoNotKeepFolderStructure.right = new FormAttachment(middle, -margin);
    wlDoNotKeepFolderStructure.setLayoutData(fdlDoNotKeepFolderStructure);
    wDoNotKeepFolderStructure = new Button(wDestinationFile, SWT.CHECK);
    PropsUi.setLook(wDoNotKeepFolderStructure);
    wDoNotKeepFolderStructure.setToolTipText(
        BaseMessages.getString(PKG, "ActionMoveFiles.DoNotKeepFolderStructure.Tooltip"));
    FormData fdDoNotKeepFolderStructure = new FormData();
    fdDoNotKeepFolderStructure.left = new FormAttachment(middle, 0);
    fdDoNotKeepFolderStructure.top = new FormAttachment(wlDoNotKeepFolderStructure, 0, SWT.CENTER);
    fdDoNotKeepFolderStructure.right = new FormAttachment(100, 0);
    wDoNotKeepFolderStructure.setLayoutData(fdDoNotKeepFolderStructure);
    wDoNotKeepFolderStructure.addListener(SWT.Selection, event -> action.setChanged());

    // Create multi-part file?
    wlAddDate = new Label(wDestinationFile, SWT.RIGHT);
    wlAddDate.setText(BaseMessages.getString(PKG, "ActionMoveFiles.AddDate.Label"));
    PropsUi.setLook(wlAddDate);
    FormData fdlAddDate = new FormData();
    fdlAddDate.left = new FormAttachment(0, 0);
    fdlAddDate.top = new FormAttachment(wlDoNotKeepFolderStructure, 2 * margin);
    fdlAddDate.right = new FormAttachment(middle, -margin);
    wlAddDate.setLayoutData(fdlAddDate);
    wAddDate = new Button(wDestinationFile, SWT.CHECK);
    PropsUi.setLook(wAddDate);
    wAddDate.setToolTipText(BaseMessages.getString(PKG, "ActionMoveFiles.AddDate.Tooltip"));
    FormData fdAddDate = new FormData();
    fdAddDate.left = new FormAttachment(middle, 0);
    fdAddDate.top = new FormAttachment(wlAddDate, 0, SWT.CENTER);
    fdAddDate.right = new FormAttachment(100, 0);
    wAddDate.setLayoutData(fdAddDate);
    wAddDate.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
            setAddDateBeforeExtension();
          }
        });
    // Create multi-part file?
    wlAddTime = new Label(wDestinationFile, SWT.RIGHT);
    wlAddTime.setText(BaseMessages.getString(PKG, "ActionMoveFiles.AddTime.Label"));
    PropsUi.setLook(wlAddTime);
    FormData fdlAddTime = new FormData();
    fdlAddTime.left = new FormAttachment(0, 0);
    fdlAddTime.top = new FormAttachment(wlAddDate, 2 * margin);
    fdlAddTime.right = new FormAttachment(middle, -margin);
    wlAddTime.setLayoutData(fdlAddTime);
    wAddTime = new Button(wDestinationFile, SWT.CHECK);
    PropsUi.setLook(wAddTime);
    wAddTime.setToolTipText(BaseMessages.getString(PKG, "ActionMoveFiles.AddTime.Tooltip"));
    FormData fdAddTime = new FormData();
    fdAddTime.left = new FormAttachment(middle, 0);
    fdAddTime.top = new FormAttachment(wlAddTime, 0, SWT.CENTER);
    fdAddTime.right = new FormAttachment(100, 0);
    wAddTime.setLayoutData(fdAddTime);
    wAddTime.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
            setAddDateBeforeExtension();
          }
        });

    // Specify date time format?
    Label wlSpecifyFormat = new Label(wDestinationFile, SWT.RIGHT);
    wlSpecifyFormat.setText(BaseMessages.getString(PKG, "ActionMoveFiles.SpecifyFormat.Label"));
    PropsUi.setLook(wlSpecifyFormat);
    FormData fdlSpecifyFormat = new FormData();
    fdlSpecifyFormat.left = new FormAttachment(0, 0);
    fdlSpecifyFormat.top = new FormAttachment(wlAddTime, 2 * margin);
    fdlSpecifyFormat.right = new FormAttachment(middle, -margin);
    wlSpecifyFormat.setLayoutData(fdlSpecifyFormat);
    wSpecifyFormat = new Button(wDestinationFile, SWT.CHECK);
    PropsUi.setLook(wSpecifyFormat);
    wSpecifyFormat.setToolTipText(
        BaseMessages.getString(PKG, "ActionMoveFiles.SpecifyFormat.Tooltip"));
    FormData fdSpecifyFormat = new FormData();
    fdSpecifyFormat.left = new FormAttachment(middle, 0);
    fdSpecifyFormat.top = new FormAttachment(wlSpecifyFormat, 0, SWT.CENTER);
    fdSpecifyFormat.right = new FormAttachment(100, 0);
    wSpecifyFormat.setLayoutData(fdSpecifyFormat);
    wSpecifyFormat.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
            setDateTimeFormat();
            setAddDateBeforeExtension();
          }
        });

    // DateTimeFormat
    wlDateTimeFormat = new Label(wDestinationFile, SWT.RIGHT);
    wlDateTimeFormat.setText(BaseMessages.getString(PKG, "ActionMoveFiles.DateTimeFormat.Label"));
    PropsUi.setLook(wlDateTimeFormat);
    FormData fdlDateTimeFormat = new FormData();
    fdlDateTimeFormat.left = new FormAttachment(0, 0);
    fdlDateTimeFormat.top = new FormAttachment(wlSpecifyFormat, 2 * margin);
    fdlDateTimeFormat.right = new FormAttachment(middle, -margin);
    wlDateTimeFormat.setLayoutData(fdlDateTimeFormat);
    wDateTimeFormat = new CCombo(wDestinationFile, SWT.BORDER | SWT.READ_ONLY);
    wDateTimeFormat.setEditable(true);
    PropsUi.setLook(wDateTimeFormat);
    wDateTimeFormat.addModifyListener(lsMod);
    FormData fdDateTimeFormat = new FormData();
    fdDateTimeFormat.left = new FormAttachment(middle, 0);
    fdDateTimeFormat.top = new FormAttachment(wlSpecifyFormat, 2 * margin);
    fdDateTimeFormat.right = new FormAttachment(100, 0);
    wDateTimeFormat.setLayoutData(fdDateTimeFormat);
    // Prepare a list of possible DateTimeFormats...
    String[] dats = Const.getDateFormats();
    for (String s : dats) {
      wDateTimeFormat.add(s);
    }

    // Add Date before extension?
    wlAddDateBeforeExtension = new Label(wDestinationFile, SWT.RIGHT);
    wlAddDateBeforeExtension.setText(
        BaseMessages.getString(PKG, "ActionMoveFiles.AddDateBeforeExtension.Label"));
    PropsUi.setLook(wlAddDateBeforeExtension);
    FormData fdlAddDateBeforeExtension = new FormData();
    fdlAddDateBeforeExtension.left = new FormAttachment(0, 0);
    fdlAddDateBeforeExtension.top = new FormAttachment(wDateTimeFormat, margin);
    fdlAddDateBeforeExtension.right = new FormAttachment(middle, -margin);
    wlAddDateBeforeExtension.setLayoutData(fdlAddDateBeforeExtension);
    wAddDateBeforeExtension = new Button(wDestinationFile, SWT.CHECK);
    PropsUi.setLook(wAddDateBeforeExtension);
    wAddDateBeforeExtension.setToolTipText(
        BaseMessages.getString(PKG, "ActionMoveFiles.AddDateBeforeExtension.Tooltip"));
    FormData fdAddDateBeforeExtension = new FormData();
    fdAddDateBeforeExtension.left = new FormAttachment(middle, 0);
    fdAddDateBeforeExtension.top = new FormAttachment(wlAddDateBeforeExtension, 0, SWT.CENTER);
    fdAddDateBeforeExtension.right = new FormAttachment(100, 0);
    wAddDateBeforeExtension.setLayoutData(fdAddDateBeforeExtension);
    wAddDateBeforeExtension.addListener(SWT.Selection, event -> action.setChanged());

    // If File Exists
    Label wlIfFileExists = new Label(wDestinationFile, SWT.RIGHT);
    wlIfFileExists.setText(BaseMessages.getString(PKG, "ActionMoveFiles.IfFileExists.Label"));
    PropsUi.setLook(wlIfFileExists);
    FormData fdlIfFileExists = new FormData();
    fdlIfFileExists.left = new FormAttachment(0, 0);
    fdlIfFileExists.right = new FormAttachment(middle, 0);
    fdlIfFileExists.top = new FormAttachment(wlAddDateBeforeExtension, 2 * margin);
    wlIfFileExists.setLayoutData(fdlIfFileExists);

    wIfFileExists = new CCombo(wDestinationFile, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wIfFileExists.add(BaseMessages.getString(PKG, "ActionMoveFiles.Do_Nothing_IfFileExists.Label"));
    wIfFileExists.add(
        BaseMessages.getString(PKG, "ActionMoveFiles.Overwrite_File_IfFileExists.Label"));
    wIfFileExists.add(
        BaseMessages.getString(PKG, "ActionMoveFiles.Unique_Name_IfFileExists.Label"));
    wIfFileExists.add(
        BaseMessages.getString(PKG, "ActionMoveFiles.Delete_Source_File_IfFileExists.Label"));
    wIfFileExists.add(
        BaseMessages.getString(PKG, "ActionMoveFiles.Move_To_Folder_IfFileExists.Label"));
    wIfFileExists.add(BaseMessages.getString(PKG, "ActionMoveFiles.Fail_IfFileExists.Label"));
    wIfFileExists.select(0); // +1: starts at -1
    PropsUi.setLook(wIfFileExists);
    FormData fdIfFileExists = new FormData();
    fdIfFileExists.left = new FormAttachment(middle, 0);
    fdIfFileExists.top = new FormAttachment(wlAddDateBeforeExtension, 2 * margin);
    fdIfFileExists.right = new FormAttachment(100, 0);
    wIfFileExists.setLayoutData(fdIfFileExists);
    wIfFileExists.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {

            activeDestinationFolder();
            setMovedDateTimeFormat();
            setAddMovedDateBeforeExtension();
          }
        });

    FormData fdDestinationFile = new FormData();
    fdDestinationFile.left = new FormAttachment(0, margin);
    fdDestinationFile.top = new FormAttachment(wName, margin);
    fdDestinationFile.right = new FormAttachment(100, -margin);
    wDestinationFile.setLayoutData(fdDestinationFile);

    // ///////////////////////////////////////////////////////////
    // / END OF DestinationFile GROUP
    // ///////////////////////////////////////////////////////////

    // MoveTo grouping?
    // ////////////////////////
    // START OF MoveTo GROUP
    //

    Group wMoveToGroup = new Group(wDestinationFileComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wMoveToGroup);
    wMoveToGroup.setText(BaseMessages.getString(PKG, "ActionMoveFiles.GroupMoveToGroup.Label"));

    FormLayout movetoLayoutFile = new FormLayout();
    movetoLayoutFile.marginWidth = 10;
    movetoLayoutFile.marginHeight = 10;
    wMoveToGroup.setLayout(movetoLayoutFile);

    // DestinationFolder line
    wlDestinationFolder = new Label(wMoveToGroup, SWT.RIGHT);
    wlDestinationFolder.setText(
        BaseMessages.getString(PKG, "ActionMoveFiles.DestinationFolder.Label"));
    PropsUi.setLook(wlDestinationFolder);
    FormData fdlDestinationFolder = new FormData();
    fdlDestinationFolder.left = new FormAttachment(0, 0);
    fdlDestinationFolder.top = new FormAttachment(wDestinationFile, margin);
    fdlDestinationFolder.right = new FormAttachment(middle, -margin);
    wlDestinationFolder.setLayoutData(fdlDestinationFolder);

    wbDestinationFolder = new Button(wMoveToGroup, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbDestinationFolder);
    wbDestinationFolder.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbDestinationFolder = new FormData();
    fdbDestinationFolder.right = new FormAttachment(100, 0);
    fdbDestinationFolder.top = new FormAttachment(wDestinationFile, 0);
    wbDestinationFolder.setLayoutData(fdbDestinationFolder);

    wDestinationFolder = new TextVar(variables, wMoveToGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDestinationFolder);
    wDestinationFolder.addModifyListener(lsMod);
    FormData fdDestinationFolder = new FormData();
    fdDestinationFolder.left = new FormAttachment(middle, 0);
    fdDestinationFolder.top = new FormAttachment(wDestinationFile, margin);
    fdDestinationFolder.right = new FormAttachment(wbDestinationFolder, -margin);
    wDestinationFolder.setLayoutData(fdDestinationFolder);

    // Whenever something changes, set the tooltip to the expanded version:
    wDestinationFolder.addModifyListener(
        e -> wDestinationFolder.setToolTipText(variables.resolve(wDestinationFolder.getText())));
    wbDestinationFolder.addListener(
        SWT.Selection,
        e -> BaseDialog.presentDirectoryDialog(shell, wDestinationFolder, variables));

    // Create destination folder/parent folder
    wlCreateMoveToFolder = new Label(wMoveToGroup, SWT.RIGHT);
    wlCreateMoveToFolder.setText(
        BaseMessages.getString(PKG, "ActionMoveFiles.CreateMoveToFolder.Label"));
    PropsUi.setLook(wlCreateMoveToFolder);
    FormData fdlCreateMoveToFolder = new FormData();
    fdlCreateMoveToFolder.left = new FormAttachment(0, 0);
    fdlCreateMoveToFolder.top = new FormAttachment(wDestinationFolder, margin);
    fdlCreateMoveToFolder.right = new FormAttachment(middle, -margin);
    wlCreateMoveToFolder.setLayoutData(fdlCreateMoveToFolder);
    wCreateMoveToFolder = new Button(wMoveToGroup, SWT.CHECK);
    PropsUi.setLook(wCreateMoveToFolder);
    wCreateMoveToFolder.setToolTipText(
        BaseMessages.getString(PKG, "ActionMoveFiles.CreateMoveToFolder.Tooltip"));
    FormData fdCreateMoveToFolder = new FormData();
    fdCreateMoveToFolder.left = new FormAttachment(middle, 0);
    fdCreateMoveToFolder.top = new FormAttachment(wlCreateMoveToFolder, 0, SWT.CENTER);
    fdCreateMoveToFolder.right = new FormAttachment(100, 0);
    wCreateMoveToFolder.setLayoutData(fdCreateMoveToFolder);
    wCreateMoveToFolder.addListener(SWT.Selection, event -> action.setChanged());

    // Create multi-part file?
    wlAddMovedDate = new Label(wMoveToGroup, SWT.RIGHT);
    wlAddMovedDate.setText(BaseMessages.getString(PKG, "ActionMoveFiles.AddMovedDate.Label"));
    PropsUi.setLook(wlAddMovedDate);
    FormData fdlAddMovedDate = new FormData();
    fdlAddMovedDate.left = new FormAttachment(0, 0);
    fdlAddMovedDate.top = new FormAttachment(wlCreateMoveToFolder, 2 * margin);
    fdlAddMovedDate.right = new FormAttachment(middle, -margin);
    wlAddMovedDate.setLayoutData(fdlAddMovedDate);
    wAddMovedDate = new Button(wMoveToGroup, SWT.CHECK);
    PropsUi.setLook(wAddMovedDate);
    wAddMovedDate.setToolTipText(
        BaseMessages.getString(PKG, "ActionMoveFiles.AddMovedDate.Tooltip"));
    FormData fdAddMovedDate = new FormData();
    fdAddMovedDate.left = new FormAttachment(middle, 0);
    fdAddMovedDate.top = new FormAttachment(wlAddMovedDate, 0, SWT.CENTER);
    fdAddMovedDate.right = new FormAttachment(100, 0);
    wAddMovedDate.setLayoutData(fdAddMovedDate);
    wAddMovedDate.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
            setAddMovedDateBeforeExtension();
          }
        });
    // Create multi-part file?
    wlAddMovedTime = new Label(wMoveToGroup, SWT.RIGHT);
    wlAddMovedTime.setText(BaseMessages.getString(PKG, "ActionMoveFiles.AddMovedTime.Label"));
    PropsUi.setLook(wlAddMovedTime);
    FormData fdlAddMovedTime = new FormData();
    fdlAddMovedTime.left = new FormAttachment(0, 0);
    fdlAddMovedTime.top = new FormAttachment(wlAddMovedDate, 2 * margin);
    fdlAddMovedTime.right = new FormAttachment(middle, -margin);
    wlAddMovedTime.setLayoutData(fdlAddMovedTime);
    wAddMovedTime = new Button(wMoveToGroup, SWT.CHECK);
    PropsUi.setLook(wAddMovedTime);
    wAddMovedTime.setToolTipText(
        BaseMessages.getString(PKG, "ActionMoveFiles.AddMovedTime.Tooltip"));
    FormData fdAddMovedTime = new FormData();
    fdAddMovedTime.left = new FormAttachment(middle, 0);
    fdAddMovedTime.top = new FormAttachment(wlAddMovedTime, 0, SWT.CENTER);
    fdAddMovedTime.right = new FormAttachment(100, 0);
    wAddMovedTime.setLayoutData(fdAddMovedTime);
    wAddMovedTime.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
            setAddMovedDateBeforeExtension();
          }
        });

    // Specify date time format?
    wlSpecifyMoveFormat = new Label(wMoveToGroup, SWT.RIGHT);
    wlSpecifyMoveFormat.setText(
        BaseMessages.getString(PKG, "ActionMoveFiles.SpecifyMoveFormat.Label"));
    PropsUi.setLook(wlSpecifyMoveFormat);
    FormData fdlSpecifyMoveFormat = new FormData();
    fdlSpecifyMoveFormat.left = new FormAttachment(0, 0);
    fdlSpecifyMoveFormat.top = new FormAttachment(wlAddMovedTime, 2 * margin);
    fdlSpecifyMoveFormat.right = new FormAttachment(middle, -margin);
    wlSpecifyMoveFormat.setLayoutData(fdlSpecifyMoveFormat);
    wSpecifyMoveFormat = new Button(wMoveToGroup, SWT.CHECK);
    PropsUi.setLook(wSpecifyMoveFormat);
    wSpecifyMoveFormat.setToolTipText(
        BaseMessages.getString(PKG, "ActionMoveFiles.SpecifyMoveFormat.Tooltip"));
    FormData fdSpecifyMoveFormat = new FormData();
    fdSpecifyMoveFormat.left = new FormAttachment(middle, 0);
    fdSpecifyMoveFormat.top = new FormAttachment(wlSpecifyMoveFormat, 0, SWT.CENTER);
    fdSpecifyMoveFormat.right = new FormAttachment(100, 0);
    wSpecifyMoveFormat.setLayoutData(fdSpecifyMoveFormat);
    wSpecifyMoveFormat.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
            setMovedDateTimeFormat();
            setAddMovedDateBeforeExtension();
          }
        });

    // Moved DateTimeFormat
    wlMovedDateTimeFormat = new Label(wMoveToGroup, SWT.RIGHT);
    wlMovedDateTimeFormat.setText(
        BaseMessages.getString(PKG, "ActionMoveFiles.MovedDateTimeFormat.Label"));
    PropsUi.setLook(wlMovedDateTimeFormat);
    FormData fdlMovedDateTimeFormat = new FormData();
    fdlMovedDateTimeFormat.left = new FormAttachment(0, 0);
    fdlMovedDateTimeFormat.top = new FormAttachment(wlSpecifyMoveFormat, 2 * margin);
    fdlMovedDateTimeFormat.right = new FormAttachment(middle, -margin);
    wlMovedDateTimeFormat.setLayoutData(fdlMovedDateTimeFormat);
    wMovedDateTimeFormat = new CCombo(wMoveToGroup, SWT.BORDER | SWT.READ_ONLY);
    wMovedDateTimeFormat.setEditable(true);
    PropsUi.setLook(wMovedDateTimeFormat);
    wMovedDateTimeFormat.addModifyListener(lsMod);
    FormData fdMovedDateTimeFormat = new FormData();
    fdMovedDateTimeFormat.left = new FormAttachment(middle, 0);
    fdMovedDateTimeFormat.top = new FormAttachment(wlSpecifyMoveFormat, 2 * margin);
    fdMovedDateTimeFormat.right = new FormAttachment(100, 0);
    wMovedDateTimeFormat.setLayoutData(fdMovedDateTimeFormat);

    for (String dat : dats) {
      wMovedDateTimeFormat.add(dat);
    }

    // Add Date before extension?
    wlAddMovedDateBeforeExtension = new Label(wMoveToGroup, SWT.RIGHT);
    wlAddMovedDateBeforeExtension.setText(
        BaseMessages.getString(PKG, "ActionMoveFiles.AddMovedDateBeforeExtension.Label"));
    PropsUi.setLook(wlAddMovedDateBeforeExtension);
    FormData fdlAddMovedDateBeforeExtension = new FormData();
    fdlAddMovedDateBeforeExtension.left = new FormAttachment(0, 0);
    fdlAddMovedDateBeforeExtension.top = new FormAttachment(wMovedDateTimeFormat, margin);
    fdlAddMovedDateBeforeExtension.right = new FormAttachment(middle, -margin);
    wlAddMovedDateBeforeExtension.setLayoutData(fdlAddMovedDateBeforeExtension);
    wAddMovedDateBeforeExtension = new Button(wMoveToGroup, SWT.CHECK);
    PropsUi.setLook(wAddMovedDateBeforeExtension);
    wAddMovedDateBeforeExtension.setToolTipText(
        BaseMessages.getString(PKG, "ActionMoveFiles.AddMovedDateBeforeExtension.Tooltip"));
    FormData fdAddMovedDateBeforeExtension = new FormData();
    fdAddMovedDateBeforeExtension.left = new FormAttachment(middle, 0);
    fdAddMovedDateBeforeExtension.top =
        new FormAttachment(wlAddMovedDateBeforeExtension, 0, SWT.CENTER);
    fdAddMovedDateBeforeExtension.right = new FormAttachment(100, 0);
    wAddMovedDateBeforeExtension.setLayoutData(fdAddMovedDateBeforeExtension);
    wAddMovedDateBeforeExtension.addListener(SWT.Selection, event -> action.setChanged());

    // If moved File Exists
    wlIfMovedFileExists = new Label(wMoveToGroup, SWT.RIGHT);
    wlIfMovedFileExists.setText(
        BaseMessages.getString(PKG, "ActionMoveFiles.IfMovedFileExists.Label"));
    PropsUi.setLook(wlIfMovedFileExists);
    FormData fdlIfMovedFileExists = new FormData();
    fdlIfMovedFileExists.left = new FormAttachment(0, 0);
    fdlIfMovedFileExists.right = new FormAttachment(middle, 0);
    fdlIfMovedFileExists.top = new FormAttachment(wlAddMovedDateBeforeExtension, 2 * margin);
    wlIfMovedFileExists.setLayoutData(fdlIfMovedFileExists);
    wIfMovedFileExists = new CCombo(wMoveToGroup, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wIfMovedFileExists.add(
        BaseMessages.getString(PKG, "ActionMoveFiles.Do_Nothing_IfMovedFileExists.Label"));
    wIfMovedFileExists.add(
        BaseMessages.getString(PKG, "ActionMoveFiles.Overwrite_Filename_IffMovedFileExists.Label"));
    wIfMovedFileExists.add(
        BaseMessages.getString(PKG, "ActionMoveFiles.UniqueName_IfMovedFileExists.Label"));
    wIfMovedFileExists.add(
        BaseMessages.getString(PKG, "ActionMoveFiles.Fail_IfMovedFileExists.Label"));
    wIfMovedFileExists.select(0); // +1: starts at -1
    PropsUi.setLook(wIfMovedFileExists);
    FormData fdIfMovedFileExists = new FormData();
    fdIfMovedFileExists.left = new FormAttachment(middle, 0);
    fdIfMovedFileExists.top = new FormAttachment(wlAddMovedDateBeforeExtension, 2 * margin);
    fdIfMovedFileExists.right = new FormAttachment(100, 0);
    wIfMovedFileExists.setLayoutData(fdIfMovedFileExists);

    FormData fdMoveToGroup = new FormData();
    fdMoveToGroup.left = new FormAttachment(0, margin);
    fdMoveToGroup.top = new FormAttachment(wDestinationFile, margin);
    fdMoveToGroup.right = new FormAttachment(100, -margin);
    wMoveToGroup.setLayoutData(fdMoveToGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF MoveToGroup GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdDestinationFileComp = new FormData();
    fdDestinationFileComp.left = new FormAttachment(0, 0);
    fdDestinationFileComp.top = new FormAttachment(0, 0);
    fdDestinationFileComp.right = new FormAttachment(100, 0);
    fdDestinationFileComp.bottom = new FormAttachment(100, 0);
    wDestinationFileComp.setLayoutData(fdDestinationFileComp);

    wDestinationFileComp.layout();
    wDestinationFileTab.setControl(wDestinationFileComp);

    // ///////////////////////////////////////////////////////////
    // / END OF DESTINATION FILETAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////////////////
    // START OF ADVANCED TAB ///
    // ///////////////////////////////////

    CTabItem wAdvancedTab = new CTabItem(wTabFolder, SWT.NONE);
    wAdvancedTab.setFont(GuiResource.getInstance().getFontDefault());
    wAdvancedTab.setText(BaseMessages.getString(PKG, "ActionMoveFiles.Tab.Advanced.Label"));

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    Composite wAdvancedComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wAdvancedComp);
    wAdvancedComp.setLayout(contentLayout);

    // SuccessOngrouping?
    // ////////////////////////
    // START OF SUCCESS ON GROUP///
    // /
    Group wSuccessOn = new Group(wAdvancedComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wSuccessOn);
    wSuccessOn.setText(BaseMessages.getString(PKG, "ActionMoveFiles.SuccessOn.Group.Label"));

    FormLayout successongroupLayout = new FormLayout();
    successongroupLayout.marginWidth = 10;
    successongroupLayout.marginHeight = 10;

    wSuccessOn.setLayout(successongroupLayout);

    // Success Condition
    Label wlSuccessCondition = new Label(wSuccessOn, SWT.RIGHT);
    wlSuccessCondition.setText(
        BaseMessages.getString(PKG, "ActionMoveFiles.SuccessCondition.Label"));
    PropsUi.setLook(wlSuccessCondition);
    FormData fdlSuccessCondition = new FormData();
    fdlSuccessCondition.left = new FormAttachment(0, 0);
    fdlSuccessCondition.right = new FormAttachment(middle, 0);
    fdlSuccessCondition.top = new FormAttachment(0, margin);
    wlSuccessCondition.setLayoutData(fdlSuccessCondition);
    wSuccessCondition = new CCombo(wSuccessOn, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wSuccessCondition.add(
        BaseMessages.getString(PKG, "ActionMoveFiles.SuccessWhenAllWorksFine.Label"));
    wSuccessCondition.add(BaseMessages.getString(PKG, "ActionMoveFiles.SuccessWhenAtLeat.Label"));
    wSuccessCondition.add(
        BaseMessages.getString(PKG, "ActionMoveFiles.SuccessWhenErrorsLessThan.Label"));

    wSuccessCondition.select(0); // +1: starts at -1

    PropsUi.setLook(wSuccessCondition);
    FormData fdSuccessCondition = new FormData();
    fdSuccessCondition.left = new FormAttachment(middle, 0);
    fdSuccessCondition.top = new FormAttachment(0, margin);
    fdSuccessCondition.right = new FormAttachment(100, 0);
    wSuccessCondition.setLayoutData(fdSuccessCondition);
    wSuccessCondition.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            activeSuccessCondition();
          }
        });

    // Success when number of errors less than
    wlNrErrorsLessThan = new Label(wSuccessOn, SWT.RIGHT);
    wlNrErrorsLessThan.setText(
        BaseMessages.getString(PKG, "ActionMoveFiles.NrErrorsLessThan.Label"));
    PropsUi.setLook(wlNrErrorsLessThan);
    FormData fdlNrErrorsLessThan = new FormData();
    fdlNrErrorsLessThan.left = new FormAttachment(0, 0);
    fdlNrErrorsLessThan.top = new FormAttachment(wSuccessCondition, margin);
    fdlNrErrorsLessThan.right = new FormAttachment(middle, -margin);
    wlNrErrorsLessThan.setLayoutData(fdlNrErrorsLessThan);

    wNrErrorsLessThan =
        new TextVar(
            variables,
            wSuccessOn,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionMoveFiles.NrErrorsLessThan.Tooltip"));
    PropsUi.setLook(wNrErrorsLessThan);
    wNrErrorsLessThan.addModifyListener(lsMod);
    FormData fdNrErrorsLessThan = new FormData();
    fdNrErrorsLessThan.left = new FormAttachment(middle, 0);
    fdNrErrorsLessThan.top = new FormAttachment(wSuccessCondition, margin);
    fdNrErrorsLessThan.right = new FormAttachment(100, -margin);
    wNrErrorsLessThan.setLayoutData(fdNrErrorsLessThan);

    FormData fdSuccessOn = new FormData();
    fdSuccessOn.left = new FormAttachment(0, margin);
    fdSuccessOn.top = new FormAttachment(wDestinationFile, margin);
    fdSuccessOn.right = new FormAttachment(100, -margin);
    wSuccessOn.setLayoutData(fdSuccessOn);
    // ///////////////////////////////////////////////////////////
    // / END OF Success ON GROUP
    // ///////////////////////////////////////////////////////////

    // fileresult grouping?
    // ////////////////////////
    // START OF LOGGING GROUP///
    // /
    Group wFileResult = new Group(wAdvancedComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wFileResult);
    wFileResult.setText(BaseMessages.getString(PKG, "ActionMoveFiles.FileResult.Group.Label"));

    FormLayout fileresultgroupLayout = new FormLayout();
    fileresultgroupLayout.marginWidth = 10;
    fileresultgroupLayout.marginHeight = 10;

    wFileResult.setLayout(fileresultgroupLayout);

    // Add file to result
    Label wlAddFileToResult = new Label(wFileResult, SWT.RIGHT);
    wlAddFileToResult.setText(BaseMessages.getString(PKG, "ActionMoveFiles.AddFileToResult.Label"));
    PropsUi.setLook(wlAddFileToResult);
    FormData fdlAddFileToResult = new FormData();
    fdlAddFileToResult.left = new FormAttachment(0, 0);
    fdlAddFileToResult.top = new FormAttachment(wSuccessOn, margin);
    fdlAddFileToResult.right = new FormAttachment(middle, -margin);
    wlAddFileToResult.setLayoutData(fdlAddFileToResult);
    wAddFileToResult = new Button(wFileResult, SWT.CHECK);
    PropsUi.setLook(wAddFileToResult);
    wAddFileToResult.setToolTipText(
        BaseMessages.getString(PKG, "ActionMoveFiles.AddFileToResult.Tooltip"));
    FormData fdAddFileToResult = new FormData();
    fdAddFileToResult.left = new FormAttachment(middle, 0);
    fdAddFileToResult.top = new FormAttachment(wlAddFileToResult, 0, SWT.CENTER);
    fdAddFileToResult.right = new FormAttachment(100, 0);
    wAddFileToResult.setLayoutData(fdAddFileToResult);
    wAddFileToResult.addListener(SWT.Selection, event -> action.setChanged());

    FormData fdFileResult = new FormData();
    fdFileResult.left = new FormAttachment(0, margin);
    fdFileResult.top = new FormAttachment(wSuccessOn, margin);
    fdFileResult.right = new FormAttachment(100, -margin);
    wFileResult.setLayoutData(fdFileResult);
    // ///////////////////////////////////////////////////////////
    // / END OF FilesResult GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdAdvancedComp = new FormData();
    fdAdvancedComp.left = new FormAttachment(0, 0);
    fdAdvancedComp.top = new FormAttachment(0, 0);
    fdAdvancedComp.right = new FormAttachment(100, 0);
    fdAdvancedComp.bottom = new FormAttachment(100, 0);
    wAdvancedComp.setLayoutData(fdAdvancedComp);

    wAdvancedComp.layout();
    wAdvancedTab.setControl(wAdvancedComp);

    // ///////////////////////////////////////////////////////////
    // / END OF ADVANCED TAB
    // ///////////////////////////////////////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wName, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fdTabFolder);

    getData();
    checkIncludeSubFolders();
    activeSuccessCondition();
    setDateTimeFormat();
    activeSuccessCondition();

    activeDestinationFolder();
    setMovedDateTimeFormat();
    setAddDateBeforeExtension();
    setAddMovedDateBeforeExtension();

    wTabFolder.setSelection(0);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  private void activeDestinationFolder() {

    wbDestinationFolder.setEnabled(wIfFileExists.getSelectionIndex() == 4);
    wlDestinationFolder.setEnabled(wIfFileExists.getSelectionIndex() == 4);
    wDestinationFolder.setEnabled(wIfFileExists.getSelectionIndex() == 4);
    wlMovedDateTimeFormat.setEnabled(wIfFileExists.getSelectionIndex() == 4);
    wMovedDateTimeFormat.setEnabled(wIfFileExists.getSelectionIndex() == 4);
    wIfMovedFileExists.setEnabled(wIfFileExists.getSelectionIndex() == 4);
    wlIfMovedFileExists.setEnabled(wIfFileExists.getSelectionIndex() == 4);
    wlAddMovedDateBeforeExtension.setEnabled(wIfFileExists.getSelectionIndex() == 4);
    wAddMovedDateBeforeExtension.setEnabled(wIfFileExists.getSelectionIndex() == 4);
    wlAddMovedDate.setEnabled(wIfFileExists.getSelectionIndex() == 4);
    wAddMovedDate.setEnabled(wIfFileExists.getSelectionIndex() == 4);
    wlAddMovedTime.setEnabled(wIfFileExists.getSelectionIndex() == 4);
    wAddMovedTime.setEnabled(wIfFileExists.getSelectionIndex() == 4);
    wlSpecifyMoveFormat.setEnabled(wIfFileExists.getSelectionIndex() == 4);
    wSpecifyMoveFormat.setEnabled(wIfFileExists.getSelectionIndex() == 4);
    wlCreateMoveToFolder.setEnabled(wIfFileExists.getSelectionIndex() == 4);
    wCreateMoveToFolder.setEnabled(wIfFileExists.getSelectionIndex() == 4);
  }

  private void activeSuccessCondition() {
    wlNrErrorsLessThan.setEnabled(wSuccessCondition.getSelectionIndex() != 0);
    wNrErrorsLessThan.setEnabled(wSuccessCondition.getSelectionIndex() != 0);
  }

  private void setAddDateBeforeExtension() {
    wlAddDateBeforeExtension.setEnabled(
        wAddDate.getSelection() || wAddTime.getSelection() || wSpecifyFormat.getSelection());
    wAddDateBeforeExtension.setEnabled(
        wAddDate.getSelection() || wAddTime.getSelection() || wSpecifyFormat.getSelection());
    if (!wAddDate.getSelection() && !wAddTime.getSelection() && !wSpecifyFormat.getSelection()) {
      wAddDateBeforeExtension.setSelection(false);
    }
  }

  private void setAddMovedDateBeforeExtension() {
    wlAddMovedDateBeforeExtension.setEnabled(
        wAddMovedDate.getSelection()
            || wAddMovedTime.getSelection()
            || wSpecifyMoveFormat.getSelection());
    wAddMovedDateBeforeExtension.setEnabled(
        wAddMovedDate.getSelection()
            || wAddMovedTime.getSelection()
            || wSpecifyMoveFormat.getSelection());
    if (!wAddMovedDate.getSelection()
        && !wAddMovedTime.getSelection()
        && !wSpecifyMoveFormat.getSelection()) {
      wAddMovedDateBeforeExtension.setSelection(false);
    }
  }

  private void setDateTimeFormat() {
    if (wSpecifyFormat.getSelection()) {
      wAddDate.setSelection(false);
      wAddTime.setSelection(false);
    }

    wDateTimeFormat.setEnabled(wSpecifyFormat.getSelection());
    wlDateTimeFormat.setEnabled(wSpecifyFormat.getSelection());
    wAddDate.setEnabled(!wSpecifyFormat.getSelection());
    wlAddDate.setEnabled(!wSpecifyFormat.getSelection());
    wAddTime.setEnabled(!wSpecifyFormat.getSelection());
    wlAddTime.setEnabled(!wSpecifyFormat.getSelection());
  }

  private void setMovedDateTimeFormat() {
    if (wSpecifyMoveFormat.getSelection()) {
      wAddMovedDate.setSelection(false);
      wAddMovedTime.setSelection(false);
    }

    wlMovedDateTimeFormat.setEnabled(wSpecifyMoveFormat.getSelection());
    wMovedDateTimeFormat.setEnabled(wSpecifyMoveFormat.getSelection());
  }

  private void refreshArgFromPrevious() {
    wlFields.setEnabled(!wPrevious.getSelection());
    wFields.setEnabled(!wPrevious.getSelection());
  }

  private void checkIncludeSubFolders() {
    wlMoveEmptyFolders.setEnabled(wIncludeSubfolders.getSelection());
    wMoveEmptyFolders.setEnabled(wIncludeSubfolders.getSelection());
    wlDoNotKeepFolderStructure.setEnabled(wIncludeSubfolders.getSelection());
    wDoNotKeepFolderStructure.setEnabled(wIncludeSubfolders.getSelection());
    if (!wIncludeSubfolders.getSelection()) {
      wDoNotKeepFolderStructure.setSelection(false);
      wMoveEmptyFolders.setSelection(false);
    }
  }

  protected SelectionAdapter getFileSelectionAdapter() {
    return new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent event) {
        try {
          String path = wFields.getActiveTableItem().getText(wFields.getActiveTableColumn());
          FileObject fileObject = HopVfs.getFileObject(path);

          path =
              BaseDialog.presentFileDialog(
                  shell, null, variables, fileObject, new String[] {"*"}, FILETYPES, true);
          if (path != null) {
            wFields.getActiveTableItem().setText(wFields.getActiveTableColumn(), path);
          }
        } catch (HopFileException e) {
          LogChannel.UI.logError("Error selecting file or directory", e);
        }
      }
    };
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wName.setText(Const.NVL(action.getName(), ""));
    wMoveEmptyFolders.setSelection(action.moveEmptyFolders);

    if (action.sourceFileFolder != null) {
      for (int i = 0; i < action.sourceFileFolder.length; i++) {
        TableItem ti = wFields.table.getItem(i);
        if (action.sourceFileFolder[i] != null) {
          ti.setText(1, action.sourceFileFolder[i]);
        }
        if (action.destinationFileFolder[i] != null) {
          ti.setText(2, action.destinationFileFolder[i]);
        }
        if (action.wildcard[i] != null) {
          ti.setText(3, action.wildcard[i]);
        }
      }
      wFields.setRowNums();
      wFields.optWidth(true);
    }
    wPrevious.setSelection(action.argFromPrevious);
    wIncludeSubfolders.setSelection(action.includeSubfolders);
    wDestinationIsAFile.setSelection(action.destinationIsAFile);
    wCreateDestinationFolder.setSelection(action.createDestinationFolder);

    wAddFileToResult.setSelection(action.addResultFilenames);

    wCreateMoveToFolder.setSelection(action.createMoveToFolder);

    if (action.getNrErrorsLessThan() != null) {
      wNrErrorsLessThan.setText(action.getNrErrorsLessThan());
    } else {
      wNrErrorsLessThan.setText("10");
    }

    if (action.getSuccessCondition() != null) {
      if (action
          .getSuccessCondition()
          .equals(ActionMoveFiles.SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED)) {
        wSuccessCondition.select(1);
      } else if (action.getSuccessCondition().equals(ActionMoveFiles.SUCCESS_IF_ERRORS_LESS)) {
        wSuccessCondition.select(2);
      } else {
        wSuccessCondition.select(0);
      }
    } else {
      wSuccessCondition.select(0);
    }

    if (action.getIfFileExists() != null) {
      switch (action.getIfFileExists()) {
        case CONST_OVERWRITE_FILE -> wIfFileExists.select(1);
        case CONST_UNIQUE_NAME -> wIfFileExists.select(2);
        case "delete_file" -> wIfFileExists.select(3);
        case "move_file" -> wIfFileExists.select(4);
        case "fail" -> wIfFileExists.select(5);
        default -> wIfFileExists.select(0);
      }
    } else {
      wIfFileExists.select(0);
    }

    if (action.getDestinationFolder() != null) {
      wDestinationFolder.setText(action.getDestinationFolder());
    }

    if (action.getIfMovedFileExists() != null) {
      switch (action.getIfMovedFileExists()) {
        case CONST_OVERWRITE_FILE -> wIfMovedFileExists.select(1);
        case CONST_UNIQUE_NAME -> wIfMovedFileExists.select(2);
        case "fail" -> wIfMovedFileExists.select(3);
        default -> wIfMovedFileExists.select(0);
      }
    } else {
      wIfMovedFileExists.select(0);
    }
    wDoNotKeepFolderStructure.setSelection(action.isDoNotKeepFolderStructure());
    wAddDateBeforeExtension.setSelection(action.isAddDateBeforeExtension());
    wSimulate.setSelection(action.simulate);

    wAddDate.setSelection(action.isAddDate());
    wAddTime.setSelection(action.isAddTime());
    wSpecifyFormat.setSelection(action.isSpecifyFormat());
    if (action.getDateTimeFormat() != null) {
      wDateTimeFormat.setText(action.getDateTimeFormat());
    }

    wAddMovedDate.setSelection(action.isAddMovedDate());
    wAddMovedTime.setSelection(action.isAddMovedTime());
    wSpecifyMoveFormat.setSelection(action.isSpecifyMoveFormat());
    if (action.getMovedDateTimeFormat() != null) {
      wMovedDateTimeFormat.setText(action.getMovedDateTimeFormat());
    }
    wAddMovedDateBeforeExtension.setSelection(action.isAddMovedDateBeforeExtension());

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    action.setChanged(changed);
    action = null;
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wName.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "System.TransformActionNameMissing.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "System.ActionNameMissing.Msg"));
      mb.open();
      return;
    }
    action.setName(wName.getText());
    action.setMoveEmptyFolders(wMoveEmptyFolders.getSelection());
    action.setIncludeSubfolders(wIncludeSubfolders.getSelection());
    action.setArgFromPrevious(wPrevious.getSelection());
    action.setAddresultfilesname(wAddFileToResult.getSelection());
    action.setDestinationIsAFile(wDestinationIsAFile.getSelection());
    action.setCreateDestinationFolder(wCreateDestinationFolder.getSelection());
    action.setNrErrorsLessThan(wNrErrorsLessThan.getText());

    action.setCreateMoveToFolder(wCreateMoveToFolder.getSelection());

    if (wSuccessCondition.getSelectionIndex() == 1) {
      action.setSuccessCondition(ActionMoveFiles.SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED);
    } else if (wSuccessCondition.getSelectionIndex() == 2) {
      action.setSuccessCondition(ActionMoveFiles.SUCCESS_IF_ERRORS_LESS);
    } else {
      action.setSuccessCondition(ActionMoveFiles.SUCCESS_IF_NO_ERRORS);
    }

    if (wIfFileExists.getSelectionIndex() == 1) {
      action.setIfFileExists(CONST_OVERWRITE_FILE);
    } else if (wIfFileExists.getSelectionIndex() == 2) {
      action.setIfFileExists(CONST_UNIQUE_NAME);
    } else if (wIfFileExists.getSelectionIndex() == 3) {
      action.setIfFileExists("delete_file");
    } else if (wIfFileExists.getSelectionIndex() == 4) {
      action.setIfFileExists("move_file");
    } else if (wIfFileExists.getSelectionIndex() == 5) {
      action.setIfFileExists("fail");
    } else {
      action.setIfFileExists("do_nothing");
    }

    action.setDestinationFolder(wDestinationFolder.getText());

    if (wIfMovedFileExists.getSelectionIndex() == 1) {
      action.setIfMovedFileExists(CONST_OVERWRITE_FILE);
    } else if (wIfMovedFileExists.getSelectionIndex() == 2) {
      action.setIfMovedFileExists(CONST_UNIQUE_NAME);
    } else if (wIfMovedFileExists.getSelectionIndex() == 3) {
      action.setIfMovedFileExists("fail");
    } else {
      action.setIfMovedFileExists("do_nothing");
    }

    action.setDoNotKeepFolderStructure(wDoNotKeepFolderStructure.getSelection());
    action.setSimulate(wSimulate.getSelection());

    action.setAddDate(wAddDate.getSelection());
    action.setAddTime(wAddTime.getSelection());
    action.setSpecifyFormat(wSpecifyFormat.getSelection());
    action.setDateTimeFormat(wDateTimeFormat.getText());
    action.setAddDateBeforeExtension(wAddDateBeforeExtension.getSelection());

    action.setAddMovedDate(wAddMovedDate.getSelection());
    action.setAddMovedTime(wAddMovedTime.getSelection());
    action.setSpecifyMoveFormat(wSpecifyMoveFormat.getSelection());
    action.setMovedDateTimeFormat(wMovedDateTimeFormat.getText());
    action.setAddMovedDateBeforeExtension(wAddMovedDateBeforeExtension.getSelection());

    int nrItems = wFields.nrNonEmpty();
    int nr = 0;
    for (int i = 0; i < nrItems; i++) {
      String arg = wFields.getNonEmpty(i).getText(1);
      if (arg != null && !arg.isEmpty()) {
        nr++;
      }
    }
    action.sourceFileFolder = new String[nr];
    action.destinationFileFolder = new String[nr];
    action.wildcard = new String[nr];
    nr = 0;
    for (int i = 0; i < nrItems; i++) {
      String source = wFields.getNonEmpty(i).getText(1);
      String dest = wFields.getNonEmpty(i).getText(2);
      String wild = wFields.getNonEmpty(i).getText(3);
      if (source != null && !source.isEmpty()) {
        action.sourceFileFolder[nr] = source;
        action.destinationFileFolder[nr] = dest;
        action.wildcard[nr] = wild;
        nr++;
      }
    }
    dispose();
  }
}
