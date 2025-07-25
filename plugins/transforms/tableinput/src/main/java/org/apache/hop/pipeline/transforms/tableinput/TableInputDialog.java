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

package org.apache.hop.pipeline.transforms.tableinput;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.SQLStyledTextComp;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TextComposite;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class TableInputDialog extends BaseTransformDialog {
  private static final Class<?> PKG = TableInputMeta.class;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private TextComposite wSql;

  private CCombo wDataFrom;

  private TextVar wLimit;

  private Label wlEachRow;
  private Button wEachRow;

  private Button wVariables;

  private final TableInputMeta input;

  private Label wlPosition;

  public TableInputDialog(
      Shell parent, IVariables variables, TableInputMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "TableInputDialog.TableInput"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "TableInputDialog.TransformName"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    wConnection = addConnectionLine(shell, wTransformName, input.getConnection(), lsMod);
    wConnection.addListener(SWT.Selection, e -> getSqlReservedWords());

    // Some buttons
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wPreview = new Button(shell, SWT.PUSH);
    wPreview.setText(BaseMessages.getString(PKG, "System.Button.Preview"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    setButtonPositions(new Button[] {wOk, wPreview, wCancel}, margin, null);

    // Limit input ...
    Label wlLimit = new Label(shell, SWT.RIGHT);
    wlLimit.setText(BaseMessages.getString(PKG, "TableInputDialog.LimitSize"));
    PropsUi.setLook(wlLimit);
    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment(0, 0);
    fdlLimit.right = new FormAttachment(middle, -margin);
    fdlLimit.bottom = new FormAttachment(wOk, -2 * margin);
    wlLimit.setLayoutData(fdlLimit);
    wLimit = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wLimit);
    wLimit.addModifyListener(lsMod);
    FormData fdLimit = new FormData();
    fdLimit.left = new FormAttachment(middle, 0);
    fdLimit.right = new FormAttachment(100, 0);
    fdLimit.bottom = new FormAttachment(wlLimit, 0, SWT.CENTER);
    wLimit.setLayoutData(fdLimit);

    // Execute for each row?
    wlEachRow = new Label(shell, SWT.RIGHT);
    wlEachRow.setText(BaseMessages.getString(PKG, "TableInputDialog.ExecuteForEachRow"));
    PropsUi.setLook(wlEachRow);
    FormData fdlEachRow = new FormData();
    fdlEachRow.left = new FormAttachment(0, 0);
    fdlEachRow.right = new FormAttachment(middle, -margin);
    fdlEachRow.bottom = new FormAttachment(wlLimit, -margin);
    wlEachRow.setLayoutData(fdlEachRow);
    wEachRow = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wEachRow);
    FormData fdEachRow = new FormData();
    fdEachRow.left = new FormAttachment(middle, 0);
    fdEachRow.right = new FormAttachment(100, 0);
    fdEachRow.bottom = new FormAttachment(wlEachRow, 0, SWT.CENTER);
    wEachRow.setLayoutData(fdEachRow);
    SelectionAdapter lsSelMod =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
          }
        };
    wEachRow.addSelectionListener(lsSelMod);

    // Read date from...
    Label wlDatefrom = new Label(shell, SWT.RIGHT);
    wlDatefrom.setText(BaseMessages.getString(PKG, "TableInputDialog.InsertDataFromTransform"));
    PropsUi.setLook(wlDatefrom);
    FormData fdlDatefrom = new FormData();
    fdlDatefrom.left = new FormAttachment(0, 0);
    fdlDatefrom.right = new FormAttachment(middle, -margin);
    fdlDatefrom.bottom = new FormAttachment(wlEachRow, -margin);
    wlDatefrom.setLayoutData(fdlDatefrom);
    wDataFrom = new CCombo(shell, SWT.BORDER);
    PropsUi.setLook(wDataFrom);

    List<TransformMeta> previousTransforms =
        pipelineMeta.findPreviousTransforms(pipelineMeta.findTransform(transformName));
    for (TransformMeta transformMeta : previousTransforms) {
      wDataFrom.add(transformMeta.getName());
    }

    wDataFrom.addModifyListener(lsMod);
    FormData fdDatefrom = new FormData();
    fdDatefrom.left = new FormAttachment(middle, 0);
    fdDatefrom.right = new FormAttachment(100, 0);
    fdDatefrom.bottom = new FormAttachment(wlDatefrom, 0, SWT.CENTER);
    wDataFrom.setLayoutData(fdDatefrom);

    // Replace variables in SQL?
    //
    Label wlVariables = new Label(shell, SWT.RIGHT);
    wlVariables.setText(BaseMessages.getString(PKG, "TableInputDialog.ReplaceVariables"));
    PropsUi.setLook(wlVariables);
    FormData fdlVariables = new FormData();
    fdlVariables.left = new FormAttachment(0, 0);
    fdlVariables.right = new FormAttachment(middle, -margin);
    fdlVariables.bottom = new FormAttachment(wlDatefrom, -margin);
    wlVariables.setLayoutData(fdlVariables);
    wVariables = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wVariables);
    FormData fdVariables = new FormData();
    fdVariables.left = new FormAttachment(middle, 0);
    fdVariables.right = new FormAttachment(100, 0);
    fdVariables.bottom = new FormAttachment(wlVariables, 0, SWT.CENTER);
    wVariables.setLayoutData(fdVariables);
    wVariables.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
            setSqlToolTip();
          }
        });

    wlPosition = new Label(shell, SWT.NONE);
    PropsUi.setLook(wlPosition);
    FormData fdlPosition = new FormData();
    fdlPosition.left = new FormAttachment(0, 0);
    fdlPosition.right = new FormAttachment(100, 0);
    fdlPosition.bottom = new FormAttachment(wlVariables, -2 * margin);
    wlPosition.setLayoutData(fdlPosition);

    // Table line...
    Label wlSql = new Label(shell, SWT.NONE);
    wlSql.setText(BaseMessages.getString(PKG, "TableInputDialog.SQL"));
    PropsUi.setLook(wlSql);
    FormData fdlSql = new FormData();
    fdlSql.left = new FormAttachment(0, 0);
    fdlSql.top = new FormAttachment(wConnection, margin * 2);
    wlSql.setLayoutData(fdlSql);

    Button wbTable = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTable);
    wbTable.setText(BaseMessages.getString(PKG, "TableInputDialog.GetSQLAndSelectStatement"));
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment(100, 0);
    fdbTable.top = new FormAttachment(wConnection, margin * 2);
    wbTable.setLayoutData(fdbTable);

    if (EnvironmentUtils.getInstance().isWeb()) {
      wSql =
          new StyledTextComp(
              variables, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    } else {
      wSql =
          new SQLStyledTextComp(
              variables, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    }
    PropsUi.setLook(wSql, Props.WIDGET_STYLE_FIXED);
    wSql.addModifyListener(lsMod);
    FormData fdSql = new FormData();
    fdSql.left = new FormAttachment(0, 0);
    fdSql.top = new FormAttachment(wbTable, margin);
    fdSql.right = new FormAttachment(100, -margin);
    fdSql.bottom = new FormAttachment(wlPosition, -margin);
    wSql.setLayoutData(fdSql);
    wSql.addModifyListener(
        arg0 -> {
          setSqlToolTip();
          setPosition();
        });

    wSql.addKeyListener(
        new KeyAdapter() {
          @Override
          public void keyPressed(KeyEvent e) {
            setPosition();
          }

          @Override
          public void keyReleased(KeyEvent e) {
            setPosition();
          }
        });
    wSql.addFocusListener(
        new FocusAdapter() {
          @Override
          public void focusGained(FocusEvent e) {
            setPosition();
          }

          @Override
          public void focusLost(FocusEvent e) {
            setPosition();
          }
        });
    wSql.addMouseListener(
        new MouseAdapter() {
          @Override
          public void mouseDoubleClick(MouseEvent e) {
            setPosition();
          }

          @Override
          public void mouseDown(MouseEvent e) {
            setPosition();
          }

          @Override
          public void mouseUp(MouseEvent e) {
            setPosition();
          }
        });

    // Add listeners
    wCancel.addListener(SWT.Selection, e -> cancel());
    wPreview.addListener(SWT.Selection, e -> preview());
    wOk.addListener(SWT.Selection, e -> ok());
    wbTable.addListener(SWT.Selection, e -> getSql());
    wDataFrom.addListener(SWT.Selection, e -> setFlags());
    wDataFrom.addListener(SWT.FocusOut, e -> setFlags());

    final List<String> sqlKeywords = getSqlReservedWords();

    wSql.addLineStyleListener(sqlKeywords);
    getData();
    input.setChanged(changed);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private List<String> getSqlReservedWords() {
    // Do not search keywords when connection is empty
    if (input.getConnection() == null || input.getConnection().isEmpty()) {
      return List.of();
    }

    // If connection is a variable that can't be resolved
    if (variables.resolve(input.getConnection()).startsWith("${")) {
      return List.of();
    }

    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(input.getConnection(), variables);
    if (databaseMeta != null) {
      return Arrays.stream(databaseMeta.getReservedWords()).toList();
    } else {
      return Collections.emptyList();
    }
  }

  public void setPosition() {
    int lineNumber = wSql.getLineNumber();
    int columnNumber = wSql.getColumnNumber();
    wlPosition.setText(
        BaseMessages.getString(
            PKG, "TableInputDialog.Position.Label", "" + lineNumber, "" + columnNumber));
  }

  protected void setSqlToolTip() {
    if (wVariables.getSelection()) {
      wSql.setToolTipText(variables.resolve(wSql.getText()));
    }
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {

    if (input.getSql() != null) {
      wSql.setText(input.getSql());
    }
    if (input.getConnection() != null) {
      wConnection.setText(input.getConnection());
    }

    wLimit.setText(Const.NVL(input.getRowLimit(), ""));
    wDataFrom.setText(Const.NVL(input.getLookup(), ""));
    wEachRow.setSelection(input.isExecuteEachInputRow());
    wVariables.setSelection(input.isVariableReplacementActive());

    setSqlToolTip();
    setFlags();

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void getInfo(TableInputMeta meta, boolean preview) {

    meta.setConnection(wConnection.getText());

    meta.setSql(
        preview && !Utils.isEmpty(wSql.getSelectionText())
            ? wSql.getSelectionText()
            : wSql.getText());

    meta.setRowLimit(wLimit.getText());
    meta.setExecuteEachInputRow(wEachRow.getSelection());
    meta.setVariableReplacementActive(wVariables.getSelection());
    meta.setLookup(wDataFrom.getText());

    // Force recreate TransformIOMeta and update info stream
    meta.resetTransformIoMeta();
    meta.searchInfoAndTargetTransforms(pipelineMeta.getTransforms());
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value
    if (Utils.isEmpty(wConnection.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG, "TableInputDialog.SelectValidConnection"));
      mb.setText(BaseMessages.getString(PKG, "TableInputDialog.DialogCaptionError"));
      mb.open();
      return;
    }

    getInfo(input, false);
    dispose();
  }

  private void getSql() {
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(wConnection.getText(), variables);
    if (databaseMeta != null) {
      DatabaseExplorerDialog std =
          new DatabaseExplorerDialog(
              shell, SWT.NONE, variables, databaseMeta, pipelineMeta.getDatabases(), false, true);
      if (std.open()) {
        String sql =
            "SELECT *"
                + Const.CR
                + "FROM "
                + databaseMeta.getQuotedSchemaTableCombination(
                    variables, std.getSchemaName(), std.getTableName())
                + Const.CR;
        wSql.setText(sql);

        MessageBox yn = new MessageBox(shell, SWT.YES | SWT.NO | SWT.CANCEL | SWT.ICON_QUESTION);
        yn.setMessage(BaseMessages.getString(PKG, "TableInputDialog.IncludeFieldNamesInSQL"));
        yn.setText(BaseMessages.getString(PKG, "TableInputDialog.DialogCaptionQuestion"));
        int id = yn.open();
        switch (id) {
          case SWT.CANCEL:
            break;
          case SWT.NO:
            wSql.setText(sql);
            break;
          case SWT.YES:
            Database db = new Database(loggingObject, variables, databaseMeta);
            try {
              db.connect();
              IRowMeta fields = db.getQueryFields(sql, false);
              if (fields != null) {
                sql = "SELECT" + Const.CR;
                for (int i = 0; i < fields.size(); i++) {
                  IValueMeta field = fields.getValueMeta(i);
                  if (i == 0) {
                    sql += "  ";
                  } else {
                    sql += ", ";
                  }
                  sql += databaseMeta.quoteField(field.getName()) + Const.CR;
                }
                sql +=
                    "FROM "
                        + databaseMeta.getQuotedSchemaTableCombination(
                            variables, std.getSchemaName(), std.getTableName())
                        + Const.CR;
                wSql.setText(sql);
              } else {
                MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
                mb.setMessage(
                    BaseMessages.getString(PKG, "TableInputDialog.ERROR_CouldNotRetrieveFields")
                        + Const.CR
                        + BaseMessages.getString(PKG, "TableInputDialog.PerhapsNoPermissions"));
                mb.setText(BaseMessages.getString(PKG, "TableInputDialog.DialogCaptionError2"));
                mb.open();
              }
            } catch (HopException e) {
              MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
              mb.setText(BaseMessages.getString(PKG, "TableInputDialog.DialogCaptionError3"));
              mb.setMessage(
                  BaseMessages.getString(PKG, "TableInputDialog.AnErrorOccurred")
                      + Const.CR
                      + e.getMessage());
              mb.open();
            } finally {
              db.disconnect();
            }
            break;
          default:
            break;
        }
      }
    } else {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG, "TableInputDialog.ConnectionNoLongerAvailable"));
      mb.setText(BaseMessages.getString(PKG, "TableInputDialog.DialogCaptionError4"));
      mb.open();
    }
  }

  private void setFlags() {
    if (!Utils.isEmpty(wDataFrom.getText())) {
      // The foreach check box...
      wEachRow.setEnabled(true);
      wlEachRow.setEnabled(true);

      // The preview button...
      wPreview.setEnabled(false);
    } else {
      // The foreach check box...
      wEachRow.setEnabled(false);
      wEachRow.setSelection(false);
      wlEachRow.setEnabled(false);

      // The preview button...
      wPreview.setEnabled(true);
    }
  }

  /**
   * Preview the data generated by this transform. This generates a pipeline using this transform &
   * a dummy and previews it.
   */
  private void preview() {
    // Create the table input reader transform...
    TableInputMeta oneMeta = new TableInputMeta();
    getInfo(oneMeta, true);

    EnterNumberDialog numberDialog =
        new EnterNumberDialog(
            shell,
            props.getDefaultPreviewSize(),
            BaseMessages.getString(PKG, "TableInputDialog.EnterPreviewSize"),
            BaseMessages.getString(PKG, "TableInputDialog.NumberOfRowsToPreview"));
    int previewSize = numberDialog.open();
    if (previewSize > 0) {
      oneMeta.setRowLimit(Integer.toString(previewSize));
      PipelineMeta previewMeta =
          PipelinePreviewFactory.generatePreviewPipeline(
              pipelineMeta.getMetadataProvider(), oneMeta, wTransformName.getText());

      PipelinePreviewProgressDialog progressDialog =
          new PipelinePreviewProgressDialog(
              shell,
              variables,
              previewMeta,
              new String[] {wTransformName.getText()},
              new int[] {previewSize});
      progressDialog.open();

      Pipeline pipeline = progressDialog.getPipeline();
      String loggingText = progressDialog.getLoggingText();

      if (!progressDialog.isCancelled()) {
        if (pipeline.getResult() != null && pipeline.getResult().getNrErrors() > 0) {
          EnterTextDialog etd =
              new EnterTextDialog(
                  shell,
                  BaseMessages.getString(PKG, "System.Dialog.PreviewError.Title"),
                  BaseMessages.getString(PKG, "System.Dialog.PreviewError.Message"),
                  loggingText,
                  true);
          etd.setReadOnly();
          etd.open();
        } else {
          PreviewRowsDialog prd =
              new PreviewRowsDialog(
                  shell,
                  variables,
                  SWT.NONE,
                  wTransformName.getText(),
                  progressDialog.getPreviewRowsMeta(wTransformName.getText()),
                  progressDialog.getPreviewRows(wTransformName.getText()),
                  loggingText);
          prd.open();
        }
      }
    }
  }
}
