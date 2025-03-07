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

package org.apache.hop.pipeline.transforms.xml.addxml;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class AddXmlDialog extends BaseTransformDialog {
  private static final Class<?> PKG = AddXmlMeta.class;
  public static final String CONST_SYSTEM_COMBO_YES = "System.Combo.Yes";

  private Button wOmitXMLHeader;

  private Button wOmitNullValues;

  private CCombo wEncoding;

  private CCombo wOutputValue;

  private CCombo wRepeatElement;

  private TableView wFields;

  private final AddXmlMeta input;

  private boolean gotEncodings = false;

  private ColumnInfo[] colinf;

  private final Map<String, Integer> inputFields;

  public AddXmlDialog(
      Shell parent, IVariables variables, AddXmlMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
    inputFields = new HashMap<>();
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
    shell.setText(BaseMessages.getString(PKG, "AddXMLDialog.DialogTitle"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.TransformName.Label"));
    wlTransformName.setToolTipText(BaseMessages.getString(PKG, "System.TransformName.Tooltip"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.top = new FormAttachment(0, margin);
    fdlTransformName.right = new FormAttachment(middle, -margin);
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

    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF CONTENT TAB///
    // /
    CTabItem wContentTab = new CTabItem(wTabFolder, SWT.NONE);
    wContentTab.setFont(GuiResource.getInstance().getFontDefault());
    wContentTab.setText(BaseMessages.getString(PKG, "AddXMLDialog.ContentTab.TabTitle"));

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    Composite wContentComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wContentComp);
    wContentComp.setLayout(contentLayout);

    Label wlEncoding = new Label(wContentComp, SWT.RIGHT);
    wlEncoding.setText(BaseMessages.getString(PKG, "AddXMLDialog.Encoding.Label"));
    PropsUi.setLook(wlEncoding);
    FormData fdlEncoding = new FormData();
    fdlEncoding.left = new FormAttachment(0, 0);
    fdlEncoding.top = new FormAttachment(null, margin);
    fdlEncoding.right = new FormAttachment(middle, -margin);
    wlEncoding.setLayoutData(fdlEncoding);
    wEncoding = new CCombo(wContentComp, SWT.BORDER | SWT.READ_ONLY);
    wEncoding.setEditable(true);
    PropsUi.setLook(wEncoding);
    wEncoding.addModifyListener(lsMod);
    FormData fdEncoding = new FormData();
    fdEncoding.left = new FormAttachment(middle, 0);
    fdEncoding.top = new FormAttachment(null, margin);
    fdEncoding.right = new FormAttachment(100, 0);
    wEncoding.setLayoutData(fdEncoding);
    wEncoding.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // Do Nothing
          }

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
            shell.setCursor(busy);
            setEncodings();
            shell.setCursor(null);
            busy.dispose();
          }
        });

    Label wlOutputValue = new Label(wContentComp, SWT.RIGHT);
    wlOutputValue.setText(BaseMessages.getString(PKG, "AddXMLDialog.OutputValue.Label"));
    PropsUi.setLook(wlOutputValue);
    FormData fdlOutputValue = new FormData();
    fdlOutputValue.left = new FormAttachment(0, 0);
    fdlOutputValue.top = new FormAttachment(wEncoding, margin);
    fdlOutputValue.right = new FormAttachment(middle, -margin);
    wlOutputValue.setLayoutData(fdlOutputValue);
    wOutputValue = new CCombo(wContentComp, SWT.BORDER | SWT.READ_ONLY);
    wOutputValue.setEditable(true);
    PropsUi.setLook(wOutputValue);
    wOutputValue.addModifyListener(lsMod);
    FormData fdOutputValue = new FormData();
    fdOutputValue.left = new FormAttachment(middle, 0);
    fdOutputValue.top = new FormAttachment(wEncoding, margin);
    fdOutputValue.right = new FormAttachment(100, 0);
    wOutputValue.setLayoutData(fdOutputValue);

    Label wlRepeatElement = new Label(wContentComp, SWT.RIGHT);
    wlRepeatElement.setText(BaseMessages.getString(PKG, "AddXMLDialog.RepeatElement.Label"));
    PropsUi.setLook(wlRepeatElement);
    FormData fdlRepeatElement = new FormData();
    fdlRepeatElement.left = new FormAttachment(0, 0);
    fdlRepeatElement.top = new FormAttachment(wOutputValue, margin);
    fdlRepeatElement.right = new FormAttachment(middle, -margin);
    wlRepeatElement.setLayoutData(fdlRepeatElement);
    wRepeatElement = new CCombo(wContentComp, SWT.BORDER | SWT.READ_ONLY);
    wRepeatElement.setEditable(true);
    PropsUi.setLook(wRepeatElement);
    wRepeatElement.addModifyListener(lsMod);
    FormData fdRepeatElement = new FormData();
    fdRepeatElement.left = new FormAttachment(middle, 0);
    fdRepeatElement.top = new FormAttachment(wOutputValue, margin);
    fdRepeatElement.right = new FormAttachment(100, 0);
    wRepeatElement.setLayoutData(fdRepeatElement);

    Label wlOmitXMLHeader = new Label(wContentComp, SWT.RIGHT);
    wlOmitXMLHeader.setText(BaseMessages.getString(PKG, "AddXMLDialog.OmitXMLHeader.Label"));
    PropsUi.setLook(wlOmitXMLHeader);
    FormData fdlOmitXMLHeader = new FormData();
    fdlOmitXMLHeader.left = new FormAttachment(0, 0);
    fdlOmitXMLHeader.top = new FormAttachment(wRepeatElement, margin);
    fdlOmitXMLHeader.right = new FormAttachment(middle, -margin);
    wlOmitXMLHeader.setLayoutData(fdlOmitXMLHeader);
    wOmitXMLHeader = new Button(wContentComp, SWT.CHECK);
    PropsUi.setLook(wOmitXMLHeader);
    FormData fdOmitXMLHeader = new FormData();
    fdOmitXMLHeader.left = new FormAttachment(middle, 0);
    fdOmitXMLHeader.top = new FormAttachment(wlOmitXMLHeader, 0, SWT.CENTER);
    fdOmitXMLHeader.right = new FormAttachment(100, 0);
    wOmitXMLHeader.setLayoutData(fdOmitXMLHeader);
    wOmitXMLHeader.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    Label wlOmitNullValues = new Label(wContentComp, SWT.RIGHT);
    wlOmitNullValues.setText(BaseMessages.getString(PKG, "AddXMLDialog.OmitNullValues.Label"));
    PropsUi.setLook(wlOmitNullValues);
    FormData fdlOmitNullValues = new FormData();
    fdlOmitNullValues.left = new FormAttachment(0, 0);
    fdlOmitNullValues.top = new FormAttachment(wOmitXMLHeader, margin);
    fdlOmitNullValues.right = new FormAttachment(middle, -margin);
    wlOmitNullValues.setLayoutData(fdlOmitNullValues);
    wOmitNullValues = new Button(wContentComp, SWT.CHECK);
    PropsUi.setLook(wOmitNullValues);
    FormData fdOmitNullValues = new FormData();
    fdOmitNullValues.left = new FormAttachment(middle, 0);
    fdOmitNullValues.top = new FormAttachment(wlOmitNullValues, 0, SWT.CENTER);
    fdOmitNullValues.right = new FormAttachment(100, 0);
    wOmitNullValues.setLayoutData(fdOmitNullValues);
    wOmitNullValues.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    FormData fdContentComp = new FormData();
    fdContentComp.left = new FormAttachment(0, 0);
    fdContentComp.top = new FormAttachment(0, 0);
    fdContentComp.right = new FormAttachment(100, 0);
    fdContentComp.bottom = new FormAttachment(100, 0);
    wContentComp.setLayoutData(fdContentComp);

    wContentComp.layout();
    wContentTab.setControl(wContentComp);

    // ///////////////////////////////////////////////////////////
    // / END OF CONTENT TAB
    // ///////////////////////////////////////////////////////////

    // Fields tab...
    //
    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setFont(GuiResource.getInstance().getFontDefault());
    wFieldsTab.setText(BaseMessages.getString(PKG, "AddXMLDialog.FieldsTab.TabTitle"));

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = PropsUi.getFormMargin();
    fieldsLayout.marginHeight = PropsUi.getFormMargin();

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    wFieldsComp.setLayout(fieldsLayout);
    PropsUi.setLook(wFieldsComp);

    wGet = new Button(wFieldsComp, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "AddXMLDialog.Get.Button"));
    wGet.setToolTipText(BaseMessages.getString(PKG, "AddXMLDialog.Get.Tooltip"));

    Button wMinWidth = new Button(wFieldsComp, SWT.PUSH);
    wMinWidth.setText(BaseMessages.getString(PKG, "AddXMLDialog.MinWidth.Label"));
    wMinWidth.setToolTipText(BaseMessages.getString(PKG, "AddXMLDialog.MinWidth.Tooltip"));

    setButtonPositions(new Button[] {wGet, wMinWidth}, margin, null);

    final int FieldsRows = input.getOutputFields().length;

    colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "AddXMLDialog.Fieldname.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "AddXMLDialog.ElementName.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "AddXMLDialog.Type.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaBase.getTypes()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "AddXMLDialog.Format.Column"),
              ColumnInfo.COLUMN_TYPE_FORMAT,
              3),
          new ColumnInfo(
              BaseMessages.getString(PKG, "AddXMLDialog.Length.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "AddXMLDialog.Precision.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "AddXMLDialog.Currency.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "AddXMLDialog.Decimal.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "AddXMLDialog.Group.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "AddXMLDialog.Null.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "AddXMLDialog.Attribute.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {
                BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES),
                BaseMessages.getString(PKG, "System.Combo.No")
              },
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "AddXMLDialog.AttributeParentName.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false)
        };
    wFields =
        new TableView(
            variables,
            wFieldsComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            FieldsRows,
            lsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(0, 0);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wGet, -margin);
    wFields.setLayoutData(fdFields);

    //
    // Search the fields in the background

    final Runnable runnable =
        () -> {
          TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
          if (transformMeta != null) {
            try {
              IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);

              // Remember these fields...
              for (int i = 0; i < row.size(); i++) {
                inputFields.put(row.getValueMeta(i).getName(), Integer.valueOf(i));
              }
              setComboBoxes();
            } catch (HopException e) {
              logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
            }
          }
        };
    new Thread(runnable).start();

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment(0, 0);
    fdFieldsComp.top = new FormAttachment(0, 0);
    fdFieldsComp.right = new FormAttachment(100, 0);
    fdFieldsComp.bottom = new FormAttachment(100, 0);
    wFieldsComp.setLayoutData(fdFieldsComp);

    wFieldsComp.layout();
    wFieldsTab.setControl(wFieldsComp);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wTransformName, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fdTabFolder);

    // Add listeners
    wGet.addListener(SWT.Selection, e -> get());
    wMinWidth.addListener(SWT.Selection, e -> setMinimalWidth());

    lsResize =
        event -> {
          Point size = shell.getSize();
          wFields.setSize(size.x - 10, size.y - 50);
          wFields.table.setSize(size.x - 10, size.y - 50);
          wFields.redraw();
        };
    shell.addListener(SWT.Resize, lsResize);

    wTabFolder.setSelection(0);

    getData();
    input.setChanged(changed);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<>();

    // Add the currentMeta fields...
    fields.putAll(inputFields);

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>(keySet);

    String[] fieldNames = entries.toArray(new String[entries.size()]);

    Const.sortStrings(fieldNames);
    colinf[0].setComboValues(fieldNames);
  }

  private void setEncodings() {
    // Encoding of the text file:
    if (!gotEncodings) {
      gotEncodings = true;

      wEncoding.removeAll();
      List<Charset> values = new ArrayList<>(Charset.availableCharsets().values());
      for (Charset charSet : values) {
        wEncoding.add(charSet.displayName());
      }

      // Now select the default!
      String defEncoding = Const.getEnvironmentVariable("file.encoding", "UTF-8");
      int idx = Const.indexOfString(defEncoding, wEncoding.getItems());
      if (idx >= 0) {
        wEncoding.select(idx);
      } else {
        wEncoding.select(Const.indexOfString("UTF-8", wEncoding.getItems()));
      }
    }
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (input.getEncoding() != null) {
      wEncoding.setText(input.getEncoding());
    }
    if (input.getValueName() != null) {
      wOutputValue.setText(input.getValueName());
    }
    if (input.getRootNode() != null) {
      wRepeatElement.setText(input.getRootNode());
    }

    wOmitXMLHeader.setSelection(input.isOmitXMLheader());
    wOmitNullValues.setSelection(input.isOmitNullValues());

    logDebug(BaseMessages.getString(PKG, "AddXMLDialog.Log.GettingFieldsInfo"));

    for (int i = 0; i < input.getOutputFields().length; i++) {
      XmlField field = input.getOutputFields()[i];

      TableItem item = wFields.table.getItem(i);
      if (field.getFieldName() != null) {
        item.setText(1, field.getFieldName());
      }
      if (field.getElementName() != null) {
        item.setText(2, field.getElementName());
      }
      item.setText(3, field.getTypeDesc());
      if (field.getFormat() != null) {
        item.setText(4, field.getFormat());
      }
      if (field.getLength() >= 0) {
        item.setText(5, "" + field.getLength());
      }
      if (field.getPrecision() >= 0) {
        item.setText(6, "" + field.getPrecision());
      }
      if (field.getCurrencySymbol() != null) {
        item.setText(7, field.getCurrencySymbol());
      }
      if (field.getDecimalSymbol() != null) {
        item.setText(8, field.getDecimalSymbol());
      }
      if (field.getGroupingSymbol() != null) {
        item.setText(9, field.getGroupingSymbol());
      }
      if (field.getNullString() != null) {
        item.setText(10, field.getNullString());
      }
      item.setText(
          11,
          field.isAttribute()
              ? BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES)
              : BaseMessages.getString(PKG, "System.Combo.No"));
      if (field.getAttributeParentName() != null) {
        item.setText(12, field.getAttributeParentName());
      }
    }

    wFields.optWidth(true);

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;

    input.setChanged(backupChanged);

    dispose();
  }

  private void getInfo(AddXmlMeta tfoi) {
    tfoi.setEncoding(wEncoding.getText());
    tfoi.setValueName(wOutputValue.getText());
    tfoi.setRootNode(wRepeatElement.getText());

    tfoi.setOmitXMLheader(wOmitXMLHeader.getSelection());
    tfoi.setOmitNullValues(wOmitNullValues.getSelection());

    int nrFields = wFields.nrNonEmpty();

    tfoi.allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      XmlField field = new XmlField();

      TableItem item = wFields.getNonEmpty(i);
      field.setFieldName(item.getText(1));
      field.setElementName(item.getText(2));

      if (field.getFieldName().equals(field.getElementName())) {
        field.setElementName("");
      }

      field.setType(item.getText(3));
      field.setFormat(item.getText(4));
      field.setLength(Const.toInt(item.getText(5), -1));
      field.setPrecision(Const.toInt(item.getText(6), -1));
      field.setCurrencySymbol(item.getText(7));
      field.setDecimalSymbol(item.getText(8));
      field.setGroupingSymbol(item.getText(9));
      field.setNullString(item.getText(10));
      field.setAttribute(
          BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES).equals(item.getText(11)));
      field.setAttributeParentName(item.getText(12));

      tfoi.getOutputFields()[i] = field;
    }
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    getInfo(input);

    dispose();
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        BaseTransformDialog.getFieldsFromPrevious(
            r,
            wFields,
            1,
            new int[] {1, 2},
            new int[] {3},
            5,
            6,
            (tableItem, v) -> {
              if (v.isNumber() && v.getLength() > 0) {
                int le = v.getLength();
                int pr = v.getPrecision();

                if (v.getPrecision() <= 0) {
                  pr = 0;
                }

                String mask = " ";
                for (int m = 0; m < le - pr; m++) {
                  mask += "0";
                }
                if (pr > 0) {
                  mask += ".";
                }
                for (int m = 0; m < pr; m++) {
                  mask += "0";
                }
                tableItem.setText(4, mask);
              }
              return true;
            });
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Title"),
          BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"),
          ke);
    }
  }

  /** Sets the output width to minimal width... */
  public void setMinimalWidth() {
    int nrNonEmptyFields = wFields.nrNonEmpty();
    for (int i = 0; i < nrNonEmptyFields; i++) {
      TableItem item = wFields.getNonEmpty(i);

      item.setText(5, "");
      item.setText(6, "");

      int type = ValueMetaBase.getType(item.getText(2));
      switch (type) {
        case IValueMeta.TYPE_STRING:
          item.setText(4, "");
          break;
        case IValueMeta.TYPE_INTEGER:
          item.setText(4, "0");
          break;
        case IValueMeta.TYPE_NUMBER:
          item.setText(4, "0.#####");
          break;
        case IValueMeta.TYPE_DATE:
          break;
        default:
          break;
      }
    }
    wFields.optWidth(true);
  }
}
