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

package org.apache.hop.ui.hopgui.file.pipeline;

import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.IEngineMeta;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.Props;
import org.apache.hop.core.SwtUniversalImage;
import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.action.GuiContextActionFilter;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopMissingPluginsException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.AreaOwner.AreaType;
import org.apache.hop.core.gui.BasePainter;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.gui.IRedrawable;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.Rectangle;
import org.apache.hop.core.gui.SnapAllignDistribute;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.IGuiActionLambda;
import org.apache.hop.core.gui.plugin.IGuiRefresher;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementType;
import org.apache.hop.core.logging.DefaultLogLevel;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.IHasLogChannel;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILogParentProvided;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.core.util.ExecutorUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.ExecutionType;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.history.AuditManager;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.laf.BasePropertyHandler;
import org.apache.hop.lineage.PipelineDataLineage;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePainter;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.debug.PipelineDebugMeta;
import org.apache.hop.pipeline.debug.TransformDebugMeta;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineRunConfiguration;
import org.apache.hop.pipeline.engines.local.LocalPipelineRunConfiguration.SampleType;
import org.apache.hop.pipeline.transform.IRowDistribution;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.RowDistributionPluginType;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transform.stream.IStream.StreamType;
import org.apache.hop.pipeline.transform.stream.Stream;
import org.apache.hop.pipeline.transform.stream.StreamIcon;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ContextDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.EnterStringDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.MessageDialogWithToggle;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.dialog.ProgressMonitorDialog;
import org.apache.hop.ui.core.dialog.TransformFieldsDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.core.widget.OsHelper;
import org.apache.hop.ui.hopgui.CanvasFacade;
import org.apache.hop.ui.hopgui.CanvasListener;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiExtensionPoint;
import org.apache.hop.ui.hopgui.ServerPushSessionFacade;
import org.apache.hop.ui.hopgui.context.GuiContextUtil;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.delegates.HopGuiServerDelegate;
import org.apache.hop.ui.hopgui.dialog.EnterPreviewRowsDialog;
import org.apache.hop.ui.hopgui.dialog.NotePadDialog;
import org.apache.hop.ui.hopgui.dialog.SearchFieldsProgressDialog;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.delegates.HopGuiNotePadDelegate;
import org.apache.hop.ui.hopgui.file.pipeline.context.HopGuiPipelineContext;
import org.apache.hop.ui.hopgui.file.pipeline.context.HopGuiPipelineHopContext;
import org.apache.hop.ui.hopgui.file.pipeline.context.HopGuiPipelineNoteContext;
import org.apache.hop.ui.hopgui.file.pipeline.context.HopGuiPipelineTransformContext;
import org.apache.hop.ui.hopgui.file.pipeline.delegates.HopGuiPipelineCheckDelegate;
import org.apache.hop.ui.hopgui.file.pipeline.delegates.HopGuiPipelineClipboardDelegate;
import org.apache.hop.ui.hopgui.file.pipeline.delegates.HopGuiPipelineGridDelegate;
import org.apache.hop.ui.hopgui.file.pipeline.delegates.HopGuiPipelineHopDelegate;
import org.apache.hop.ui.hopgui.file.pipeline.delegates.HopGuiPipelineLogDelegate;
import org.apache.hop.ui.hopgui.file.pipeline.delegates.HopGuiPipelineRunDelegate;
import org.apache.hop.ui.hopgui.file.pipeline.delegates.HopGuiPipelineTransformDelegate;
import org.apache.hop.ui.hopgui.file.pipeline.delegates.HopGuiPipelineUndoDelegate;
import org.apache.hop.ui.hopgui.file.pipeline.extension.HopGuiPipelineFinishedExtension;
import org.apache.hop.ui.hopgui.file.pipeline.extension.HopGuiPipelineGraphExtension;
import org.apache.hop.ui.hopgui.file.shared.HopGuiTooltipExtension;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopGuiAbstractGraph;
import org.apache.hop.ui.hopgui.perspective.execution.ExecutionPerspective;
import org.apache.hop.ui.hopgui.perspective.execution.IExecutionViewer;
import org.apache.hop.ui.hopgui.shared.SwtGc;
import org.apache.hop.ui.pipeline.dialog.PipelineDialog;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.apache.hop.ui.util.HelpUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.MouseListener;
import org.eclipse.swt.events.MouseMoveListener;
import org.eclipse.swt.events.MouseTrackListener;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.swt.widgets.ToolTip;

/**
 * This class handles the display of the pipelines in a graphical way using icons, arrows, etc. One
 * pipeline is handled per HopGuiPipelineGraph
 */
@GuiPlugin(description = "The pipeline graph GUI plugin")
@SuppressWarnings("java:S1104")
public class HopGuiPipelineGraph extends HopGuiAbstractGraph
    implements IRedrawable,
        MouseListener,
        MouseMoveListener,
        MouseTrackListener,
        IHasLogChannel,
        ILogParentProvided,
        IHopFileTypeHandler,
        IGuiRefresher {

  private static final Class<?> PKG = HopGui.class;

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "HopGuiPipelineGraph-Toolbar";
  public static final String TOOLBAR_ITEM_START = "HopGuiPipelineGraph-ToolBar-10010-Run";
  public static final String TOOLBAR_ITEM_STOP = "HopGuiPipelineGraph-ToolBar-10030-Stop";
  public static final String TOOLBAR_ITEM_PAUSE = "HopGuiPipelineGraph-ToolBar-10020-Pause";
  public static final String TOOLBAR_ITEM_CHECK = "HopGuiPipelineGraph-ToolBar-10040-Check";
  public static final String TOOLBAR_ITEM_PREVIEW = "HopGuiPipelineGraph-ToolBar-10050-Preview";
  public static final String TOOLBAR_ITEM_DEBUG = "HopGuiPipelineGraph-ToolBar-10060-Debug";

  public static final String TOOLBAR_ITEM_UNDO_ID = "HopGuiPipelineGraph-ToolBar-10100-Undo";
  public static final String TOOLBAR_ITEM_REDO_ID = "HopGuiPipelineGraph-ToolBar-10110-Redo";

  public static final String TOOLBAR_ITEM_SHOW_EXECUTION_RESULTS =
      "HopGuiPipelineGraph-ToolBar-10400-Execution-Results";

  public static final String TOOLBAR_ITEM_ZOOM_LEVEL =
      "HopGuiPipelineGraph-ToolBar-10500-Zoom-Level";

  public static final String TOOLBAR_ITEM_ZOOM_OUT = "HopGuiPipelineGraph-ToolBar-10510-Zoom-Out";

  public static final String TOOLBAR_ITEM_ZOOM_IN = "HopGuiPipelineGraph-ToolBar-10520-Zoom-In";

  public static final String TOOLBAR_ITEM_ZOOM_100PCT =
      "HopGuiPipelineGraph-ToolBar-10530-Zoom-100Pct";
  public static final String TOOLBAR_ITEM_ZOOM_TO_FIT =
      "HopGuiPipelineGraph-ToolBar-10540-Zoom-To-Fit";

  public static final String TOOLBAR_ITEM_EDIT_PIPELINE =
      "HopGuiPipelineGraph-ToolBar-10450-EditPipeline";

  public static final String TOOLBAR_ITEM_TO_EXECUTION_INFO =
      "HopGuiPipelineGraph-ToolBar-10475-ToExecutionInfo";

  public static final String ACTION_ID_PIPELINE_GRAPH_HOP_ENABLE =
      "pipeline-graph-hop-10010-hop-enable";
  public static final String ACTION_ID_PIPELINE_GRAPH_HOP_DISABLE =
      "pipeline-graph-hop-10015-hop-disable";
  public static final String ACTION_ID_PIPELINE_GRAPH_TRANSFORM_ROWS_COPY =
      "pipeline-graph-transform-10650-rows-copy";
  public static final String ACTION_ID_PIPELINE_GRAPH_TRANSFORM_ROWS_DISTRIBUTE =
      "pipeline-graph-transform-10600-rows-distribute";
  public static final String ACTION_ID_PIPELINE_GRAPH_TRANSFORM_SPECIFY_COPIES =
      "pipeline-graph-transform-10100-copies";
  public static final String ACTION_ID_PIPELINE_GRAPH_TRANSFORM_ERROR_HANDLING =
      "pipeline-graph-transform-10800-error-handling";
  public static final String ACTION_ID_PIPELINE_GRAPH_TRANSFORM_VIEW_EXECUTION_INFO =
      "pipeline-graph-transform-11000-view-execution-info";
  public static final String CONST_ERROR = "Error";
  public static final String CONST_ERROR_PREVIEWING_PIPELINE = "Error previewing pipeline";

  private final ILogChannel log;

  private static final int HOP_SEL_MARGIN = 9;

  private static final int TOOLTIP_HIDE_DELAY_FLASH = 2000;

  private static final int TOOLTIP_HIDE_DELAY_SHORT = 5000;

  private static final int TOOLTIP_HIDE_DELAY_LONG = 10000;

  @Getter private PipelineMeta pipelineMeta;
  @Getter public IPipelineEngine<PipelineMeta> pipeline;

  @Getter private final HopDataOrchestrationPerspective perspective;

  @Getter @Setter private ToolBar toolBar;

  @Getter private GuiToolbarWidgets toolBarWidgets;

  private int iconSize;

  private Point lastClick;

  @Getter private Point lastMove;

  private Point[] previousTransformLocations;

  private Point[] previousNoteLocations;

  private List<TransformMeta> selectedTransforms;

  private TransformMeta selectedTransform;

  private List<NotePadMeta> selectedNotes;

  private NotePadMeta selectedNote;

  private PipelineHopMeta candidate;

  private boolean splitHop;

  private int lastButton;

  // Keep track if a contextual dialog box is open, do not display the tooltip
  private boolean openedContextDialog = false;

  private PipelineHopMeta lastHopSplit;

  private org.apache.hop.core.gui.Rectangle selectionRegion;

  @Getter @Setter private List<ICheckResult> remarks;

  @Getter @Setter private List<DatabaseImpact> impact;

  @Getter @Setter private boolean impactFinished;

  @Getter private PipelineDebugMeta lastPipelineDebugMeta;

  protected int currentMouseX = 0;

  protected int currentMouseY = 0;

  protected NotePadMeta currentNotePad = null;

  @Setter @Getter protected TransformMeta currentTransform;

  private final List<AreaOwner> areaOwners;

  private final SashForm sashForm;

  public CTabFolder extraViewTabFolder;

  private boolean initialized;

  private boolean halted;

  @Getter @Setter private boolean halting;

  private boolean safeStopping;

  private boolean debug;

  public HopGuiPipelineLogDelegate pipelineLogDelegate;
  public HopGuiPipelineGridDelegate pipelineGridDelegate;
  public HopGuiPipelineCheckDelegate pipelineCheckDelegate;
  public HopGuiPipelineRunDelegate pipelineRunDelegate;
  public HopGuiPipelineTransformDelegate pipelineTransformDelegate;
  public HopGuiPipelineClipboardDelegate pipelineClipboardDelegate;
  public HopGuiPipelineHopDelegate pipelineHopDelegate;
  public HopGuiPipelineUndoDelegate pipelineUndoDelegate;

  public HopGuiServerDelegate serverDelegate;
  public HopGuiNotePadDelegate notePadDelegate;

  public List<ISelectedTransformListener> transformListeners;
  public List<ITransformSelectionListener> currentTransformListeners = new ArrayList<>();

  @Getter @Setter private Map<String, String> transformLogMap;

  private TransformMeta startHopTransform;
  private Point endHopLocation;
  private boolean startErrorHopTransform;

  private TransformMeta noInputTransform;

  private TransformMeta endHopTransform;

  private StreamType candidateHopType;

  Timer redrawTimer;

  @Setter private HopPipelineFileType<PipelineMeta> fileType;
  private boolean singleClick;
  private boolean doubleClick;
  private boolean mouseMovedSinceClick;

  private PipelineHopMeta clickedPipelineHop;

  @Getter @Setter protected Map<String, RowBuffer> outputRowsMap;
  private boolean avoidContextDialog;

  public void setCurrentNote(NotePadMeta ni) {
    this.currentNotePad = ni;
  }

  public NotePadMeta getCurrentNote() {
    return currentNotePad;
  }

  public void addSelectedTransformListener(ISelectedTransformListener selectedTransformListener) {
    transformListeners.add(selectedTransformListener);
  }

  public void addCurrentTransformListener(ITransformSelectionListener transformSelectionListener) {
    currentTransformListeners.add(transformSelectionListener);
  }

  public HopGuiPipelineGraph(
      Composite parent,
      final HopGui hopGui,
      final CTabItem parentTabItem,
      final HopDataOrchestrationPerspective perspective,
      final PipelineMeta pipelineMeta,
      final HopPipelineFileType<PipelineMeta> fileType) {
    super(hopGui, parent, SWT.NO_BACKGROUND, parentTabItem);
    this.hopGui = hopGui;
    this.parentTabItem = parentTabItem;
    this.perspective = perspective;
    this.pipelineMeta = pipelineMeta;
    this.fileType = fileType;
    this.areaOwners = new ArrayList<>();

    this.log = hopGui.getLog();

    // Adjust the internal variables
    //
    pipelineMeta.setInternalHopVariables(variables);

    pipelineLogDelegate = new HopGuiPipelineLogDelegate(hopGui, this);
    pipelineGridDelegate = new HopGuiPipelineGridDelegate(hopGui, this);
    pipelineCheckDelegate = new HopGuiPipelineCheckDelegate(hopGui, this);
    pipelineClipboardDelegate = new HopGuiPipelineClipboardDelegate(hopGui, this);
    pipelineTransformDelegate = new HopGuiPipelineTransformDelegate(hopGui, this);
    pipelineHopDelegate = new HopGuiPipelineHopDelegate(hopGui, this);
    pipelineUndoDelegate = new HopGuiPipelineUndoDelegate(hopGui, this);
    pipelineRunDelegate = new HopGuiPipelineRunDelegate(hopGui, this);

    serverDelegate = new HopGuiServerDelegate(hopGui, this);
    notePadDelegate = new HopGuiNotePadDelegate(hopGui, this);

    transformListeners = new ArrayList<>();

    // This composite takes up all the variables in the parent
    //
    FormData formData = new FormData();
    formData.left = new FormAttachment(0, 0);
    formData.top = new FormAttachment(0, 0);
    formData.right = new FormAttachment(100, 0);
    formData.bottom = new FormAttachment(100, 0);
    setLayoutData(formData);

    // The layout in the widget is done using a FormLayout
    //
    setLayout(new FormLayout());

    // Add a tool-bar at the top of the tab
    // The form-data is set on the native widget automatically
    //
    addToolBar();

    // The main composite contains the graph view, but if needed also
    // a view with an extra tab containing log, etc.
    //
    Composite mainComposite = new Composite(this, SWT.NONE);
    mainComposite.setLayout(new FormLayout());
    FormData fdMainComposite = new FormData();
    fdMainComposite.left = new FormAttachment(0, 0);
    fdMainComposite.top =
        new FormAttachment(0, toolBar.getBounds().height); // Position below toolbar
    fdMainComposite.right = new FormAttachment(100, 0);
    fdMainComposite.bottom = new FormAttachment(100, 0);
    mainComposite.setLayoutData(fdMainComposite);

    // To allow for a splitter later on, we will add the splitter here...
    //
    sashForm =
        new SashForm(
            mainComposite,
            PropsUi.getInstance().isGraphExtraViewVerticalOrientation()
                ? SWT.VERTICAL
                : SWT.HORIZONTAL);
    FormData fdSashForm = new FormData();
    fdSashForm.left = new FormAttachment(0, 0);
    fdSashForm.top = new FormAttachment(0, 0);
    fdSashForm.right = new FormAttachment(100, 0);
    fdSashForm.bottom = new FormAttachment(100, 0);
    sashForm.setLayoutData(fdSashForm);

    // Add a canvas below it, use up all space initially
    //
    canvas = new Canvas(sashForm, SWT.NO_BACKGROUND | SWT.BORDER);
    Listener listener = CanvasListener.getInstance();
    canvas.addListener(SWT.MouseDown, listener);
    canvas.addListener(SWT.MouseMove, listener);
    canvas.addListener(SWT.MouseUp, listener);
    canvas.addListener(SWT.Paint, listener);
    FormData fdCanvas = new FormData();
    fdCanvas.left = new FormAttachment(0, 0);
    fdCanvas.top = new FormAttachment(0, 0);
    fdCanvas.right = new FormAttachment(100, 0);
    fdCanvas.bottom = new FormAttachment(100, 0);
    canvas.setLayoutData(fdCanvas);

    sashForm.setWeights(100);

    toolTip = new ToolTip(getShell(), SWT.BALLOON);
    toolTip.setAutoHide(true);

    iconSize = hopGui.getProps().getIconSize();

    clearSettings();

    remarks = new ArrayList<>();
    impact = new ArrayList<>();
    impactFinished = false;

    setVisible(true);
    newProps();

    canvas.setBackground(GuiResource.getInstance().getColorBlueCustomGrid());
    canvas.addPaintListener(this::paintControl);

    selectedTransforms = null;
    lastClick = null;

    /*
     * Handle the mouse...
     */

    canvas.addMouseListener(this);
    if (!EnvironmentUtils.getInstance().isWeb()) {
      canvas.addMouseMoveListener(this);
      canvas.addMouseTrackListener(this);
      canvas.addMouseWheelListener(this::mouseScrolled);
    }

    setBackground(GuiResource.getInstance().getColorBackground());

    // Add keyboard listeners from the main GUI and this class (toolbar etc) to the canvas. That's
    // where the focus should be
    //
    hopGui.replaceKeyboardShortcutListeners(this);

    // Scrolled composite ...
    //
    canvas.pack();

    // Update menu, toolbar, force redraw canvas
    //
    updateGui();
  }

  public static HopGuiPipelineGraph getInstance() {
    return HopGui.getActivePipelineGraph();
  }

  @Override
  public void dispose() {
    disposeExtraView();
    super.dispose();
  }

  @Override
  public void mouseDoubleClick(MouseEvent e) {

    if (!PropsUi.getInstance().useDoubleClick()) {
      return;
    }

    doubleClick = true;
    clearSettings();

    Point real = screen2real(e.x, e.y);

    // Hide the tooltip!
    hideToolTips();

    AreaOwner areaOwner = getVisibleAreaOwner(real.x, real.y);

    try {
      HopGuiPipelineGraphExtension ext = new HopGuiPipelineGraphExtension(this, e, real, areaOwner);
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL, variables, HopExtensionPoint.PipelineGraphMouseDoubleClick.id, ext);
      if (ext.isPreventingDefault()) {
        return;
      }
    } catch (Exception ex) {
      LogChannel.GENERAL.logError(
          "Error calling PipelineGraphMouseDoubleClick extension point", ex);
    }

    TransformMeta transformMeta = pipelineMeta.getTransform(real.x, real.y, iconSize);
    if (transformMeta != null) {
      if (e.button == 1) {
        editTransform(transformMeta);
      } else {
        editDescription(transformMeta);
      }
    } else {
      // Check if point lies on one of the many hop-lines...
      PipelineHopMeta online = findPipelineHop(real.x, real.y);
      if (online != null) {
        editHop(online);
      } else {
        NotePadMeta ni = pipelineMeta.getNote(real.x, real.y);
        if (ni != null) {
          selectedNote = null;
          editNote(ni);
        } else {
          // See if the double click was in one of the visible drawn area's...
          //
          if (areaOwner != null
              && areaOwner.getParent() instanceof TransformMeta transform
              && areaOwner
                  .getOwner()
                  .equals(PipelinePainter.STRING_PARTITIONING_CURRENT_TRANSFORM)) {

            pipelineTransformDelegate.editTransformPartitioning(pipelineMeta, transform);
          } else {
            editPipelineProperties(new HopGuiPipelineContext(pipelineMeta, this, real));
          }
        }
      }
    }
  }

  @Override
  public void mouseDown(MouseEvent e) {
    if (EnvironmentUtils.getInstance().isWeb()) {
      // RAP does not support certain mouse events.
      mouseHover(e);
    }
    doubleClick = false;
    mouseMovedSinceClick = false;

    boolean alt = (e.stateMask & SWT.ALT) != 0;
    boolean control = (e.stateMask & SWT.MOD1) != 0;
    boolean shift = (e.stateMask & SWT.SHIFT) != 0;

    lastButton = e.button;
    Point real = screen2real(e.x, e.y);
    lastClick = new Point(real.x, real.y);

    // Hide the tooltip!
    hideToolTips();

    AreaOwner areaOwner = getVisibleAreaOwner(real.x, real.y);

    try {
      HopGuiPipelineGraphExtension ext = new HopGuiPipelineGraphExtension(this, e, real, areaOwner);
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL, variables, HopExtensionPoint.PipelineGraphMouseDown.id, ext);
      if (ext.isPreventingDefault()) {
        return;
      }
    } catch (Exception ex) {
      LogChannel.GENERAL.logError("Error calling PipelineGraphMouseDown extension point", ex);
    }

    // A single left or middle click on one of the area owners...
    //
    if (areaOwner != null && areaOwner.getAreaType() != null) {
      switch (areaOwner.getAreaType()) {
        case TRANSFORM_INFO_ICON:
          // Click on the transform info icon means: Edit transformation description
          //
          this.editDescription((TransformMeta) areaOwner.getOwner());
          break;

        case HOP_INFO_ICON:
          // Click on the hop info icon means: Edit transform that use info
          //
          pipelineTransformDelegate.editTransform(
              pipelineMeta, (TransformMeta) areaOwner.getOwner());
          break;

        case TRANSFORM_TARGET_HOP_ICON:
          // Click on the hop target icon means: Edit transform that dispatch row
          //
          pipelineTransformDelegate.editTransform(
              pipelineMeta, (TransformMeta) areaOwner.getParent());
          break;

        case HOP_COPY_ICON:
          break;

        case HOP_ERROR_ICON:
          // Click on the error icon means: Edit error handling
          //
          pipelineTransformDelegate.editTransformErrorHandling(
              pipelineMeta, (TransformMeta) areaOwner.getParent());
          break;

        case TRANSFORM_ICON:
          if (shift && control) {
            openReferencedObject();
            return;
          }

          if (candidate != null) {
            /** Avoid duplicate pop-up for hop handling as candidate is never null? */
            if (!OsHelper.isMac()) {
              addCandidateAsHop(e.x, e.y);
            }
          }

          TransformMeta transformMeta = (TransformMeta) areaOwner.getOwner();
          currentTransform = transformMeta;

          for (ITransformSelectionListener listener : currentTransformListeners) {
            listener.onUpdateSelection(currentTransform);
          }

          // ALT-Click: edit error handling
          //
          if (e.button == 1 && alt && transformMeta.supportsErrorHandling()) {
            pipelineTransformDelegate.editTransformErrorHandling(pipelineMeta, transformMeta);
            return;
          } else if (e.button == 1 && startHopTransform != null && endHopTransform == null) {
            candidate = new PipelineHopMeta(startHopTransform, currentTransform);
            addCandidateAsHop(e.x, e.y);
          } else if (e.button == 2 || (e.button == 1 && shift)) {
            // SHIFT CLICK is start of drag to create a new hop
            //
            canvas.setData("mode", "hop");
            startHopTransform = transformMeta;
          } else {
            canvas.setData("mode", "drag");
            selectedTransforms = pipelineMeta.getSelectedTransforms();
            selectedTransform = transformMeta;
            //
            // When an icon is moved that is not selected, it gets
            // selected too late.
            // It is not captured here, but in the mouseMoveListener...
            //
            previousTransformLocations = pipelineMeta.getSelectedTransformLocations();

            Point p = transformMeta.getLocation();
            iconOffset = new Point(real.x - p.x, real.y - p.y);
          }
          redraw();
          break;

        case NOTE:
          currentNotePad = (NotePadMeta) areaOwner.getOwner();
          selectedNotes = pipelineMeta.getSelectedNotes();
          selectedNote = currentNotePad;
          Point loc = currentNotePad.getLocation();

          previousNoteLocations = pipelineMeta.getSelectedNoteLocations();

          noteOffset = new Point(real.x - loc.x, real.y - loc.y);

          redraw();
          break;

        case TRANSFORM_COPIES_TEXT:
          copies((TransformMeta) areaOwner.getOwner());
          break;

        case TRANSFORM_DATA_SERVICE:
          editProperties(pipelineMeta, hopGui, PipelineDialog.Tabs.EXTRA_TAB);
          break;
        default:
          break;
      }
    } else {
      // hop links between transforms are found searching by (x,y) coordinates.
      PipelineHopMeta hop = findPipelineHop(real.x, real.y);
      if (hop != null) {
        // Delete hop with on click
        if (e.button == 1 && shift && control) {
          // Delete the hop
          pipelineHopDelegate.delHop(pipelineMeta, hop);
          updateGui();
        }
        // User held control and clicked a hop between steps - We want to flip the active state of
        // the hop.
        //
        else if (e.button == 2 || (e.button == 1 && control)) {
          hop.setEnabled(!hop.isEnabled());
          updateGui();
        } else {
          // A hop: show context dialog in mouseUp()
          //
          clickedPipelineHop = hop;
        }
      } else {
        // If we're dragging a candidate hop around and click on the background it simply needs to
        // go away.
        //
        if (startHopTransform != null) {
          startHopTransform = null;
          candidate = null;
          lastClick = null;
          avoidContextDialog = true;
          redraw();
          return;
        }

        // Dragging on the background?
        // See if we're dragging around the view-port over the pipeline graph.
        //
        if (setupDragView(e.button, control, new Point(e.x, e.y))) {
          return;
        }

        // No area-owner & no hop means : background click:
        //
        canvas.setData("mode", "select");
        if (!control && e.button == 1) {
          selectionRegion = new org.apache.hop.core.gui.Rectangle(real.x, real.y, 0, 0);
        }
        updateGui();
      }
    }
    if (EnvironmentUtils.getInstance().isWeb()) {
      // RAP does not support certain mouse events.
      mouseMove(e);
    }
  }

  @SuppressWarnings("java:S115")
  private enum SingleClickType {
    Pipeline,
    Transform,
    Note,
    Hop,
  }

  @Override
  public void mouseUp(MouseEvent e) {
    // canvas.setData("mode", null); does not work.
    canvas.setData("mode", "null");

    if (EnvironmentUtils.getInstance().isWeb()) {
      // RAP does not support certain mouse events.
      mouseMove(e);
    }

    // Default cursor
    setCursor(null);

    if (viewPortNavigation || viewDrag) {
      viewDrag = false;
      viewPortNavigation = false;
      viewPortStart = null;
      setCursor(null);
      return;
    }

    boolean control = (e.stateMask & SWT.MOD1) != 0;
    PipelineHopMeta selectedHop = findPipelineHop(e.x, e.y);
    updateErrorMetaForHop(selectedHop);
    boolean singleClick = false;
    mouseOverName = null;
    viewDrag = false;
    viewDragStart = null;
    SingleClickType singleClickType = null;
    TransformMeta singleClickTransform = null;
    NotePadMeta singleClickNote = null;
    PipelineHopMeta singleClickHop = null;

    if (iconOffset == null) {
      iconOffset = new Point(0, 0);
    }
    Point real = screen2real(e.x, e.y);
    Point icon = new Point(real.x - iconOffset.x, real.y - iconOffset.y);
    AreaOwner areaOwner = getVisibleAreaOwner(real.x, real.y);

    try {
      HopGuiPipelineGraphExtension ext = new HopGuiPipelineGraphExtension(this, e, real, areaOwner);
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL, variables, HopExtensionPoint.PipelineGraphMouseUp.id, ext);
      if (ext.isPreventingDefault()) {
        redraw();
        clearSettings();
        return;
      }
    } catch (Exception ex) {
      LogChannel.GENERAL.logError("Error calling PipelineGraphMouseUp extension point", ex);
    }

    // Did we select a region on the screen? Mark transforms in region as
    // selected
    //
    if (selectionRegion != null) {
      selectionRegion.width = real.x - selectionRegion.x;
      selectionRegion.height = real.y - selectionRegion.y;
      if (selectionRegion.isEmpty()) {
        singleClick = true;
        singleClickType = SingleClickType.Pipeline;
      } else {
        pipelineMeta.unselectAll();
        selectInRect(pipelineMeta, selectionRegion);
        selectionRegion = null;
        updateGui();
        return;
      }
    }

    // Special cases...
    //
    if (areaOwner != null && areaOwner.getAreaType() != null) {
      switch (areaOwner.getAreaType()) {
        case TRANSFORM_OUTPUT_DATA:
          if ((!mouseMovedSinceClick || EnvironmentUtils.getInstance().isWeb())
              && showTransformOutputData(areaOwner)) {
            return;
          }
          break;
        case TRANSFORM_ICON:
          if (startHopTransform != null) {
            // Mouse up while dragging around a hop candidate
            //
            currentTransform = (TransformMeta) areaOwner.getOwner();
            candidate = new PipelineHopMeta(startHopTransform, currentTransform);
            addCandidateAsHop(e.x, e.y);
            redraw();
            return;
          }
          break;
        case TRANSFORM_NAME:
          if (startHopTransform == null
              && selectionRegion == null
              && selectedTransforms == null
              && selectedNotes == null) {
            // This is available only in single click mode...
            //
            startHopTransform = null;
            selectionRegion = null;

            TransformMeta transformMeta = (TransformMeta) areaOwner.getParent();
            editTransform(transformMeta);
          }
          return;

        default:
          break;
      }
    }

    // Clicked on an icon?
    //
    if (selectedTransform != null && startHopTransform == null) {
      if (e.button == 1) {
        Point realClick = screen2real(e.x, e.y);
        if (lastClick.x == realClick.x && lastClick.y == realClick.y) {
          // Flip selection when control is pressed!
          if (control) {
            selectedTransform.flipSelected();
          } else {
            singleClick = true;
            singleClickType = SingleClickType.Transform;
            singleClickTransform = selectedTransform;

            // If the clicked transform is not part of the current selection, cancel the
            // current selection
            if (!selectedTransform.isSelected()) {
              pipelineMeta.unselectAll();
              selectedTransform.setSelected(true);
            }
          }
        } else {
          // Find out which Transforms & Notes are selected
          selectedTransforms = pipelineMeta.getSelectedTransforms();
          selectedNotes = pipelineMeta.getSelectedNotes();

          // We moved around some items: store undo info...
          //
          boolean also = false;
          if (selectedNotes != null && !selectedNotes.isEmpty() && previousNoteLocations != null) {
            int[] indexes = pipelineMeta.getNoteIndexes(selectedNotes);

            also = selectedTransforms != null && !selectedTransforms.isEmpty();
            hopGui.undoDelegate.addUndoPosition(
                pipelineMeta,
                selectedNotes.toArray(new NotePadMeta[selectedNotes.size()]),
                indexes,
                previousNoteLocations,
                pipelineMeta.getSelectedNoteLocations(),
                also);
          }
          if (selectedTransforms != null && previousTransformLocations != null) {
            int[] indexes = pipelineMeta.getTransformIndexes(selectedTransforms);
            hopGui.undoDelegate.addUndoPosition(
                pipelineMeta,
                selectedTransforms.toArray(new TransformMeta[selectedTransforms.size()]),
                indexes,
                previousTransformLocations,
                pipelineMeta.getSelectedTransformLocations(),
                also);
          }
        }
      }

      // OK, we moved the transform, did we move it across a hop?
      // If so, ask to split the hop!
      if (splitHop) {
        PipelineHopMeta hi =
            findPipelineHop(icon.x + iconSize / 2, icon.y + iconSize / 2, selectedTransform);
        if (hi != null) {
          splitHop(hi);
        }
        splitHop = false;
      }

      selectedTransforms = null;
      selectedNotes = null;
      selectedTransform = null;
      selectedNote = null;
      startHopTransform = null;
      endHopLocation = null;

      updateGui();
    } else {
      // Notes?
      //

      if (selectedNote != null) {
        if (e.button == 1) {
          if (lastClick.x == real.x && lastClick.y == real.y) {
            // Flip selection when control is pressed!
            if (control) {
              selectedNote.flipSelected();
            } else {
              // single click on a note: ask what needs to happen...
              //
              singleClick = true;
              singleClickType = SingleClickType.Note;
              singleClickNote = selectedNote;
            }
          } else {
            // Find out which Transforms & Notes are selected
            selectedTransforms = pipelineMeta.getSelectedTransforms();
            selectedNotes = pipelineMeta.getSelectedNotes();

            // We moved around some items: store undo info...

            boolean also = false;
            if (selectedNotes != null
                && !selectedNotes.isEmpty()
                && previousNoteLocations != null) {
              int[] indexes = pipelineMeta.getNoteIndexes(selectedNotes);
              hopGui.undoDelegate.addUndoPosition(
                  pipelineMeta,
                  selectedNotes.toArray(new NotePadMeta[selectedNotes.size()]),
                  indexes,
                  previousNoteLocations,
                  pipelineMeta.getSelectedNoteLocations(),
                  also);
              also = selectedTransforms != null && !selectedTransforms.isEmpty();
            }
            if (selectedTransforms != null
                && !selectedTransforms.isEmpty()
                && previousTransformLocations != null) {
              int[] indexes = pipelineMeta.getTransformIndexes(selectedTransforms);
              hopGui.undoDelegate.addUndoPosition(
                  pipelineMeta,
                  selectedTransforms.toArray(new TransformMeta[selectedTransforms.size()]),
                  indexes,
                  previousTransformLocations,
                  pipelineMeta.getSelectedTransformLocations(),
                  also);
            }
          }
        }

        selectedNotes = null;
        selectedTransforms = null;
        selectedTransform = null;
        selectedNote = null;
        startHopTransform = null;
        endHopLocation = null;
        updateGui();
      }
    }

    if (avoidContextDialog) {
      avoidContextDialog = false;
      selectionRegion = null;
      return;
    }

    if (clickedPipelineHop != null) {
      // Clicked on a hop
      //
      singleClick = true;
      singleClickType = SingleClickType.Hop;
      singleClickHop = clickedPipelineHop;
    }
    clickedPipelineHop = null;

    // Only do this "mouseUp()" if this is not part of a double click...
    //
    final boolean fSingleClick = singleClick;
    final SingleClickType fSingleClickType = singleClickType;
    final TransformMeta fSingleClickTransform = singleClickTransform;
    final NotePadMeta fSingleClickNote = singleClickNote;
    final PipelineHopMeta fSingleClickHop = singleClickHop;

    if (PropsUi.getInstance().useDoubleClick()) {
      hopGui
          .getDisplay()
          .timerExec(
              hopGui.getDisplay().getDoubleClickTime(),
              () ->
                  showActionDialog(
                      e,
                      real,
                      fSingleClick,
                      fSingleClickType,
                      fSingleClickTransform,
                      fSingleClickNote,
                      fSingleClickHop));
    } else {
      showActionDialog(
          e,
          real,
          fSingleClick,
          fSingleClickType,
          fSingleClickTransform,
          fSingleClickNote,
          fSingleClickHop);
    }
    lastButton = 0;
  }

  @GuiContextAction(
      id = "pipeline-graph-transform-1000-view-output",
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Info,
      name = "i18n::HopGuiPipelineGraph.ViewOutput.GuiAction.Name",
      tooltip = "i18n::HopGuiPipelineGraph.ViewOutput.GuiAction.Tooltip",
      image = "ui/images/data.svg",
      category = "Preview",
      categoryOrder = "3")
  public void showTransformOutputData(HopGuiPipelineTransformContext context) {
    TransformMeta dataTransformMeta = context.getTransformMeta();
    if (outputRowsMap != null) {
      RowBuffer rowBuffer = outputRowsMap.get(dataTransformMeta.getName());
      if (rowBuffer != null) {
        showTransformOutputData(dataTransformMeta, rowBuffer);
      }
    }
  }

  public boolean showTransformOutputData(AreaOwner areaOwner) {
    TransformMeta dataTransform = (TransformMeta) areaOwner.getParent();
    RowBuffer rowBuffer = (RowBuffer) areaOwner.getOwner();
    return showTransformOutputData(dataTransform, rowBuffer);
  }

  private boolean showTransformOutputData(TransformMeta dataTransformMeta, RowBuffer rowBuffer) {
    if (rowBuffer != null) {
      synchronized (rowBuffer.getBuffer()) {
        if (!rowBuffer.isEmpty()) {
          try {
            String title =
                BaseMessages.getString(
                    PKG,
                    "PipelineGraph.ViewOutput.OutputDialog.Header",
                    dataTransformMeta.getName());
            String message =
                BaseMessages.getString(
                    PKG,
                    "PipelineGraph.ViewOutput.OutputDialog.OutputRows.Text",
                    dataTransformMeta.getName());
            String prefix = "";

            if (pipeline != null && pipeline.getPipelineRunConfiguration() != null) {
              PipelineRunConfiguration pipelineRunConfiguration =
                  pipeline.getPipelineRunConfiguration();
              if (pipelineRunConfiguration.getEngineRunConfiguration()
                  instanceof LocalPipelineRunConfiguration localPipelineRunConfiguration) {
                String sampleTypeInGui = localPipelineRunConfiguration.getSampleTypeInGui();
                if (StringUtils.isNotEmpty(sampleTypeInGui)) {
                  try {
                    SampleType sampleType = SampleType.valueOf(sampleTypeInGui);
                    switch (sampleType) {
                      case None:
                        break;
                      case First:
                        prefix =
                            BaseMessages.getString(
                                PKG, "PipelineGraph.ViewOutput.OutputDialog.First.Text");
                        break;
                      case Last:
                        prefix =
                            BaseMessages.getString(
                                PKG, "PipelineGraph.ViewOutput.OutputDialog.Last.Text");
                        break;
                      case Random:
                        prefix +=
                            BaseMessages.getString(
                                PKG, "PipelineGraph.ViewOutput.OutputDialog.Random.Text");
                        break;
                      default:
                        break;
                    }
                  } catch (Exception ex) {
                    LogChannel.UI.logError("Unknown sample type: " + sampleTypeInGui);
                  }
                }
              }
            }

            PreviewRowsDialog previewRowsDialog =
                new PreviewRowsDialog(
                    hopGui.getActiveShell(),
                    variables,
                    SWT.NONE,
                    dataTransformMeta.getName(),
                    rowBuffer.getRowMeta(),
                    rowBuffer.getBuffer());
            previewRowsDialog.setTitleMessage(title, prefix + message);
            previewRowsDialog.open();
          } catch (Exception ex) {
            new ErrorDialog(
                hopGui.getActiveShell(), CONST_ERROR, "Error showing preview dialog", ex);
          }
        }
      }
      return true;
    }
    return false;
  }

  private void showActionDialog(
      MouseEvent e,
      Point real,
      boolean fSingleClick,
      SingleClickType fSingleClickType,
      TransformMeta fSingleClickTransform,
      NotePadMeta fSingleClickNote,
      PipelineHopMeta fSingleClickHop) {

    // In any case clear the selection region...
    //
    selectionRegion = null;

    // See if there are transforms selected.
    // If we get a background single click then simply clear selection...
    //
    if (fSingleClickType == SingleClickType.Pipeline) {
      if (!pipelineMeta.getSelectedTransforms().isEmpty()
          || !pipelineMeta.getSelectedNotes().isEmpty()) {
        pipelineMeta.unselectAll();
        selectionRegion = null;
        updateGui();

        // Show a short tooltip
        //
        toolTip.setVisible(false);
        toolTip.setAutoHide(true);
        toolTip.setText(Const.CR + "  Selection cleared " + Const.CR);
        showToolTip(new org.eclipse.swt.graphics.Point(e.x, e.y));

        return;
      }
    }

    if (!doubleClick) {
      // Just a single click on the background:
      // We have a bunch of possible actions for you...
      //
      if (fSingleClick && fSingleClickType != null) {
        IGuiContextHandler contextHandler = null;
        String message = null;
        switch (fSingleClickType) {
          case Pipeline:
            message =
                BaseMessages.getString(PKG, "PipelineGraph.ContextualActionDialog.Pipeline.Header");
            contextHandler = new HopGuiPipelineContext(pipelineMeta, this, real);
            break;
          case Transform:
            message =
                BaseMessages.getString(
                    PKG,
                    "PipelineGraph.ContextualActionDialog.Transform.Header",
                    fSingleClickTransform.getName());
            contextHandler =
                new HopGuiPipelineTransformContext(pipelineMeta, fSingleClickTransform, this, real);
            break;
          case Note:
            message =
                BaseMessages.getString(PKG, "PipelineGraph.ContextualActionDialog.Note.Header");
            contextHandler =
                new HopGuiPipelineNoteContext(pipelineMeta, fSingleClickNote, this, real);
            break;
          case Hop:
            message =
                BaseMessages.getString(PKG, "PipelineGraph.ContextualActionDialog.Hop.Header");
            contextHandler =
                new HopGuiPipelineHopContext(pipelineMeta, fSingleClickHop, this, real);
            break;
          default:
            break;
        }
        if (contextHandler != null) {
          Shell parent = hopShell();
          org.eclipse.swt.graphics.Point p = parent.getDisplay().map(canvas, null, e.x, e.y);

          this.openedContextDialog = true;
          this.hideToolTips();

          // Show the context dialog
          //
          avoidContextDialog =
              GuiContextUtil.getInstance()
                  .handleActionSelection(parent, message, new Point(p.x, p.y), contextHandler);

          this.openedContextDialog = false;
        }
      }
    }
  }

  private void splitHop(PipelineHopMeta hop) {
    int id = 0;
    if (!hopGui.getProps().getAutoSplit()) {
      MessageDialogWithToggle md =
          new MessageDialogWithToggle(
              hopShell(),
              BaseMessages.getString(PKG, "PipelineGraph.Dialog.SplitHop.Title"),
              BaseMessages.getString(PKG, "PipelineGraph.Dialog.SplitHop.Message")
                  + Const.CR
                  + hop.toString(),
              SWT.ICON_QUESTION,
              new String[] {
                BaseMessages.getString(PKG, "System.Button.Yes"),
                BaseMessages.getString(PKG, "System.Button.No")
              },
              BaseMessages.getString(PKG, "PipelineGraph.Dialog.Option.SplitHop.DoNotAskAgain"),
              hopGui.getProps().getAutoSplit());
      id = md.open();
      hopGui.getProps().setAutoSplit(md.getToggleState());
    }

    if ((id & 0xFF) == 0) { // Means: "Yes" button clicked!
      pipelineTransformDelegate.insertTransform(pipelineMeta, hop, currentTransform);
      redraw();
    }
    // Discard this hop-split attempt.
    splitHop = false;
  }

  @Override
  public void mouseMove(MouseEvent event) {
    boolean shift = (event.stateMask & SWT.SHIFT) != 0;
    noInputTransform = null;
    mouseMovedSinceClick = true;
    boolean doRedraw = false;
    PipelineHopMeta hop = null;

    // disable the tooltip
    //
    toolTip.setVisible(false);

    // Check to see if we're navigating with the view port
    //
    if (viewPortNavigation) {
      dragViewPort(new Point(event.x, event.y));
      return;
    }

    Point real = screen2real(event.x, event.y);

    currentMouseX = real.x;
    currentMouseY = real.y;

    // Remember the last position of the mouse for paste with keyboard
    //
    lastMove = real;

    if (iconOffset == null) {
      iconOffset = new Point(0, 0);
    }
    Point icon = new Point(real.x - iconOffset.x, real.y - iconOffset.y);

    if (noteOffset == null) {
      noteOffset = new Point(0, 0);
    }
    Point note = new Point(real.x - noteOffset.x, real.y - noteOffset.y);

    // Moved over an area?
    //
    AreaOwner areaOwner = getVisibleAreaOwner(real.x, real.y);

    // Moved over an hop?
    //
    if (areaOwner == null) {
      hop = this.findPipelineHop(real.x, real.y);
    }

    try {
      HopGuiPipelineGraphExtension ext =
          new HopGuiPipelineGraphExtension(this, event, real, areaOwner);
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL, variables, HopExtensionPoint.PipelineGraphMouseMoved.id, ext);
      if (ext.isPreventingDefault()) {
        return;
      }
    } catch (Exception ex) {
      LogChannel.GENERAL.logError("Error calling PipelineGraphMouseMoved extension point", ex);
    }

    // Mouse over the name of the transform
    //
    if (!PropsUi.getInstance().useDoubleClick()) {
      if (areaOwner != null && areaOwner.getAreaType() == AreaType.TRANSFORM_NAME) {
        if (mouseOverName == null) {
          doRedraw = true;
        }
        mouseOverName = (String) areaOwner.getOwner();
      } else {
        if (mouseOverName != null) {
          doRedraw = true;
        }
        mouseOverName = null;
      }
    }

    //
    // First see if the icon we clicked on was selected.
    // If the icon was not selected, we should un-select all other
    // icons, selected and move only the one icon
    //
    if (selectedTransform != null && !selectedTransform.isSelected()) {
      pipelineMeta.unselectAll();
      selectedTransform.setSelected(true);
      selectedTransforms = new ArrayList<>();
      selectedTransforms.add(selectedTransform);
      previousTransformLocations = new Point[] {selectedTransform.getLocation()};
      doRedraw = true;
    } else if (selectedNote != null && !selectedNote.isSelected()) {
      pipelineMeta.unselectAll();
      selectedNote.setSelected(true);
      selectedNotes = new ArrayList<>();
      selectedNotes.add(selectedNote);
      previousNoteLocations = new Point[] {selectedNote.getLocation()};
      doRedraw = true;
    } else if (selectionRegion != null && startHopTransform == null) {
      // Did we select a region...?
      //
      selectionRegion.width = real.x - selectionRegion.x;
      selectionRegion.height = real.y - selectionRegion.y;
      doRedraw = true;
    } else if (selectedTransform != null
        && lastButton == 1
        && !shift
        && startHopTransform == null) {
      //
      // One or more icons are selected and moved around...
      //
      // new : new position of the ICON (not the mouse pointer) dx : difference with previous
      // position
      //
      int dx = icon.x - selectedTransform.getLocation().x;
      int dy = icon.y - selectedTransform.getLocation().y;

      // See if we have a hop-split candidate
      //
      PipelineHopMeta hi =
          findPipelineHop(icon.x + iconSize / 2, icon.y + iconSize / 2, selectedTransform);
      if (hi != null) {
        // OK, we want to split the hop in 2

        // Check if we can split A-->--B and insert the selected transform C if
        // C-->--A or C-->--B or A-->--C or B-->--C don't exists...
        //
        if (pipelineMeta.findPipelineHop(selectedTransform, hi.getFromTransform()) == null
            && pipelineMeta.findPipelineHop(selectedTransform, hi.getToTransform()) == null
            && pipelineMeta.findPipelineHop(hi.getToTransform(), selectedTransform) == null
            && pipelineMeta.findPipelineHop(hi.getFromTransform(), selectedTransform) == null) {
          splitHop = true;
          lastHopSplit = hi;
          hi.split = true;
        }
      } else {
        if (lastHopSplit != null) {
          lastHopSplit.split = false;
          lastHopSplit = null;
          splitHop = false;
        }
      }

      moveSelected(dx, dy);

      doRedraw = true;
    } else if ((startHopTransform != null && endHopTransform == null)
        || (endHopTransform != null && startHopTransform == null)) {
      // Are we creating a new hop with the middle button or pressing SHIFT?
      //

      TransformMeta transformMeta = pipelineMeta.getTransform(real.x, real.y, iconSize);
      endHopLocation = new Point(real.x, real.y);
      if (transformMeta != null
          && ((startHopTransform != null && !startHopTransform.equals(transformMeta))
              || (endHopTransform != null && !endHopTransform.equals(transformMeta)))) {
        ITransformIOMeta ioMeta = transformMeta.getTransform().getTransformIOMeta();
        if (candidate == null) {
          // See if the transform accepts input. If not, we can't create a new hop...
          //
          if (startHopTransform != null) {
            if (ioMeta.isInputAcceptor()) {
              candidate = new PipelineHopMeta(startHopTransform, transformMeta);
              endHopLocation = null;
            } else {
              noInputTransform = transformMeta;
              toolTip.setText("This transform does not accept any input from other transforms");
              showToolTip(new org.eclipse.swt.graphics.Point(real.x, real.y));
            }
          } else if (endHopTransform != null) {
            if (ioMeta.isOutputProducer()) {
              candidate = new PipelineHopMeta(transformMeta, endHopTransform);
              endHopLocation = null;
            } else {
              noInputTransform = transformMeta;
              toolTip.setText(
                  "This transform doesn't pass any output to other transforms. (except perhaps for targetted output)");
              showToolTip(new org.eclipse.swt.graphics.Point(real.x, real.y));
            }
          }
        }
      } else {
        if (candidate != null) {
          candidate = null;
          doRedraw = true;
        }
      }

      doRedraw = true;
    } else {
      // Drag the view around with middle button on the background?
      //
      if (viewDrag && lastClick != null) {
        dragView(viewDragStart, new Point(event.x, event.y));
      }
    }

    // Move around notes & transforms
    //
    if (selectedNote != null) {
      if (lastButton == 1 && !shift) {
        /*
         * One or more notes are selected and moved around...
         *
         * new : new position of the note (not the mouse pointer) dx : difference with previous position
         */
        int dx = note.x - selectedNote.getLocation().x;
        int dy = note.y - selectedNote.getLocation().y;

        moveSelected(dx, dy);

        doRedraw = true;
      }
    }

    Cursor cursor = null;
    // Change cursor when dragging view or view port
    if (viewDrag || viewPortNavigation) {
      cursor = getDisplay().getSystemCursor(SWT.CURSOR_SIZEALL);
    }
    // Change cursor when selecting a region
    else if (selectionRegion != null) {
      cursor = getDisplay().getSystemCursor(SWT.CURSOR_CROSS);
    }
    // Change cursor when hover an hop or an area that support hover
    else if (hop != null
        || (areaOwner != null
            && areaOwner.getAreaType() != null
            && areaOwner.getAreaType().isSupportHover())) {
      cursor = getDisplay().getSystemCursor(SWT.CURSOR_HAND);
    }
    setCursor(cursor);

    if (doRedraw) {
      redraw();
    }
  }

  @Override
  public void mouseHover(MouseEvent event) {
    boolean tip = true;

    toolTip.setVisible(false);
    Point real = screen2real(event.x, event.y);

    // Show a tool tip upon mouse-over of an object on the canvas
    if (tip) {
      setToolTip(real.x, real.y, event.x, event.y);
    }
  }

  protected void moveSelected(int dx, int dy) {
    selectedNotes = pipelineMeta.getSelectedNotes();
    selectedTransforms = pipelineMeta.getSelectedTransforms();

    // Check minimum location of selected elements
    if (selectedTransforms != null) {
      for (TransformMeta transformMeta : selectedTransforms) {
        Point location = transformMeta.getLocation();
        if (location.x + dx < 0) {
          dx = -location.x;
        }
        if (location.y + dy < 0) {
          dy = -location.y;
        }
      }
    }
    if (selectedNotes != null) {
      for (NotePadMeta notePad : selectedNotes) {
        Point location = notePad.getLocation();
        if (location.x + dx < 0) {
          dx = -location.x;
        }
        if (location.y + dy < 0) {
          dy = -location.y;
        }
      }
    }

    // Adjust location of selected transforms...
    if (selectedTransforms != null) {
      for (TransformMeta transformMeta : selectedTransforms) {
        PropsUi.setLocation(
            transformMeta, transformMeta.getLocation().x + dx, transformMeta.getLocation().y + dy);
      }
    }
    // Adjust location of selected hops...
    if (selectedNotes != null) {
      for (NotePadMeta notePadMeta : selectedNotes) {
        PropsUi.setLocation(
            notePadMeta, notePadMeta.getLocation().x + dx, notePadMeta.getLocation().y + dy);
      }
    }
  }

  private void addCandidateAsHop(int mouseX, int mouseY) {

    boolean forward = startHopTransform != null;

    TransformMeta fromTransform = candidate.getFromTransform();
    TransformMeta toTransform = candidate.getToTransform();
    if (fromTransform.equals(toTransform)) {
      return; // Don't add
    }

    // See what the options are.
    // - Does the source transform has multiple stream options?
    // - Does the target transform have multiple input stream options?
    //
    List<IStream> streams = new ArrayList<>();

    ITransformIOMeta fromIoMeta = fromTransform.getTransform().getTransformIOMeta();
    List<IStream> targetStreams = fromIoMeta.getTargetStreams();
    if (forward) {
      streams.addAll(targetStreams);
    }

    ITransformIOMeta toIoMeta = toTransform.getTransform().getTransformIOMeta();
    List<IStream> infoStreams = toIoMeta.getInfoStreams();
    if (!forward) {
      streams.addAll(infoStreams);
    }

    if (forward) {
      if (fromIoMeta.isOutputProducer() && toTransform.equals(currentTransform)) {
        streams.add(
            new Stream(
                StreamType.OUTPUT,
                fromTransform,
                BaseMessages.getString(PKG, "HopGui.Hop.MainOutputOfTransform"),
                StreamIcon.OUTPUT,
                null));
      }

      if (fromTransform.supportsErrorHandling() && toTransform.equals(currentTransform)) {
        streams.add(
            new Stream(
                StreamType.ERROR,
                fromTransform,
                BaseMessages.getString(PKG, "HopGui.Hop.ErrorHandlingOfTransform"),
                StreamIcon.ERROR,
                null));
      }
    } else {
      if (toIoMeta.isInputAcceptor() && fromTransform.equals(currentTransform)) {
        streams.add(
            new Stream(
                StreamType.INPUT,
                toTransform,
                BaseMessages.getString(PKG, "HopGui.Hop.MainInputOfTransform"),
                StreamIcon.INPUT,
                null));
      }

      if (fromTransform.supportsErrorHandling() && fromTransform.equals(currentTransform)) {
        streams.add(
            new Stream(
                StreamType.ERROR,
                fromTransform,
                BaseMessages.getString(PKG, "HopGui.Hop.ErrorHandlingOfTransform"),
                StreamIcon.ERROR,
                null));
      }
    }

    // Targets can be dynamically added to this transform...
    //
    if (forward) {
      streams.addAll(fromTransform.getTransform().getOptionalStreams());
    } else {
      streams.addAll(toTransform.getTransform().getOptionalStreams());
    }

    // Show a list of options on the canvas...
    //
    if (streams.size() > 1) {
      // Show a pop-up menu with all the possible options...
      //
      Menu menu = new Menu(canvas);
      for (final IStream stream : streams) {
        MenuItem item = new MenuItem(menu, SWT.NONE);
        item.setText(Const.NVL(stream.getDescription(), ""));
        item.setImage(getImageFor(stream));
        item.addSelectionListener(
            new SelectionAdapter() {
              @Override
              public void widgetSelected(SelectionEvent e) {
                addHop(stream);
              }
            });
      }
      menu.setLocation(canvas.toDisplay(mouseX, mouseY));
      menu.setVisible(true);

      return;
    }
    if (streams.size() == 1) {
      addHop(streams.get(0));
    } else {
      return;
    }

    candidate = null;
    selectedTransforms = null;
    startHopTransform = null;
    endHopLocation = null;
    startErrorHopTransform = false;
  }

  private Image getImageFor(IStream stream) {
    Display disp = hopDisplay();
    SwtUniversalImage swtImage =
        SwtGc.getNativeImage(BasePainter.getStreamIconImage(stream.getStreamIcon(), true));
    return swtImage.getAsBitmapForSize(disp, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  protected void addHop(IStream stream) {
    if (candidate == null) {
      return;
    }
    switch (stream.getStreamType()) {
      case ERROR:
        addErrorHop();
        candidate.setErrorHop(true);
        pipelineHopDelegate.newHop(pipelineMeta, candidate);
        break;
      case INPUT:
        pipelineHopDelegate.newHop(pipelineMeta, candidate);
        break;
      case OUTPUT:
        TransformErrorMeta transformErrorMeta =
            candidate.getFromTransform().getTransformErrorMeta();
        if (transformErrorMeta != null && transformErrorMeta.getTargetTransform() != null) {
          if (transformErrorMeta.getTargetTransform().equals(candidate.getToTransform())) {
            candidate.getFromTransform().setTransformErrorMeta(null);
          }
        }
        pipelineHopDelegate.newHop(pipelineMeta, candidate);
        break;
      case INFO:
        stream.setTransformMeta(candidate.getFromTransform());
        candidate.getToTransform().getTransform().handleStreamSelection(stream);
        pipelineHopDelegate.newHop(pipelineMeta, candidate);
        break;
      case TARGET:
        // We connect a target of the source transform to an output transform...
        //
        stream.setTransformMeta(candidate.getToTransform());
        candidate.getFromTransform().getTransform().handleStreamSelection(stream);
        pipelineHopDelegate.newHop(pipelineMeta, candidate);
        break;
      default:
        break;
    }
    clearSettings();
  }

  private void addErrorHop() {
    // Automatically configure the transform error handling too!
    //
    if (candidate == null || candidate.getFromTransform() == null) {
      return;
    }
    TransformErrorMeta errorMeta = candidate.getFromTransform().getTransformErrorMeta();
    if (errorMeta == null) {
      errorMeta = new TransformErrorMeta(candidate.getFromTransform());
    }
    errorMeta.setEnabled(true);
    errorMeta.setTargetTransform(candidate.getToTransform());
    candidate.getFromTransform().setTransformErrorMeta(errorMeta);
  }

  @Override
  public void mouseEnter(MouseEvent arg0) {
    // Do nothing
  }

  @Override
  public void mouseExit(MouseEvent arg0) {
    // Do nothing
  }

  protected void asyncRedraw() {
    hopDisplay()
        .asyncExec(
            () -> {
              if (!HopGuiPipelineGraph.this.isDisposed()) {
                HopGuiPipelineGraph.this.redraw();
              }
            });
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_ZOOM_LEVEL,
      label = "i18n:org.apache.hop.ui.hopgui:HopGui.Toolbar.Zoom",
      toolTip = "i18n::HopGuiPipelineGraph.GuiAction.ZoomInOut.Tooltip",
      type = GuiToolbarElementType.COMBO,
      alignRight = true,
      comboValuesMethod = "getZoomLevels")
  public void zoomLevel() {
    readMagnification();
    redraw();
  }

  @Override
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_ZOOM_IN,
      toolTip = "i18n::HopGuiPipelineGraph.GuiAction.ZoomIn.Tooltip",
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/zoom-in.svg")
  public void zoomIn() {
    super.zoomIn();
  }

  @Override
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_ZOOM_OUT,
      toolTip = "i18n::HopGuiPipelineGraph.GuiAction.ZoomOut.Tooltip",
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/zoom-out.svg")
  public void zoomOut() {
    super.zoomOut();
  }

  @Override
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_ZOOM_TO_FIT,
      toolTip = "i18n::HopGuiPipelineGraph.GuiAction.ZoomFitToScreen.Tooltip",
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/zoom-fit.svg")
  public void zoomFitToScreen() {
    super.zoomFitToScreen();
  }

  @Override
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_ZOOM_100PCT,
      toolTip = "i18n::HopGuiPipelineGraph.GuiAction.Zoom100.Tooltip",
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/zoom-100.svg")
  public void zoom100Percent() {
    super.zoom100Percent();
  }

  public List<String> getZoomLevels() {
    return Arrays.asList(PipelinePainter.magnificationDescriptions);
  }

  private void addToolBar() {
    try {
      // Create a new toolbar at the top of the main composite...
      //
      toolBar = new ToolBar(this, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
      toolBarWidgets = new GuiToolbarWidgets();
      toolBarWidgets.registerGuiPluginObject(this);
      toolBarWidgets.createToolbarWidgets(toolBar, GUI_PLUGIN_TOOLBAR_PARENT_ID);
      FormData layoutData = new FormData();
      layoutData.left = new FormAttachment(0, 0);
      layoutData.top = new FormAttachment(0, 0);
      layoutData.right = new FormAttachment(100, 0);
      toolBar.setLayoutData(layoutData);
      toolBar.pack();
      PropsUi.setLook(toolBar, Props.WIDGET_STYLE_TOOLBAR);

      // enable / disable the icons in the toolbar too.
      //
      updateGui();

    } catch (Throwable t) {
      log.logError("Error setting up the navigation toolbar for HopUI", t);
      new ErrorDialog(
          hopShell(),
          CONST_ERROR,
          "Error setting up the navigation toolbar for HopGUI",
          new Exception(t));
    }
  }

  @Override
  public void setZoomLabel() {
    Combo combo = (Combo) toolBarWidgets.getWidgetsMap().get(TOOLBAR_ITEM_ZOOM_LEVEL);
    if (combo == null || combo.isDisposed()) {
      return;
    }
    String newString = Math.round(magnification * 100) + "%";
    String oldString = combo.getText();
    if (!newString.equals(oldString)) {
      combo.setText(Math.round(magnification * 100) + "%");
    }
  }

  /** Allows for magnifying to any percentage entered by the user... */
  private void readMagnification() {
    Combo zoomLabel = (Combo) toolBarWidgets.getWidgetsMap().get(TOOLBAR_ITEM_ZOOM_LEVEL);
    if (zoomLabel == null) {
      return;
    }
    String possibleText = zoomLabel.getText().replace("%", "");

    float possibleFloatMagnification;
    try {
      possibleFloatMagnification = Float.parseFloat(possibleText) / 100;
      magnification = possibleFloatMagnification;
      if (zoomLabel.getText().indexOf('%') < 0) {
        zoomLabel.setText(zoomLabel.getText().concat("%"));
      }
    } catch (Exception e) {
      modalMessageDialog(
          BaseMessages.getString(PKG, "PipelineGraph.Dialog.InvalidZoomMeasurement.Title"),
          BaseMessages.getString(
              PKG, "PipelineGraph.Dialog.InvalidZoomMeasurement.Message", zoomLabel.getText()),
          SWT.YES | SWT.ICON_ERROR);
    }

    canvas.setFocus();
    redraw();
  }

  protected void hideToolTips() {
    toolTip.setVisible(false);
  }

  /**
   * Select all the transforms and notes in a certain (screen) rectangle
   *
   * @param rect The selection area as a rectangle
   */
  public void selectInRect(PipelineMeta pipelineMeta, Rectangle rect) {

    // Normalize the selection area
    // Only for people not dragging from left top to right bottom
    if (rect.height < 0) {
      rect.y = rect.y + rect.height;
      rect.height = -rect.height;
    }
    if (rect.width < 0) {
      rect.x = rect.x + rect.width;
      rect.width = -rect.width;
    }

    for (TransformMeta transform : pipelineMeta.getTransforms()) {
      if (rect.contains(transform.getLocation())) {
        transform.setSelected(true);
      }
    }
    for (NotePadMeta note : pipelineMeta.getNotes()) {
      Point a = note.getLocation();
      Point b = new Point(a.x + note.width, a.y + note.height);
      if (rect.contains(a) && rect.contains(b)) {
        note.setSelected(true);
      }
    }
  }

  @Override
  public boolean setFocus() {
    return (canvas != null && !canvas.isDisposed()) ? canvas.setFocus() : false;
  }

  public void renameTransform(TransformMeta transformMeta, String transformName) {
    String newname = transformName;

    TransformMeta smeta = pipelineMeta.findTransform(newname, transformMeta);
    int nr = 2;
    while (smeta != null) {
      newname = transformName + " " + nr;
      smeta = pipelineMeta.findTransform(newname);
      nr++;
    }
    if (nr > 2) {
      transformName = newname;
      modalMessageDialog(
          BaseMessages.getString(PKG, "HopGui.Dialog.TransformnameExists.Title"),
          BaseMessages.getString(PKG, "HopGui.Dialog.TransformnameExists.Message", transformName),
          SWT.OK | SWT.ICON_INFORMATION);
    }
    transformMeta.setName(transformName);
    transformMeta.setChanged();
    redraw();
  }

  public void clearSettings() {
    selectedTransform = null;
    noInputTransform = null;
    selectedNote = null;
    selectedTransforms = null;
    selectionRegion = null;
    candidate = null;
    lastHopSplit = null;
    lastButton = 0;
    iconOffset = null;
    startHopTransform = null;
    endHopTransform = null;
    endHopLocation = null;
    pipelineMeta.unselectAll();
    for (int i = 0; i < pipelineMeta.nrPipelineHops(); i++) {
      pipelineMeta.getPipelineHop(i).setSplit(false);
    }
  }

  /**
   * See if location (x,y) is on a line between two transforms: the hop!
   *
   * @param x
   * @param y
   * @return the pipeline hop on the specified location, otherwise: null
   */
  protected PipelineHopMeta findPipelineHop(int x, int y) {
    return findPipelineHop(x, y, null);
  }

  /**
   * See if location (x,y) is on a line between two transforms: the hop!
   *
   * @param x
   * @param y
   * @param exclude the transform to exclude from the hops (from or to location). Specify null if no
   *     transform is to be excluded.
   * @return the pipeline hop on the specified location, otherwise: null
   */
  private PipelineHopMeta findPipelineHop(int x, int y, TransformMeta exclude) {
    int i;
    PipelineHopMeta online = null;
    for (i = 0; i < pipelineMeta.nrPipelineHops(); i++) {
      PipelineHopMeta hi = pipelineMeta.getPipelineHop(i);
      TransformMeta fs = hi.getFromTransform();
      TransformMeta ts = hi.getToTransform();

      if (fs == null || ts == null) {
        return null;
      }

      // If either the "from" or "to" transform is excluded, skip this hop.
      //
      if (exclude != null && (exclude.equals(fs) || exclude.equals(ts))) {
        continue;
      }

      int[] line = getLine(fs, ts);

      if (pointOnLine(x, y, line)) {
        online = hi;
      }
    }
    return online;
  }

  private int[] getLine(TransformMeta fs, TransformMeta ts) {
    Point from = fs.getLocation();
    Point to = ts.getLocation();

    int x1 = from.x + iconSize / 2;
    int y1 = from.y + iconSize / 2;

    int x2 = to.x + iconSize / 2;
    int y2 = to.y + iconSize / 2;

    return new int[] {x1, y1, x2, y2};
  }

  @GuiContextAction(
      id = "pipeline-graph-transform-90000-transform-help",
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Info,
      name = "i18n::System.Button.Help",
      tooltip = "i18n::System.Tooltip.Help",
      image = "ui/images/help.svg",
      category = "Basic",
      categoryOrder = "1")
  public void openTransformHelp(HopGuiPipelineTransformContext context) {
    IPlugin plugin =
        PluginRegistry.getInstance()
            .getPlugin(TransformPluginType.class, context.getTransformMeta().getPluginId());

    HelpUtils.openHelp(getShell(), plugin);
  }

  @GuiContextAction(
      id = "pipeline-graph-transform-10100-transform-detach",
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiPipelineGraph.TransformAction.DetachTransform.Name",
      tooltip = "i18n::HopGuiPipelineGraph.TransformAction.DetachTransform.Tooltip",
      image = "ui/images/hop-delete.svg",
      category = "Basic",
      categoryOrder = "1")
  public void detachTransform(HopGuiPipelineTransformContext context) {
    TransformMeta transformMeta = context.getTransformMeta();
    PipelineHopMeta fromHop = pipelineMeta.findPipelineHopTo(transformMeta);
    PipelineHopMeta toHop = pipelineMeta.findPipelineHopFrom(transformMeta);

    for (int i = pipelineMeta.nrPipelineHops() - 1; i >= 0; i--) {
      PipelineHopMeta hop = pipelineMeta.getPipelineHop(i);
      if (transformMeta.equals(hop.getFromTransform())
          || transformMeta.equals(hop.getToTransform())) {
        // Transform is connected with a hop, remove this hop.
        //
        hopGui.undoDelegate.addUndoNew(pipelineMeta, new PipelineHopMeta[] {hop}, new int[] {i});
        pipelineMeta.removePipelineHop(i);
      }
    }

    // If the transform was part of a chain, re-connect it.
    //
    if (fromHop != null && toHop != null) {
      pipelineHopDelegate.newHop(
          pipelineMeta, new PipelineHopMeta(fromHop.getFromTransform(), toHop.getToTransform()));
    }

    updateGui();
  }

  @GuiContextAction(
      id = "pipeline-graph-transform-10700-partitioning",
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiPipelineGraph.TransformAction.Partitioning.Name",
      tooltip = "i18n::HopGuiPipelineGraph.TransformAction.Partitioning.Tooltip",
      image = "ui/images/partition_schema.svg",
      category = "Data routing",
      categoryOrder = "2")
  public void partitioning(HopGuiPipelineTransformContext context) {
    pipelineTransformDelegate.editTransformPartitioning(pipelineMeta, context.getTransformMeta());
  }

  @GuiContextAction(
      id = ACTION_ID_PIPELINE_GRAPH_TRANSFORM_ERROR_HANDLING,
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiPipelineGraph.TransformAction.ErrorHandling.Name",
      tooltip = "i18n::HopGuiPipelineGraph.TransformAction.ErrorHandling.Tooltip",
      image = "ui/images/error.svg",
      category = "Data routing",
      categoryOrder = "2")
  public void errorHandling(HopGuiPipelineTransformContext context) {
    pipelineTransformDelegate.editTransformErrorHandling(pipelineMeta, context.getTransformMeta());
  }

  public void newHopChoice() {
    selectedTransforms = null;
    newHop();
  }

  @GuiContextAction(
      id = "pipeline-graph-transform-10000-edit",
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiPipelineGraph.TransformAction.EditTransform.Name",
      tooltip = "i18n::HopGuiPipelineGraph.TransformAction.EditTransform.Tooltip",
      image = "ui/images/edit.svg",
      category = "Basic",
      categoryOrder = "1")
  public void editTransform(HopGuiPipelineTransformContext context) {
    editTransform(context.getTransformMeta());
  }

  public void editTransform() {
    selectedTransforms = null;
    editTransform(getCurrentTransform());
  }

  @GuiContextAction(
      id = "pipeline-graph-transform-10800-edit-description",
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiPipelineGraph.TransformAction.EditDescription.Name",
      tooltip = "i18n::HopGuiPipelineGraph.TransformAction.EditDescription.Tooltip",
      image = "ui/images/edit_description.svg",
      category = "Basic",
      categoryOrder = "1")
  public void editDescription(HopGuiPipelineTransformContext context) {
    editDescription(context.getTransformMeta());
  }

  @GuiContextAction(
      id = ACTION_ID_PIPELINE_GRAPH_TRANSFORM_ROWS_DISTRIBUTE,
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiPipelineGraph.TransformAction.DistributeRows.Name",
      tooltip = "i18n::HopGuiPipelineGraph.TransformAction.DistributeRows.Tooltip",
      image = "ui/images/distribute.svg",
      category = "Data routing",
      categoryOrder = "2")
  public void setDistributes(HopGuiPipelineTransformContext context) {
    context.getTransformMeta().setDistributes(true);
    context.getTransformMeta().setRowDistribution(null);
    redraw();
  }

  @GuiContextAction(
      id = ACTION_ID_PIPELINE_GRAPH_TRANSFORM_ROWS_COPY,
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiPipelineGraph.TransformAction.CopyRows.Name",
      tooltip = "i18n::HopGuiPipelineGraph.TransformAction.CopyRows.Tooltip",
      image = "ui/images/copy-rows.svg",
      category = "Data routing",
      categoryOrder = "2")
  public void setCopies(HopGuiPipelineTransformContext context) {
    context.getTransformMeta().setDistributes(false);
    context.getTransformMeta().setRowDistribution(null);
    redraw();
  }

  /**
   * Implement HOP-530 before re-enabling @GuiContextAction( id =
   * "pipeline-graph-transform-10500-custom-row-distribution", parentId =
   * HopGuiPipelineTransformContext.CONTEXT_ID, type = GuiActionType.Modify, name = "Specify row
   * distribution", tooltip = "Specify how the transform should distribute rows to next transforms",
   * image = "ui/images/Edit.svg" ) public void setCustomRowDistribution(
   * HopGuiPipelineTransformContext context ) { // ask user which row distribution is needed... //
   * IRowDistribution rowDistribution = askUserForCustomDistributionMethod();
   * context.getTransformMeta().setDistributes( true );
   * context.getTransformMeta().setRowDistribution( rowDistribution ); redraw(); }
   */
  public IRowDistribution askUserForCustomDistributionMethod() {
    List<IPlugin> plugins =
        PluginRegistry.getInstance().getPlugins(RowDistributionPluginType.class);
    if (Utils.isEmpty(plugins)) {
      return null;
    }
    List<String> choices = new ArrayList<>();
    for (IPlugin plugin : plugins) {
      choices.add(plugin.getName() + " : " + plugin.getDescription());
    }
    EnterSelectionDialog dialog =
        new EnterSelectionDialog(
            hopShell(),
            choices.toArray(new String[choices.size()]),
            BaseMessages.getString(PKG, "HopGuiPipelineGraph.DistributionMethodDialog.Header"),
            BaseMessages.getString(PKG, "HopGuiPipelineGraph.DistributionMethodDialog.Text"));
    if (dialog.open() != null) {
      IPlugin plugin = plugins.get(dialog.getSelectionNr());
      try {
        return (IRowDistribution) PluginRegistry.getInstance().loadClass(plugin);
      } catch (Exception e) {
        new ErrorDialog(hopShell(), CONST_ERROR, "Error loading row distribution plugin class", e);
        return null;
      }
    } else {
      return null;
    }
  }

  @GuiContextAction(
      id = ACTION_ID_PIPELINE_GRAPH_TRANSFORM_SPECIFY_COPIES,
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiPipelineGraph.TransformAction.SpecifyCopies.Name",
      tooltip = "i18n::HopGuiPipelineGraph.TransformAction.SpecifyCopies.Tooltip",
      image = "ui/images/exponent.svg",
      category = "Data routing",
      categoryOrder = "2")
  public void copies(HopGuiPipelineTransformContext context) {
    TransformMeta transformMeta = context.getTransformMeta();
    copies(transformMeta);
  }

  public void copies(TransformMeta transformMeta) {
    final boolean multipleOK = checkNumberOfCopies(pipelineMeta, transformMeta);
    selectedTransforms = null;
    String tt = BaseMessages.getString(PKG, "PipelineGraph.Dialog.NrOfCopiesOfTransform.Title");
    String mt = BaseMessages.getString(PKG, "PipelineGraph.Dialog.NrOfCopiesOfTransform.Message");
    EnterStringDialog nd =
        new EnterStringDialog(hopShell(), transformMeta.getCopiesString(), tt, mt, true, variables);
    String cop = nd.open();
    if (!Utils.isEmpty(cop)) {

      int copies = Const.toInt(hopGui.getVariables().resolve(cop), -1);
      if (copies > 1 && !multipleOK) {
        cop = "1";

        modalMessageDialog(
            BaseMessages.getString(
                PKG, "PipelineGraph.Dialog.MultipleCopiesAreNotAllowedHere.Title"),
            BaseMessages.getString(
                PKG, "PipelineGraph.Dialog.MultipleCopiesAreNotAllowedHere.Message"),
            SWT.YES | SWT.ICON_WARNING);
      }
      String cps = transformMeta.getCopiesString();
      if ((cps != null && !cps.equals(cop)) || (cps == null && cop != null)) {
        transformMeta.setChanged();
      }
      transformMeta.setCopiesString(cop);
      redraw();
    }
  }

  @GuiContextAction(
      id = "pipeline-graph-transform-10900-delete",
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Delete,
      name = "i18n::HopGuiPipelineGraph.TransformAction.DeleteTransform.Name",
      tooltip = "i18n::HopGuiPipelineGraph.TransformAction.DeleteTransform.Tooltip",
      image = "ui/images/delete.svg",
      category = "Basic",
      categoryOrder = "1")
  public void delTransform(HopGuiPipelineTransformContext context) {
    delSelected(context.getTransformMeta());
  }

  @GuiContextAction(
      id = "pipeline-graph-transform-10200-fields-before",
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Info,
      name = "i18n::HopGuiPipelineGraph.TransformAction.Transform.ShowInputFields.Name",
      tooltip = "i18n::HopGuiPipelineGraph.TransformAction.Transform.ShowInputFields.Tooltip",
      image = "ui/images/input.svg",
      category = "Basic",
      categoryOrder = "1")
  public void fieldsBefore(HopGuiPipelineTransformContext context) {
    selectedTransforms = null;
    inputOutputFields(context.getTransformMeta(), true);
  }

  @GuiContextAction(
      id = "pipeline-graph-transform-10300-fields-after",
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Info,
      name = "i18n::HopGuiPipelineGraph.TransformAction.Transform.ShowOutputFields.Name",
      tooltip = "i18n::HopGuiPipelineGraph.TransformAction.Transform.ShowOutputFields.Tooltip",
      image = "ui/images/output.svg",
      category = "Basic",
      categoryOrder = "1")
  public void fieldsAfter(HopGuiPipelineTransformContext context) {
    selectedTransforms = null;
    inputOutputFields(context.getTransformMeta(), false);
  }

  public void fieldsLineage() {
    PipelineDataLineage tdl = new PipelineDataLineage(pipelineMeta);
    try {
      tdl.calculateLineage(variables);
    } catch (Exception e) {
      new ErrorDialog(hopShell(), "Lineage error", "Unexpected lineage calculation error", e);
    }
  }

  @GuiContextAction(
      id = ACTION_ID_PIPELINE_GRAPH_HOP_ENABLE,
      parentId = HopGuiPipelineHopContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiPipelineGraph.HopAction.EnableHop.Name",
      tooltip = "i18n::HopGuiPipelineGraph.HopAction.EnableHop.Tooltip",
      image = "ui/images/hop.svg",
      category = "Basic",
      categoryOrder = "1")
  public void enableHop(HopGuiPipelineHopContext context) {
    PipelineHopMeta hop = context.getHopMeta();
    if (!hop.isEnabled()) {
      PipelineHopMeta before = hop.clone();
      setHopEnabled(hop, true);
      if (pipelineMeta.hasLoop(hop.getToTransform())) {
        setHopEnabled(hop, false);
        modalMessageDialog(
            BaseMessages.getString(PKG, "PipelineGraph.Dialog.LoopAfterHopEnabled.Title"),
            BaseMessages.getString(PKG, "PipelineGraph.Dialog.LoopAfterHopEnabled.Message"),
            SWT.OK | SWT.ICON_ERROR);
      } else {
        PipelineHopMeta after = hop.clone();
        hopGui.undoDelegate.addUndoChange(
            pipelineMeta,
            new PipelineHopMeta[] {before},
            new PipelineHopMeta[] {after},
            new int[] {pipelineMeta.indexOfPipelineHop(hop)});
        redraw();
      }
    }
    updateErrorMetaForHop(hop);
  }

  /**
   * We're filtering out the disable action for hops which are already disabled. The same for the
   * enabled hops.
   *
   * @param contextActionId
   * @param context
   * @return True if the action should be shown and false otherwise.
   */
  @GuiContextActionFilter(parentId = HopGuiPipelineHopContext.CONTEXT_ID)
  public boolean filterHopActions(String contextActionId, HopGuiPipelineHopContext context) {
    if (contextActionId.equals(ACTION_ID_PIPELINE_GRAPH_HOP_ENABLE)) {
      return !context.getHopMeta().isEnabled();
    }
    if (contextActionId.equals(ACTION_ID_PIPELINE_GRAPH_HOP_DISABLE)) {
      return context.getHopMeta().isEnabled();
    }

    return true;
  }

  /**
   * We're filtering out certain actions for transforms which don't make sense.
   *
   * @param contextActionId
   * @param context
   * @return True if the action should be shown and false otherwise.
   */
  @GuiContextActionFilter(parentId = HopGuiPipelineTransformContext.CONTEXT_ID)
  public boolean filterTransformActions(
      String contextActionId, HopGuiPipelineTransformContext context) {
    if (contextActionId.equals(ACTION_ID_PIPELINE_GRAPH_TRANSFORM_ROWS_DISTRIBUTE)) {
      return !context.getTransformMeta().isDistributes();
    }
    if (contextActionId.equals(ACTION_ID_PIPELINE_GRAPH_TRANSFORM_ROWS_COPY)) {
      return context.getTransformMeta().isDistributes();
    }
    if (contextActionId.equals(ACTION_ID_PIPELINE_GRAPH_TRANSFORM_SPECIFY_COPIES)) {
      return context.getTransformMeta().supportsMultiCopyExecution();
    }
    if (contextActionId.equals(ACTION_ID_PIPELINE_GRAPH_TRANSFORM_ERROR_HANDLING)) {
      return context.getTransformMeta().supportsErrorHandling();
    }

    if (contextActionId.equals(ACTION_ID_PIPELINE_GRAPH_TRANSFORM_VIEW_EXECUTION_INFO)) {
      // See if the pipeline is running and that the transform is running in one or more copies.
      // Also disable this if the running pipeline doesn't have a location configured.
      //
      if (pipeline == null) {
        return false;
      }
      PipelineRunConfiguration runConfiguration = pipeline.getPipelineRunConfiguration();
      String runConfigName = variables.resolve(runConfiguration.getExecutionInfoLocationName());
      if (StringUtils.isEmpty(runConfigName)) {
        return false;
      }

      TransformMeta transformMeta = context.getTransformMeta();
      List<IEngineComponent> componentCopies = pipeline.getComponentCopies(transformMeta.getName());
      return componentCopies != null && !componentCopies.isEmpty();
    }

    return true;
  }

  @GuiContextAction(
      id = ACTION_ID_PIPELINE_GRAPH_HOP_DISABLE,
      parentId = HopGuiPipelineHopContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiPipelineGraph.HopAction.DisableHop.Name",
      tooltip = "i18n::HopGuiPipelineGraph.HopAction.DisableHop.Tooltip",
      image = "ui/images/hop-disable.svg",
      category = "Basic",
      categoryOrder = "1")
  public void disableHop(HopGuiPipelineHopContext context) {
    PipelineHopMeta hopMeta = context.getHopMeta();
    if (hopMeta.isEnabled()) {
      PipelineHopMeta before = hopMeta.clone();
      setHopEnabled(hopMeta, false);

      PipelineHopMeta after = hopMeta.clone();
      hopGui.undoDelegate.addUndoChange(
          pipelineMeta,
          new PipelineHopMeta[] {before},
          new PipelineHopMeta[] {after},
          new int[] {pipelineMeta.indexOfPipelineHop(hopMeta)});
      redraw();
    }
    updateErrorMetaForHop(hopMeta);
  }

  @GuiContextAction(
      id = "pipeline-graph-hop-10020-hop-delete",
      parentId = HopGuiPipelineHopContext.CONTEXT_ID,
      type = GuiActionType.Delete,
      name = "i18n::HopGuiPipelineGraph.HopAction.DeleteHop.Name",
      tooltip = "i18n::HopGuiPipelineGraph.HopAction.DeleteHop.Tooltip",
      image = "ui/images/hop-delete.svg",
      category = "Basic",
      categoryOrder = "1")
  public void deleteHop(HopGuiPipelineHopContext context) {
    pipelineHopDelegate.delHop(pipelineMeta, context.getHopMeta());
  }

  private void updateErrorMetaForHop(PipelineHopMeta hop) {
    if (hop != null && hop.isErrorHop()) {
      TransformErrorMeta errorMeta = hop.getFromTransform().getTransformErrorMeta();
      if (errorMeta != null) {
        errorMeta.setEnabled(hop.isEnabled());
      }
    }
  }

  @GuiContextAction(
      id = "pipeline-graph-hop-10065-hop-enable-between-selected-transforms",
      parentId = HopGuiPipelineHopContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiPipelineGraph.HopAction.EnableBetweenSelectedTransforms.Name",
      tooltip = "i18n::HopGuiPipelineGraph.HopAction.EnableBetweenSelectedTransforms.Tooltip",
      image = "ui/images/hop-enable-between-selected.svg",
      category = "Bulk",
      categoryOrder = "2")
  public void enableHopsBetweenSelectedTransforms(final HopGuiPipelineHopContext context) {
    enableHopsBetweenSelectedTransforms(true);
  }

  @GuiContextAction(
      id = "pipeline-graph-hop-10075-hop-disable-between-selected-transforms",
      parentId = HopGuiPipelineHopContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiPipelineGraph.HopAction.DisableBetweenSelectedTransforms.Name",
      tooltip = "i18n::HopGuiPipelineGraph.HopAction.DisableBetweenSelectedTransforms.Tooltip",
      image = "ui/images/hop-disable-between-selected.svg",
      category = "Bulk",
      categoryOrder = "2")
  public void disableHopsBetweenSelectedTransforms(final HopGuiPipelineHopContext context) {
    enableHopsBetweenSelectedTransforms(false);
  }

  /** This method enables or disables all the hops between the selected transforms. */
  public void enableHopsBetweenSelectedTransforms(boolean enabled) {
    List<TransformMeta> list = pipelineMeta.getSelectedTransforms();

    boolean hasLoop = false;

    for (int i = 0; i < pipelineMeta.nrPipelineHops(); i++) {
      PipelineHopMeta hop = pipelineMeta.getPipelineHop(i);
      if (list.contains(hop.getFromTransform()) && list.contains(hop.getToTransform())) {

        PipelineHopMeta before = hop.clone();
        setHopEnabled(hop, enabled);
        PipelineHopMeta after = hop.clone();
        hopGui.undoDelegate.addUndoChange(
            pipelineMeta,
            new PipelineHopMeta[] {before},
            new PipelineHopMeta[] {after},
            new int[] {pipelineMeta.indexOfPipelineHop(hop)});

        if (pipelineMeta.hasLoop(hop.getToTransform())) {
          hasLoop = true;
          setHopEnabled(hop, false);
        }
      }
    }

    if (enabled && hasLoop) {
      modalMessageDialog(
          BaseMessages.getString(PKG, "PipelineGraph.Dialog.HopCausesLoop.Title"),
          BaseMessages.getString(PKG, "PipelineGraph.Dialog.HopCausesLoop.Message"),
          SWT.OK | SWT.ICON_ERROR);
    }

    updateGui();
  }

  @GuiContextAction(
      id = "pipeline-graph-hop-10060-hop-enable-downstream",
      parentId = HopGuiPipelineHopContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiPipelineGraph.HopAction.EnableDownstreamHop.Name",
      tooltip = "i18n::HopGuiPipelineGraph.HopAction.EnableDownstreamHop.Tooltip",
      image = "ui/images/hop-enable-downstream.svg",
      category = "Bulk",
      categoryOrder = "2")
  public void enableHopsDownstream(final HopGuiPipelineHopContext context) {
    enableDisableHopsDownstream(context.getHopMeta(), true);
  }

  @GuiContextAction(
      id = "pipeline-graph-hop-10070-hop-disable-downstream",
      parentId = HopGuiPipelineHopContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiPipelineGraph.HopAction.DisableDownstreamHop.Name",
      tooltip = "i18n::HopGuiPipelineGraph.HopAction.DisableDownstreamHop.Tooltip",
      image = "ui/images/hop-disable-downstream.svg",
      category = "Bulk",
      categoryOrder = "2")
  public void disableHopsDownstream(final HopGuiPipelineHopContext context) {
    enableDisableHopsDownstream(context.getHopMeta(), false);
  }

  public void enableDisableHopsDownstream(PipelineHopMeta hop, boolean enabled) {
    PipelineHopMeta before = hop.clone();
    setHopEnabled(hop, enabled);
    PipelineHopMeta after = hop.clone();
    hopGui.undoDelegate.addUndoChange(
        pipelineMeta,
        new PipelineHopMeta[] {before},
        new PipelineHopMeta[] {after},
        new int[] {pipelineMeta.indexOfPipelineHop(hop)});

    Set<TransformMeta> checkedTransforms =
        enableDisableNextHops(hop.getToTransform(), enabled, new HashSet<>());

    if (checkedTransforms.stream().anyMatch(entry -> pipelineMeta.hasLoop(entry))) {
      modalMessageDialog(
          BaseMessages.getString(PKG, "PipelineGraph.Dialog.HopCausesLoop.Title"),
          BaseMessages.getString(PKG, "PipelineGraph.Dialog.HopCausesLoop.Message"),
          SWT.OK | SWT.ICON_ERROR);
    }

    updateGui();
  }

  private Set<TransformMeta> enableDisableNextHops(
      TransformMeta from, boolean enabled, Set<TransformMeta> checkedEntries) {
    checkedEntries.add(from);
    pipelineMeta.getPipelineHops().stream()
        .filter(hop -> from.equals(hop.getFromTransform()))
        .forEach(
            hop -> {
              if (hop.isEnabled() != enabled) {
                PipelineHopMeta before = hop.clone();
                setHopEnabled(hop, enabled);
                PipelineHopMeta after = hop.clone();
                hopGui.undoDelegate.addUndoChange(
                    pipelineMeta,
                    new PipelineHopMeta[] {before},
                    new PipelineHopMeta[] {after},
                    new int[] {pipelineMeta.indexOfPipelineHop(hop)});
              }
              if (!checkedEntries.contains(hop.getToTransform())) {
                enableDisableNextHops(hop.getToTransform(), enabled, checkedEntries);
              }
            });
    return checkedEntries;
  }

  @GuiContextAction(
      id = "pipeline-graph-hop-10080-hop-insert-transform",
      parentId = HopGuiPipelineHopContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiPipelineGraph.HopAction.InsetTransform.Text",
      tooltip = "i18n::HopGuiPipelineGraph.HopAction.InsetTransform.Tooltip",
      image = "ui/images/add-item.svg",
      category = "Basic",
      categoryOrder = "13")
  public void insertTransform(HopGuiPipelineHopContext context) {
    // Build actions list
    //
    List<GuiAction> guiActions = new ArrayList<>();
    PluginRegistry registry = PluginRegistry.getInstance();
    for (IPlugin plugin : registry.getPlugins(TransformPluginType.class)) {

      GuiAction guiAction =
          new GuiAction(
              "pipeline-graph-insert-transform-" + plugin.getIds()[0],
              GuiActionType.Create,
              plugin.getName(),
              plugin.getDescription(),
              plugin.getImageFile(),
              (shiftClicked, controlClicked, t) ->
                  pipelineTransformDelegate.insertTransform(
                      pipelineMeta,
                      context.getHopMeta(),
                      plugin.getIds()[0],
                      plugin.getName(),
                      context.getClick()));
      guiAction.getKeywords().addAll(Arrays.asList(plugin.getKeywords()));
      guiAction.getKeywords().add(plugin.getCategory());
      guiAction.setCategory(plugin.getCategory());
      guiAction.setCategoryOrder(plugin.getCategory());
      try {
        guiAction.setClassLoader(registry.getClassLoader(plugin));
      } catch (HopPluginException e) {
        LogChannel.UI.logError(
            "Unable to get classloader for transform plugin " + plugin.getIds()[0], e);
      }
      guiActions.add(guiAction);
    }

    String message =
        BaseMessages.getString(
            PKG, "HopGuiPipelineGraph.ContextualActionDialog.InsertTransform.Header");

    ContextDialog contextDialog =
        new ContextDialog(
            hopShell(), message, context.getClick(), guiActions, HopGuiPipelineContext.CONTEXT_ID);

    GuiAction selectedAction = contextDialog.open();

    if (selectedAction != null) {
      IGuiActionLambda<?> actionLambda = selectedAction.getActionLambda();
      actionLambda.executeAction(contextDialog.isShiftClicked(), contextDialog.isCtrlClicked());
    }
  }

  @GuiContextAction(
      id = "pipeline-graph-10-edit-note",
      parentId = HopGuiPipelineNoteContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiPipelineGraph.NoteAction.EditNote.Name",
      tooltip = "i18n::HopGuiPipelineGraph.NoteAction.EditNote.Tooltip",
      image = "ui/images/edit.svg",
      category = "Basic",
      categoryOrder = "1")
  public void editNote(HopGuiPipelineNoteContext context) {
    selectionRegion = null;
    editNote(context.getNotePadMeta());
  }

  @GuiContextAction(
      id = "pipeline-graph-20-delete-note",
      parentId = HopGuiPipelineNoteContext.CONTEXT_ID,
      type = GuiActionType.Delete,
      name = "i18n::HopGuiPipelineGraph.NoteAction.DeleteNote.Name",
      tooltip = "i18n::HopGuiPipelineGraph.NoteAction.DeleteNote.Tooltip",
      image = "ui/images/delete.svg",
      category = "Basic",
      categoryOrder = "1")
  public void deleteNote(HopGuiPipelineNoteContext context) {
    selectionRegion = null;
    int idx = pipelineMeta.indexOfNote(context.getNotePadMeta());
    if (idx >= 0) {
      pipelineMeta.removeNote(idx);
      hopGui.undoDelegate.addUndoDelete(
          pipelineMeta, new NotePadMeta[] {context.getNotePadMeta().clone()}, new int[] {idx});
      updateGui();
    }
  }

  @GuiContextAction(
      id = "pipeline-graph-transform-10100-create-note",
      parentId = HopGuiPipelineContext.CONTEXT_ID,
      type = GuiActionType.Create,
      name = "i18n::HopGuiPipelineGraph.NoteAction.CreateNote.Name",
      tooltip = "i18n::HopGuiPipelineGraph.NoteAction.CreateNote.Tooltip",
      image = "ui/images/note-add.svg",
      category = "Basic",
      categoryOrder = "1")
  public void newNote(HopGuiPipelineContext context) {
    selectionRegion = null;
    String title = BaseMessages.getString(PKG, "PipelineGraph.Dialog.NoteEditor.Title");
    NotePadDialog dd = new NotePadDialog(variables, hopShell(), title);
    NotePadMeta n = dd.open();
    if (n != null) {
      NotePadMeta npi =
          new NotePadMeta(
              n.getNote(),
              context.getClick().x,
              context.getClick().y,
              ConstUi.NOTE_MIN_SIZE,
              ConstUi.NOTE_MIN_SIZE,
              n.getFontName(),
              n.getFontSize(),
              n.isFontBold(),
              n.isFontItalic(),
              n.getFontColorRed(),
              n.getFontColorGreen(),
              n.getFontColorBlue(),
              n.getBackGroundColorRed(),
              n.getBackGroundColorGreen(),
              n.getBackGroundColorBlue(),
              n.getBorderColorRed(),
              n.getBorderColorGreen(),
              n.getBorderColorBlue());
      pipelineMeta.addNote(npi);
      hopGui.undoDelegate.addUndoNew(
          pipelineMeta, new NotePadMeta[] {npi}, new int[] {pipelineMeta.indexOfNote(npi)});
      updateGui();
    }
  }

  @GuiContextAction(
      id = "pipeline-graph-transform-10110-copy-notepad-to-clipboard",
      parentId = HopGuiPipelineNoteContext.CONTEXT_ID,
      type = GuiActionType.Custom,
      name = "i18n::HopGuiPipelineGraph.PipelineAction.CopyToClipboard.Name",
      tooltip = "i18n::HopGuiPipelineGraph.PipelineAction.CopyToClipboard.Tooltip",
      image = "ui/images/copy.svg",
      category = "Basic",
      categoryOrder = "1")
  public void copyNotePadToClipboard(HopGuiPipelineNoteContext context) {
    pipelineClipboardDelegate.copySelected(
        pipelineMeta, Collections.emptyList(), Arrays.asList(context.getNotePadMeta()));
  }

  @GuiContextAction(
      id = "pipeline-graph-edit-pipeline",
      parentId = HopGuiPipelineContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiPipelineGraph.PipelineAction.EditPipeline.Name",
      tooltip = "i18n::HopGuiPipelineGraph.PipelineAction.EditPipeline.Tooltip",
      image = "ui/images/pipeline.svg",
      category = "Basic",
      categoryOrder = "1")
  public void editPipelineProperties(HopGuiPipelineContext context) {
    editProperties(pipelineMeta, hopGui, true);
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_EDIT_PIPELINE,
      toolTip = "i18n:org.apache.hop.ui.hopgui:HopGui.Toolbar.EditProperties.Tooltip",
      image = "ui/images/pipeline.svg",
      separator = true)
  @GuiKeyboardShortcut(control = true, key = 't')
  @GuiOsxKeyboardShortcut(command = true, key = 't')
  public void editPipelineProperties() {
    editProperties(pipelineMeta, hopGui, true);
  }

  public void newTransform(String description) {
    TransformMeta transformMeta =
        pipelineTransformDelegate.newTransform(
            pipelineMeta,
            null,
            description,
            description,
            false,
            true,
            new Point(currentMouseX, currentMouseY));
    PropsUi.setLocation(transformMeta, currentMouseX, currentMouseY);
    updateGui();
  }

  private boolean checkNumberOfCopies(PipelineMeta pipelineMeta, TransformMeta transformMeta) {
    boolean enabled = true;
    List<TransformMeta> prevTransforms = pipelineMeta.findPreviousTransforms(transformMeta);
    for (TransformMeta prevTransform : prevTransforms) {
      // See what the target transforms are.
      // If one of the target transforms is our original transform, we can't start multiple copies
      //
      String[] targetTransforms =
          prevTransform.getTransform().getTransformIOMeta().getTargetTransformNames();
      if (targetTransforms != null) {
        for (int t = 0; t < targetTransforms.length && enabled; t++) {
          if (!Utils.isEmpty(targetTransforms[t])
              && targetTransforms[t].equalsIgnoreCase(transformMeta.getName())) {
            enabled = false;
          }
        }
      }
    }
    return enabled;
  }

  private AreaOwner setToolTip(int x, int y, int screenX, int screenY) {
    AreaOwner subject = null;

    if (!hopGui.getProps().showToolTips() || openedContextDialog) {
      return subject;
    }

    canvas.setToolTipText(null);

    String newTip = null;
    Image tipImage = null;

    final PipelineHopMeta hi = findPipelineHop(x, y);
    // check the area owner list...
    //
    StringBuilder tip = new StringBuilder();
    AreaOwner areaOwner = getVisibleAreaOwner(x, y);
    if (areaOwner != null && areaOwner.getAreaType() != null) {
      AreaType areaType = areaOwner.getAreaType();
      switch (areaType) {
        case TRANSFORM_PARTITIONING:
          TransformMeta transform = (TransformMeta) areaOwner.getParent();
          tip.append("Transform partitioning:")
              .append(Const.CR)
              .append("-----------------------")
              .append(Const.CR);
          tip.append(transform.getTransformPartitioningMeta().toString()).append(Const.CR);
          if (transform.getTargetTransformPartitioningMeta() != null) {
            tip.append(Const.CR)
                .append(Const.CR)
                .append("TARGET: " + transform.getTargetTransformPartitioningMeta().toString())
                .append(Const.CR);
          }
          break;
        case TRANSFORM_FAILURE_ICON:
          String log = (String) areaOwner.getParent();
          tip.append(log);
          tipImage = GuiResource.getInstance().getImageFailure();
          break;
        case HOP_COPY_ICON:
          transform = (TransformMeta) areaOwner.getParent();
          tip.append(
              BaseMessages.getString(
                  PKG, "PipelineGraph.Hop.Tooltip.HopTypeCopy", transform.getName(), Const.CR));
          tipImage = GuiResource.getInstance().getImageCopyHop();
          break;
        case ROW_DISTRIBUTION_ICON:
          transform = (TransformMeta) areaOwner.getParent();
          tip.append(
              BaseMessages.getString(
                  PKG,
                  "PipelineGraph.Hop.Tooltip.RowDistribution",
                  transform.getName(),
                  transform.getRowDistribution() == null
                      ? ""
                      : transform.getRowDistribution().getDescription()));
          tip.append(Const.CR);
          tipImage = GuiResource.getInstance().getImageBalance();
          break;
        case HOP_INFO_ICON:
          TransformMeta from = (TransformMeta) areaOwner.getParent();
          TransformMeta to = (TransformMeta) areaOwner.getOwner();
          tip.append(
              BaseMessages.getString(
                  PKG,
                  "PipelineGraph.Hop.Tooltip.HopTypeInfo",
                  to.getName(),
                  from.getName(),
                  Const.CR));
          tipImage = GuiResource.getInstance().getImageInfo();
          break;
        case HOP_ERROR_ICON:
          from = (TransformMeta) areaOwner.getParent();
          to = (TransformMeta) areaOwner.getOwner();
          areaOwner.getOwner();
          tip.append(
              BaseMessages.getString(
                  PKG,
                  "PipelineGraph.Hop.Tooltip.HopTypeError",
                  from.getName(),
                  to.getName(),
                  Const.CR));
          tipImage = GuiResource.getInstance().getImageError();
          break;
        case HOP_INFO_TRANSFORM_COPIES_ERROR:
          from = (TransformMeta) areaOwner.getParent();
          to = (TransformMeta) areaOwner.getOwner();
          tip.append(
              BaseMessages.getString(
                  PKG,
                  "PipelineGraph.Hop.Tooltip.InfoTransformCopies",
                  from.getName(),
                  to.getName(),
                  Const.CR));
          tipImage = GuiResource.getInstance().getImageError();
          break;
        case HOP_INFO_TRANSFORMS_PARTITIONED:
          from = (TransformMeta) areaOwner.getParent();
          to = (TransformMeta) areaOwner.getOwner();
          tip.append(
              BaseMessages.getString(
                  PKG,
                  "PipelineGraph.Hop.Tooltip.InfoTransformsPartitioned",
                  from.getName(),
                  to.getName(),
                  Const.CR));
          tipImage = GuiResource.getInstance().getImageError();
          break;

        case TRANSFORM_TARGET_HOP_ICON:
          IStream stream = (IStream) areaOwner.getOwner();
          tip.append(stream.getDescription());

          if (stream.getStreamIcon() == StreamIcon.TRUE) {
            tipImage = GuiResource.getInstance().getImageTrue();
          } else if (stream.getStreamIcon() == StreamIcon.FALSE) {
            tipImage = GuiResource.getInstance().getImageFalse();
          } else {
            tipImage = GuiResource.getInstance().getImageTarget();
          }
          break;

        case TRANSFORM_INFO_ICON, TRANSFORM_ICON:
          TransformMeta iconTransformMeta = (TransformMeta) areaOwner.getOwner();

          // If transform is deprecated, display first
          if (iconTransformMeta.isDeprecated()) {
            tip.append(
                    BaseMessages.getString(PKG, "PipelineGraph.DeprecatedTransform.Tooltip.Title"))
                .append(Const.CR);
            String tipNext =
                BaseMessages.getString(
                    PKG,
                    "PipelineGraph.DeprecatedTransform.Tooltip.Message1",
                    iconTransformMeta.getName());
            int length = tipNext.length() + 5;
            for (int i = 0; i < length; i++) {
              tip.append("-");
            }
            tip.append(Const.CR).append(tipNext).append(Const.CR);
            tip.append(
                BaseMessages.getString(PKG, "PipelineGraph.DeprecatedTransform.Tooltip.Message2"));
            if (!Utils.isEmpty(iconTransformMeta.getSuggestion())
                && !(iconTransformMeta.getSuggestion().startsWith("!")
                    && iconTransformMeta.getSuggestion().endsWith("!"))) {
              tip.append(" ");
              tip.append(
                  BaseMessages.getString(
                      PKG,
                      "PipelineGraph.DeprecatedTransform.Tooltip.Message3",
                      iconTransformMeta.getSuggestion()));
            }
            tipImage = GuiResource.getInstance().getImageDeprecated();
          } else if (!Utils.isEmpty(iconTransformMeta.getDescription())) {
            tip.append(iconTransformMeta.getDescription());
          }
          break;
        case TRANSFORM_OUTPUT_DATA:
          RowBuffer rowBuffer = (RowBuffer) areaOwner.getOwner();
          if (rowBuffer != null && !rowBuffer.isEmpty()) {
            tip.append("Available output rows: " + rowBuffer.size());
            tipImage = GuiResource.getInstance().getImageData();
          }
          break;
        default:
          // For plugins...
          //
          try {
            HopGuiTooltipExtension tooltipExt =
                new HopGuiTooltipExtension(x, y, screenX, screenY, areaOwner, tip);
            ExtensionPointHandler.callExtensionPoint(
                hopGui.getLog(),
                variables,
                HopExtensionPoint.HopGuiPipelineGraphAreaHover.name(),
                tooltipExt);
            tipImage = tooltipExt.tooltipImage;
          } catch (Exception ex) {
            hopGui
                .getLog()
                .logError(
                    "Error calling extension point "
                        + HopExtensionPoint.HopGuiPipelineGraphAreaHover.name(),
                    ex);
          }
          break;
      }
    }

    if (hi != null && tip.length() == 0) { // We clicked on a HOP!
      // Set the tooltip for the hop:
      tip.append(Const.CR)
          .append(BaseMessages.getString(PKG, "PipelineGraph.Dialog.HopInfo"))
          .append(newTip = hi.toString())
          .append(Const.CR);
    }

    if (tip.length() == 0) {
      newTip = null;
    } else {
      newTip = tip.toString();
    }

    if (newTip == null) {
      toolTip.setVisible(false);
      if (hi != null) { // We clicked on a HOP!

        // Set the tooltip for the hop:
        newTip =
            BaseMessages.getString(PKG, "PipelineGraph.Dialog.HopInfo")
                + Const.CR
                + BaseMessages.getString(PKG, "PipelineGraph.Dialog.HopInfo.SourceTransform")
                + " "
                + hi.getFromTransform().getName()
                + Const.CR
                + BaseMessages.getString(PKG, "PipelineGraph.Dialog.HopInfo.TargetTransform")
                + " "
                + hi.getToTransform().getName()
                + Const.CR
                + BaseMessages.getString(PKG, "PipelineGraph.Dialog.HopInfo.Status")
                + " "
                + (hi.isEnabled()
                    ? BaseMessages.getString(PKG, "PipelineGraph.Dialog.HopInfo.Enable")
                    : BaseMessages.getString(PKG, "PipelineGraph.Dialog.HopInfo.Disable"));
        toolTip.setText(newTip);
        showToolTip(new org.eclipse.swt.graphics.Point(screenX, screenY));
      }

    } else if (!newTip.equalsIgnoreCase(getToolTipText())) {
      Image tooltipImage = null;
      if (tipImage != null) {
        tooltipImage = tipImage;
      } else {
        tooltipImage = GuiResource.getInstance().getImageHopUi();
      }
      showTooltip(newTip, tooltipImage, screenX, screenY);
    }

    return subject;
  }

  public void showTooltip(String label, Image image, int screenX, int screenY) {
    toolTip.setText(label);
    toolTip.setVisible(false);
    showToolTip(new org.eclipse.swt.graphics.Point(screenX, screenY));
  }

  public synchronized AreaOwner getVisibleAreaOwner(int x, int y) {
    for (int i = areaOwners.size() - 1; i >= 0; i--) {
      AreaOwner areaOwner = areaOwners.get(i);
      if (areaOwner.contains(x, y)) {
        return areaOwner;
      }
    }
    return null;
  }

  public void delSelected(TransformMeta transformMeta) {
    List<TransformMeta> selection = pipelineMeta.getSelectedTransforms();
    if (currentTransform == null
        && transformMeta == null
        && selection.isEmpty()
        && pipelineMeta.getSelectedNotes().isEmpty()) {
      return; // nothing to do
    }
    if (transformMeta != null && selection.isEmpty()) {
      pipelineTransformDelegate.delTransform(pipelineMeta, transformMeta);
      return;
    }

    if (currentTransform != null && selection.contains(currentTransform)) {
      currentTransform = null;
      for (ITransformSelectionListener listener : currentTransformListeners) {
        listener.onUpdateSelection(currentTransform);
      }
    }

    if (!selection.isEmpty()) {
      pipelineTransformDelegate.delTransforms(pipelineMeta, selection);
    }
    if (!pipelineMeta.getSelectedNotes().isEmpty()) {
      notePadDelegate.deleteNotes(pipelineMeta, pipelineMeta.getSelectedNotes());
    }
  }

  public void editDescription(TransformMeta transformMeta) {
    String title = BaseMessages.getString(PKG, "PipelineGraph.Dialog.TransformDescription.Title");
    String message =
        BaseMessages.getString(PKG, "PipelineGraph.Dialog.TransformDescription.Message");
    EnterTextDialog dialog =
        new EnterTextDialog(hopShell(), title, message, transformMeta.getDescription());
    String description = dialog.open();
    if (description != null) {
      transformMeta.setDescription(description);
      transformMeta.setChanged();
      updateGui();
    }
  }

  /**
   * Display the input- or outputfields for a transform.
   *
   * @param transformMeta The transform (it's metadata) to query
   * @param before set to true if you want to have the fields going INTO the transform, false if you
   *     want to see all the fields that exit the transform.
   */
  private void inputOutputFields(TransformMeta transformMeta, boolean before) {
    redraw();

    SearchFieldsProgressDialog op =
        new SearchFieldsProgressDialog(variables, pipelineMeta, transformMeta, before);
    boolean alreadyThrownError = false;
    try {
      final ProgressMonitorDialog pmd = new ProgressMonitorDialog(hopShell());

      // Run something in the background to cancel active database queries. Force this if needed!
      //
      Runnable run =
          () -> {
            IProgressMonitor monitor = pmd.getProgressMonitor();
            while (pmd.getShell() == null
                || (!pmd.getShell().isDisposed() && !monitor.isCanceled())) {
              try {
                Thread.sleep(250);
              } catch (InterruptedException e) {
                // Ignore
              }
            }

            if (monitor.isCanceled()) { // Disconnect and see what happens!

              try {
                pipelineMeta.cancelQueries();
              } catch (Exception e) {
                // Ignore
              }
            }
          };
      // Dump the cancel looker in the background!
      new Thread(run).start();

      pmd.run(true, op);
    } catch (InvocationTargetException | InterruptedException e) {
      new ErrorDialog(
          hopShell(),
          BaseMessages.getString(PKG, "PipelineGraph.Dialog.GettingFields.Title"),
          BaseMessages.getString(PKG, "PipelineGraph.Dialog.GettingFields.Message"),
          e);
      alreadyThrownError = true;
    }

    IRowMeta fields = op.getFields();

    if (fields != null && fields.size() > 0) {
      TransformFieldsDialog sfd =
          new TransformFieldsDialog(
              hopShell(), variables, SWT.NONE, transformMeta.getName(), fields);
      String sn = (String) sfd.open();
      if (sn != null) {
        TransformMeta esi = pipelineMeta.findTransform(sn);
        if (esi != null) {
          editTransform(esi);
        }
      }
    } else {
      if (!alreadyThrownError) {
        modalMessageDialog(
            BaseMessages.getString(PKG, "PipelineGraph.Dialog.CouldntFindFields.Title"),
            BaseMessages.getString(PKG, "PipelineGraph.Dialog.CouldntFindFields.Message"),
            SWT.OK | SWT.ICON_INFORMATION);
      }
    }
  }

  public void paintControl(PaintEvent e) {
    Point area = getArea();
    if (area.x == 0 || area.y == 0) {
      return; // nothing to do!
    }

    // Do double buffering to prevent flickering on Windows
    //
    boolean needsDoubleBuffering =
        Const.isWindows() && "GUI".equalsIgnoreCase(Const.getHopPlatformRuntime());

    Image image = null;
    GC swtGc = e.gc;

    if (needsDoubleBuffering) {
      image = new Image(hopDisplay(), area.x, area.y);
      swtGc = new GC(image);
    }

    drawPipelineImage(swtGc, area.x, area.y);

    if (needsDoubleBuffering) {
      // Draw the image onto the canvas and get rid of the resources
      //
      e.gc.drawImage(image, 0, 0);
      swtGc.dispose();
      image.dispose();
    }
  }

  public void drawPipelineImage(GC swtGc, int width, int height) {

    if (EnvironmentUtils.getInstance().isWeb()) {}

    IGc gc = new SwtGc(swtGc, width, height, iconSize);
    try {
      PropsUi propsUi = PropsUi.getInstance();

      // Can we determine the maximum while drawing?
      //
      maximum = pipelineMeta.getMaximum();
      int gridSize = propsUi.isShowCanvasGridEnabled() ? propsUi.getCanvasGridSize() : 1;

      PipelinePainter pipelinePainter =
          new PipelinePainter(
              gc,
              variables,
              pipelineMeta,
              new Point(width, height),
              offset,
              candidate,
              selectionRegion,
              areaOwners,
              propsUi.getIconSize(),
              propsUi.getLineWidth(),
              gridSize,
              propsUi.getNoteFont().getName(),
              propsUi.getNoteFont().getHeight(),
              pipeline,
              propsUi.isIndicateSlowPipelineTransformsEnabled(),
              propsUi.getZoomFactor(),
              outputRowsMap,
              propsUi.isBorderDrawnAroundCanvasNames(),
              mouseOverName,
              stateMap);

      pipelinePainter.setMagnification((float) (magnification * PropsUi.getNativeZoomFactor()));
      pipelinePainter.setTransformLogMap(transformLogMap);
      pipelinePainter.setStartHopTransform(startHopTransform);
      pipelinePainter.setEndHopLocation(endHopLocation);
      pipelinePainter.setNoInputTransform(noInputTransform);
      pipelinePainter.setEndHopTransform(endHopTransform);
      pipelinePainter.setCandidateHopType(candidateHopType);
      pipelinePainter.setStartErrorHopTransform(startErrorHopTransform);
      pipelinePainter.setMaximum(maximum);
      pipelinePainter.setShowingNavigationView(true);
      pipelinePainter.setScreenMagnification(magnification);
      pipelinePainter.setShowingNavigationView(!PropsUi.getInstance().isHideViewportEnabled());

      try {
        pipelinePainter.drawPipelineImage();

        // Keep the rectangles of the navigation view around
        //
        this.viewPort = pipelinePainter.getViewPort();
        this.graphPort = pipelinePainter.getGraphPort();

        if (pipelineMeta.isEmpty()) {
          SvgFile svgFile =
              new SvgFile(
                  BasePropertyHandler.getProperty("PipelineCanvas_image"),
                  getClass().getClassLoader());
          gc.setTransform(0.0f, 0.0f, (float) (magnification * PropsUi.getNativeZoomFactor()));
          gc.drawImage(svgFile, 150, 150, 32, 40, gc.getMagnification(), 0);
          gc.drawText(
              BaseMessages.getString(PKG, "PipelineGraph.NewPipelineBackgroundMessage"),
              155,
              125,
              true);
        }

      } catch (Exception e) {
        new ErrorDialog(hopGui.getActiveShell(), CONST_ERROR, "Error drawing pipeline image", e);
      }
    } finally {
      gc.dispose();
    }
    CanvasFacade.setData(canvas, magnification, offset, pipelineMeta);
  }

  private void editTransform(TransformMeta transformMeta) {
    pipelineMeta.unselectAll();
    updateGui();
    pipelineTransformDelegate.editTransform(pipelineMeta, transformMeta);
  }

  private void editNote(NotePadMeta ni) {
    NotePadMeta before = ni.clone();

    String title = BaseMessages.getString(PKG, "PipelineGraph.Dialog.EditNote.Title");
    NotePadDialog dd = new NotePadDialog(variables, hopShell(), title, ni);
    NotePadMeta n = dd.open();

    if (n != null) {
      ni.setChanged();
      ni.setNote(n.getNote());
      ni.setFontName(n.getFontName());
      ni.setFontSize(n.getFontSize());
      ni.setFontBold(n.isFontBold());
      ni.setFontItalic(n.isFontItalic());
      // font color
      ni.setFontColorRed(n.getFontColorRed());
      ni.setFontColorGreen(n.getFontColorGreen());
      ni.setFontColorBlue(n.getFontColorBlue());
      // background color
      ni.setBackGroundColorRed(n.getBackGroundColorRed());
      ni.setBackGroundColorGreen(n.getBackGroundColorGreen());
      ni.setBackGroundColorBlue(n.getBackGroundColorBlue());
      // border color
      ni.setBorderColorRed(n.getBorderColorRed());
      ni.setBorderColorGreen(n.getBorderColorGreen());
      ni.setBorderColorBlue(n.getBorderColorBlue());
      ni.width = ConstUi.NOTE_MIN_SIZE;
      ni.height = ConstUi.NOTE_MIN_SIZE;

      NotePadMeta after = (NotePadMeta) ni.clone();
      hopGui.undoDelegate.addUndoChange(
          pipelineMeta,
          new NotePadMeta[] {before},
          new NotePadMeta[] {after},
          new int[] {pipelineMeta.indexOfNote(ni)});
      updateGui();
    }
  }

  private void editHop(PipelineHopMeta pipelineHopMeta) {
    String name = pipelineHopMeta.toString();
    if (log.isDebug()) {
      log.logDebug(BaseMessages.getString(PKG, "PipelineGraph.Logging.EditingHop") + name);
    }
    pipelineHopDelegate.editHop(pipelineMeta, pipelineHopMeta);
  }

  private void newHop() {
    List<TransformMeta> selection = pipelineMeta.getSelectedTransforms();
    if (selection.size() == 2) {
      TransformMeta fr = selection.get(0);
      TransformMeta to = selection.get(1);
      pipelineHopDelegate.newHop(pipelineMeta, fr, to);
    }
  }

  @GuiContextAction(
      id = "pipeline-graph-transform-10050-create-hop",
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Create,
      name = "i18n::HopGuiPipelineGraph.HopAction.CreateHop.Name",
      tooltip = "i18n::HopGuiPipelineGraph.HopAction.CreateHop.Tooltip",
      image = "ui/images/hop.svg",
      category = "Basic",
      categoryOrder = "1")
  public void newHopCandidate(HopGuiPipelineTransformContext context) {
    startHopTransform = context.getTransformMeta();
    endHopTransform = null;
    redraw();
  }

  private boolean pointOnLine(int x, int y, int[] line) {
    int dx;
    int dy;
    int pm = HOP_SEL_MARGIN / 2;
    boolean retval = false;

    for (dx = -pm; dx <= pm && !retval; dx++) {
      for (dy = -pm; dy <= pm && !retval; dy++) {
        retval = pointOnThinLine(x + dx, y + dy, line);
      }
    }

    return retval;
  }

  private boolean pointOnThinLine(int x, int y, int[] line) {
    int x1 = line[0];
    int y1 = line[1];
    int x2 = line[2];
    int y2 = line[3];

    // Not in the square formed by these 2 points: ignore!
    if (!(((x >= x1 && x <= x2) || (x >= x2 && x <= x1))
        && ((y >= y1 && y <= y2) || (y >= y2 && y <= y1)))) {
      return false;
    }

    double angleLine = Math.atan2(y2 - y1, x2 - x1) + Math.PI;
    double anglePoint = Math.atan2(y - y1, x - x1) + Math.PI;

    // Same angle, or close enough?
    if (anglePoint >= angleLine - 0.01 && anglePoint <= angleLine + 0.01) {
      return true;
    }

    return false;
  }

  public SnapAllignDistribute createSnapAlignDistribute() {
    List<TransformMeta> selection = pipelineMeta.getSelectedTransforms();
    int[] indices = pipelineMeta.getTransformIndexes(selection);

    return new SnapAllignDistribute(pipelineMeta, selection, indices, hopGui.undoDelegate, this);
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_PREVIEW,
      // label = "Preview",
      toolTip = "i18n::PipelineGraph.Toolbar.Preview.Tooltip",
      image = "ui/images/preview.svg")
  @Override
  public void preview() {
    try {
      pipelineRunDelegate.executePipeline(
          hopGui.getLog(),
          pipelineMeta,
          true,
          false,
          pipelineRunDelegate.getPipelinePreviewExecutionConfiguration().getLogLevel());
    } catch (Exception e) {
      new ErrorDialog(hopShell(), CONST_ERROR, CONST_ERROR_PREVIEWING_PIPELINE, e);
    }
  }

  @GuiContextAction(
      id = "pipeline-graph-transform-10100-preview-output",
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Info,
      name = "i18n::HopGuiPipelineGraph.PipelineAction.Preview.Name",
      tooltip = "i18n::HopGuiPipelineGraph.PipelineAction.Preview.Tooltip",
      image = "ui/images/preview.svg",
      category = "Preview",
      categoryOrder = "3")
  /** Preview a single transform */
  public void preview(HopGuiPipelineTransformContext context) {
    try {
      context.getPipelineMeta().unselectAll();
      context.getTransformMeta().setSelected(true);
      pipelineRunDelegate.executePipeline(
          hopGui.getLog(),
          pipelineMeta,
          true,
          false,
          pipelineRunDelegate.getPipelinePreviewExecutionConfiguration().getLogLevel());
    } catch (Exception e) {
      new ErrorDialog(hopShell(), CONST_ERROR, CONST_ERROR_PREVIEWING_PIPELINE, e);
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_DEBUG,
      // label = "Debug",
      toolTip = "i18n::PipelineGraph.Toolbar.Debug.Tooltip",
      image = "ui/images/debug.svg")
  @Override
  public void debug() {
    try {
      pipelineRunDelegate.executePipeline(
          hopGui.getLog(),
          pipelineMeta,
          false,
          true,
          pipelineRunDelegate.getPipelineDebugExecutionConfiguration().getLogLevel());
    } catch (Exception e) {
      new ErrorDialog(hopShell(), CONST_ERROR, "Error debugging pipeline", e);
    }
  }

  @GuiContextAction(
      id = "pipeline-graph-transform-10150-debug-output",
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Info,
      name = "i18n::HopGuiPipelineGraph.PipelineAction.DebugOutput.Name",
      tooltip = "i18n::HopGuiPipelineGraph.PipelineAction.DebugOutput.Tooltip",
      image = "ui/images/debug.svg",
      category = "Preview",
      categoryOrder = "3")
  /** Debug a single transform */
  public void debug(HopGuiPipelineTransformContext context) {
    try {
      context.getPipelineMeta().unselectAll();
      context.getTransformMeta().setSelected(true);
      pipelineRunDelegate.executePipeline(
          hopGui.getLog(),
          pipelineMeta,
          false,
          debug,
          pipelineRunDelegate.getPipelinePreviewExecutionConfiguration().getLogLevel());
    } catch (Exception e) {
      new ErrorDialog(hopShell(), CONST_ERROR, CONST_ERROR_PREVIEWING_PIPELINE, e);
    }
  }

  public void newProps() {
    iconSize = hopGui.getProps().getIconSize();
  }

  public IEngineMeta getMeta() {
    return pipelineMeta;
  }

  /**
   * @param pipelineMeta the pipelineMeta to set
   */
  public void setPipelineMeta(PipelineMeta pipelineMeta) {
    this.pipelineMeta = pipelineMeta;
    if (pipelineMeta != null) {
      pipelineMeta.setInternalHopVariables(variables);
    }
  }

  @Override
  public String getName() {
    return pipelineMeta.getName();
  }

  @Override
  public void setName(String name) {
    pipelineMeta.setName(name);
  }

  @Override
  public void setFilename(String filename) {
    pipelineMeta.setFilename(filename);
  }

  @Override
  public String getFilename() {
    return pipelineMeta.getFilename();
  }

  public boolean canBeClosed() {
    return !pipelineMeta.hasChanged();
  }

  public PipelineMeta getManagedObject() {
    return pipelineMeta;
  }

  /**
   * @deprecated Use method hasChanged()
   */
  @Deprecated(since = "2.10")
  public boolean hasContentChanged() {
    return pipelineMeta.hasChanged();
  }

  public boolean editProperties(
      PipelineMeta pipelineMeta, HopGui hopGui, boolean allowDirectoryChange) {
    return editProperties(pipelineMeta, hopGui, null);
  }

  public boolean editProperties(
      PipelineMeta pipelineMeta, HopGui hopGui, PipelineDialog.Tabs currentTab) {
    if (pipelineMeta == null) {
      return false;
    }

    Shell shell = hopGui.getActiveShell();
    if (shell == null) {
      shell = hopGui.getShell();
    }
    PipelineDialog tid = new PipelineDialog(shell, SWT.NONE, variables, pipelineMeta, currentTab);
    if (tid.open() != null) {
      hopGui.setParametersAsVariablesInUI(pipelineMeta, variables);
      updateGui();
      perspective.updateTabs();
      return true;
    }
    return false;
  }

  @Override
  public boolean hasChanged() {
    return pipelineMeta.hasChanged();
  }

  @Override
  public void setChanged() {
    pipelineMeta.setChanged();
  }

  @Override
  public synchronized void save() throws HopException {
    try {
      ExtensionPointHandler.callExtensionPoint(
          log, variables, HopExtensionPoint.PipelineBeforeSave.id, pipelineMeta);

      if (StringUtils.isEmpty(pipelineMeta.getFilename())) {
        throw new HopException("No filename: please specify a filename for this pipeline");
      }

      // Keep track of save
      //
      AuditManager.registerEvent(
          HopNamespace.getNamespace(), "file", pipelineMeta.getFilename(), "save");

      String xml = pipelineMeta.getXml(variables);
      OutputStream out = HopVfs.getOutputStream(pipelineMeta.getFilename(), false);
      try {
        out.write(XmlHandler.getXmlHeader(Const.XML_ENCODING).getBytes(StandardCharsets.UTF_8));
        out.write(xml.getBytes(StandardCharsets.UTF_8));
        pipelineMeta.clearChanged();
        updateGui();
        HopGui.getDataOrchestrationPerspective().updateTabs();
      } finally {
        out.flush();
        out.close();

        ExtensionPointHandler.callExtensionPoint(
            log, variables, HopExtensionPoint.PipelineAfterSave.id, pipelineMeta);
      }
    } catch (Exception e) {
      throw new HopException(
          "Error saving pipeline to file '" + pipelineMeta.getFilename() + "'", e);
    }
  }

  @Override
  public void saveAs(String filename) throws HopException {

    try {

      // Enforce file extension
      if (!filename.toLowerCase().endsWith(this.getFileType().getDefaultFileExtension())) {
        filename = filename + this.getFileType().getDefaultFileExtension();
      }

      FileObject fileObject = HopVfs.getFileObject(filename);
      if (fileObject.exists()) {
        MessageBox box = new MessageBox(hopGui.getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
        box.setText("Overwrite?");
        box.setMessage("Are you sure you want to overwrite file '" + filename + "'?");
        int answer = box.open();
        if ((answer & SWT.YES) == 0) {
          return;
        }
      }

      pipelineMeta.setFilename(filename);
      save();
      hopGui.fileRefreshDelegate.register(fileObject.getPublicURIString(), this);
    } catch (Exception e) {
      throw new HopException("Error validating file existence for '" + filename + "'", e);
    }
  }

  @Override
  public void close() {
    perspective.remove(this);
  }

  @Override
  public boolean isCloseable() {
    try {
      // Check if the file is saved. If not, ask for it to be stopped before closing
      //
      if (pipeline != null && (pipeline.isRunning() || pipeline.isPaused())) {
        MessageBox messageDialog =
            new MessageBox(hopShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO | SWT.CANCEL);
        messageDialog.setText(
            BaseMessages.getString(PKG, "PipelineGraph.RunningFile.Dialog.Header"));
        messageDialog.setMessage(
            BaseMessages.getString(
                PKG, "PipelineGraph.RunningFile.Dialog.Message", buildTabName()));
        int answer = messageDialog.open();
        // The NO answer means: ignore the state of the pipeline and just let it run in the
        // background
        // It can be seen in the execution information perspective if a location was set up.
        //
        if ((answer & SWT.YES) != 0) {
          // Stop the execution and close if the file hasn't been changed
          pipeline.stopAll();
        } else if ((answer & SWT.CANCEL) != 0) {
          return false;
        }
      }
      if (pipelineMeta.hasChanged()) {

        MessageBox messageDialog =
            new MessageBox(hopShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO | SWT.CANCEL);
        messageDialog.setText(BaseMessages.getString(PKG, "PipelineGraph.SaveFile.Dialog.Header"));
        messageDialog.setMessage(
            BaseMessages.getString(PKG, "PipelineGraph.SaveFile.Dialog.Message", buildTabName()));
        int answer = messageDialog.open();
        if ((answer & SWT.YES) != 0) {
          if (StringUtils.isEmpty(this.getFilename())) {
            // Ask for the filename
            //
            String filename =
                BaseDialog.presentFileDialog(
                    true,
                    hopGui.getActiveShell(),
                    fileType.getFilterExtensions(),
                    fileType.getFilterNames(),
                    true);
            if (filename == null) {
              return false;
            }

            filename = hopGui.getVariables().resolve(filename);
            saveAs(filename);
          } else {
            save();
          }
          return true;
        }
        if ((answer & SWT.NO) != 0) {
          // User doesn't want to save but close
          return true;
        }
        return false;
      } else {
        return true;
      }
    } catch (Exception e) {
      new ErrorDialog(hopShell(), CONST_ERROR, "Error preparing file close", e);
    }
    return false;
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_START,
      toolTip = "i18n::PipelineGraph.Toolbar.Start.Tooltip",
      image = "ui/images/run.svg")
  @Override
  public void start() {
    try {
      pipelineMeta.setShowDialog(pipelineMeta.isAlwaysShowRunOptions());
      ServerPushSessionFacade.start();
      Thread thread =
          new Thread(
              () ->
                  getDisplay()
                      .asyncExec(
                          () -> {
                            try {
                              if (isRunning() && pipeline.isPaused()) {
                                pauseResume();
                              } else {
                                pipelineRunDelegate.executePipeline(
                                    hopGui.getLog(), pipelineMeta, false, false, LogLevel.BASIC);
                                ServerPushSessionFacade.stop();
                              }
                            } catch (Throwable e) {
                              new ErrorDialog(
                                  getShell(),
                                  "Execute pipeline",
                                  "There was an error during pipeline execution",
                                  e);
                            }
                          }));
      thread.start();
    } catch (Throwable e) {
      log.logError("Severe error in pipeline execution detected", e);
    }
  }

  @Override
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_PAUSE,
      // label = "Pause",
      toolTip = "i18n::PipelineGraph.Toolbar.Pause.Tooltip",
      image = "ui/images/pause.svg")
  public void pause() {
    pauseResume();
  }

  @Override
  public void resume() {
    pauseResume();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_CHECK,
      toolTip = "i18n:org.apache.hop.ui.hopgui:HopGui.Tooltip.VerifyPipeline",
      image = "ui/images/check.svg",
      separator = true)
  @GuiKeyboardShortcut(key = SWT.F7)
  public void checkPipeline() {

    // Show the results views
    //
    addAllTabs();

    this.pipelineCheckDelegate.checkPipeline();
  }

  /** TODO: re-introduce public void analyseImpact() { hopGui.analyseImpact(); } */

  /** TODO: re-introduce public void getSql() { hopGui.getSql(); } */

  /* TODO: re-introduce
  public void exploreDatabase() {
    hopGui.exploreDatabase();
  }
   */
  public boolean isExecutionResultsPaneVisible() {
    return extraViewTabFolder != null && !extraViewTabFolder.isDisposed();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_SHOW_EXECUTION_RESULTS,
      // label = "HopGui.Menu.ShowExecutionResults",
      toolTip = "i18n:org.apache.hop.ui.hopgui:HopGui.Tooltip.ShowExecutionResults",
      image = "ui/images/show-results.svg",
      separator = true)
  public void showExecutionResults() {
    ToolItem item = toolBarWidgets.findToolItem(TOOLBAR_ITEM_SHOW_EXECUTION_RESULTS);
    if (isExecutionResultsPaneVisible()) {
      disposeExtraView();
    } else {
      addAllTabs();
    }
  }

  /** If the extra tab view at the bottom is empty, we close it. */
  public void checkEmptyExtraView() {
    if (extraViewTabFolder.getItemCount() == 0) {
      disposeExtraView();
    }
  }

  private void disposeExtraView() {
    if (extraViewTabFolder == null) {
      return;
    }

    extraViewTabFolder.dispose();
    sashForm.layout();
    sashForm.setWeights(100);

    ToolItem item = toolBarWidgets.findToolItem(TOOLBAR_ITEM_SHOW_EXECUTION_RESULTS);
    item.setToolTipText(BaseMessages.getString(PKG, "HopGui.Tooltip.ShowExecutionResults"));
    item.setImage(GuiResource.getInstance().getImageShowResults());
  }

  private void minMaxExtraView() {
    // What is the state?
    //
    boolean maximized = sashForm.getMaximizedControl() != null;
    if (maximized) {
      // Minimize
      //
      sashForm.setMaximizedControl(null);
      minMaxItem.setImage(GuiResource.getInstance().getImageMaximizePanel());
      minMaxItem.setToolTipText(
          BaseMessages.getString(PKG, "PipelineGraph.ExecutionResultsPanel.MaxButton.Tooltip"));
    } else {
      // Maximize
      //
      sashForm.setMaximizedControl(extraViewTabFolder);
      minMaxItem.setImage(GuiResource.getInstance().getImageMinimizePanel());
      minMaxItem.setToolTipText(
          BaseMessages.getString(PKG, "PipelineGraph.ExecutionResultsPanel.MinButton.Tooltip"));
    }
  }

  private void rotateExtraView() {
    // Toggle orientation
    boolean orientation = !PropsUi.getInstance().isGraphExtraViewVerticalOrientation();
    PropsUi.getInstance().setGraphExtraViewVerticalOrientation(orientation);

    if (orientation) {
      sashForm.setOrientation(SWT.VERTICAL);
      rotateItem.setImage(GuiResource.getInstance().getImageRotateRight());
    } else {
      sashForm.setOrientation(SWT.HORIZONTAL);
      rotateItem.setImage(GuiResource.getInstance().getImageRotateLeft());
    }
  }

  private ToolItem minMaxItem;
  private ToolItem rotateItem;

  /** Add an extra view to the main composite SashForm */
  public void addExtraView() {

    // Add a tab folder ...
    //
    extraViewTabFolder = new CTabFolder(sashForm, SWT.MULTI);
    PropsUi.setLook(extraViewTabFolder, Props.WIDGET_STYLE_TAB);

    extraViewTabFolder.addMouseListener(
        new MouseAdapter() {

          @Override
          public void mouseDoubleClick(MouseEvent arg0) {
            if (sashForm.getMaximizedControl() == null) {
              sashForm.setMaximizedControl(extraViewTabFolder);
            } else {
              sashForm.setMaximizedControl(null);
            }
          }
        });

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.top = new FormAttachment(0, 0);
    fdTabFolder.bottom = new FormAttachment(100, 0);
    extraViewTabFolder.setLayoutData(fdTabFolder);

    // Create toolbar for close and min/max to the upper right corner...
    //
    ToolBar extraViewToolBar = new ToolBar(extraViewTabFolder, SWT.FLAT);
    extraViewTabFolder.setTopRight(extraViewToolBar, SWT.RIGHT);
    PropsUi.setLook(extraViewToolBar);

    minMaxItem = new ToolItem(extraViewToolBar, SWT.PUSH);
    minMaxItem.setImage(GuiResource.getInstance().getImageMaximizePanel());
    minMaxItem.setToolTipText(
        BaseMessages.getString(PKG, "PipelineGraph.ExecutionResultsPanel.MaxButton.Tooltip"));
    minMaxItem.addListener(SWT.Selection, e -> minMaxExtraView());

    rotateItem = new ToolItem(extraViewToolBar, SWT.PUSH);
    rotateItem.setImage(
        PropsUi.getInstance().isGraphExtraViewVerticalOrientation()
            ? GuiResource.getInstance().getImageRotateRight()
            : GuiResource.getInstance().getImageRotateLeft());
    rotateItem.setToolTipText(
        BaseMessages.getString(PKG, "PipelineGraph.ExecutionResultsPanel.RotateButton.Tooltip"));
    rotateItem.addListener(SWT.Selection, e -> rotateExtraView());

    ToolItem closeItem = new ToolItem(extraViewToolBar, SWT.PUSH);
    closeItem.setImage(GuiResource.getInstance().getImageClosePanel());
    closeItem.setToolTipText(
        BaseMessages.getString(PKG, "PipelineGraph.ExecutionResultsPanel.CloseButton.Tooltip"));
    closeItem.addListener(SWT.Selection, e -> disposeExtraView());

    int height = extraViewToolBar.computeSize(SWT.DEFAULT, SWT.DEFAULT).y;
    extraViewTabFolder.setTabHeight(Math.max(height, extraViewTabFolder.getTabHeight()));

    sashForm.setWeights(new int[] {60, 40});
  }

  public synchronized void start(PipelineExecutionConfiguration executionConfiguration)
      throws HopException {

    // If filename set & not changed ?
    //
    if (handlePipelineMetaChanges(pipelineMeta)) {

      // If the pipeline is not running, start the pipeline...
      //
      if (!isRunning()) {
        try {
          // Set the requested logging level..
          //
          DefaultLogLevel.setLogLevel(executionConfiguration.getLogLevel());

          // Do we need to clear the log before running?
          //
          if (executionConfiguration.isClearingLog()) {
            pipelineLogDelegate.clearLog();
          }

          // Also make sure to clear the log entries in the central log store & registry
          //
          if (pipeline != null) {
            HopLogStore.discardLines(pipeline.getLogChannelId(), true);
          }

          // Important: even though pipelineMeta is passed to the Pipeline constructor, it is not
          // the same object as is in
          // memory. To be able to completely test this, we need to run it as we would normally do
          // in hop-run
          //
          String pipelineRunConfigurationName = executionConfiguration.getRunConfiguration();
          pipeline =
              PipelineEngineFactory.createPipelineEngine(
                  variables,
                  variables.resolve(pipelineRunConfigurationName),
                  hopGui.getMetadataProvider(),
                  pipelineMeta);

          // Set the variables from the execution configuration
          // These are values set by the user in the execution dialog
          //
          Map<String, String> variablesMap = executionConfiguration.getVariablesMap();
          Set<String> variableKeys = variablesMap.keySet();
          for (String key : variableKeys) {
            String value = variablesMap.get(key);
            if (StringUtils.isNotEmpty(value)) {
              pipeline.setVariable(key, value);
            }
          }

          // Set the named parameters
          //
          Map<String, String> parametersMap = executionConfiguration.getParametersMap();
          Set<String> parametersKeys = parametersMap.keySet();
          for (String key : parametersKeys) {
            pipeline.setParameterValue(key, Const.NVL(parametersMap.get(key), ""));
          }
          pipeline.activateParameters(pipeline);

          String guiLogObjectId = UUID.randomUUID().toString();
          SimpleLoggingObject guiLoggingObject =
              new SimpleLoggingObject("HOP GUI", LoggingObjectType.HOP_GUI, null);
          guiLoggingObject.setContainerObjectId(guiLogObjectId);
          guiLoggingObject.setLogLevel(executionConfiguration.getLogLevel());
          pipeline.setParent(guiLoggingObject);

          pipeline.setLogLevel(executionConfiguration.getLogLevel());
          log.logBasic(BaseMessages.getString(PKG, "PipelineLog.Log.PipelineOpened"));

          try {
            ExtensionPointHandler.callExtensionPoint(
                log, variables, HopExtensionPoint.HopGuiPipelineBeforeStart.id, pipeline);
          } catch (HopException e) {
            log.logError(e.getMessage(), pipelineMeta.getFilename());
          }

        } catch (HopException e) {
          pipeline = null;
          new ErrorDialog(
              hopShell(),
              BaseMessages.getString(PKG, "PipelineLog.Dialog.ErrorOpeningPipeline.Title"),
              BaseMessages.getString(PKG, "PipelineLog.Dialog.ErrorOpeningPipeline.Message"),
              e);
        }
        if (pipeline != null) {
          log.logBasic(
              BaseMessages.getString(PKG, "PipelineLog.Log.LaunchingPipeline")
                  + pipeline.getPipelineMeta().getName()
                  + "]...");

          // Launch the transform preparation in a different thread.
          // That way HopGui doesn't block anymore and that way we can follow the progress of the
          // initialization
          //
          final Thread parentThread = Thread.currentThread();

          getDisplay()
              .asyncExec(
                  () -> {
                    addAllTabs();
                    preparePipeline(parentThread);
                  });

          log.logBasic(BaseMessages.getString(PKG, "PipelineLog.Log.StartedExecutionOfPipeline"));

          updateGui();

          // Update the GUI at the end of the pipeline
          //
          pipeline.addExecutionFinishedListener(e -> pipelineFinished());
        }
      } else {
        modalMessageDialog(
            BaseMessages.getString(PKG, "PipelineLog.Dialog.DoNoStartPipelineTwice.Title"),
            BaseMessages.getString(PKG, "PipelineLog.Dialog.DoNoStartPipelineTwice.Message"),
            SWT.OK | SWT.ICON_WARNING);
      }
    } else {
      showSaveFileMessage();
    }
  }

  private void pipelineFinished() {
    try {
      HopGuiPipelineFinishedExtension ext = new HopGuiPipelineFinishedExtension(this, pipeline);
      ExtensionPointHandler.callExtensionPoint(
          log, variables, HopGuiExtensionPoint.HopGuiPipelineFinished.id, ext);
    } catch (HopException e) {
      new ErrorDialog(
          getShell(),
          CONST_ERROR,
          "Hop GUI encountered an error with an extension point at the end of a pipeline",
          e);
    }

    updateGui();
  }

  private void addRowsSamplerToPipeline(IPipelineEngine<PipelineMeta> pipeline) {

    if (!(pipeline.getPipelineRunConfiguration().getEngineRunConfiguration()
        instanceof LocalPipelineRunConfiguration)) {
      return;
    }
    LocalPipelineRunConfiguration lprConfig =
        (LocalPipelineRunConfiguration)
            pipeline.getPipelineRunConfiguration().getEngineRunConfiguration();

    if (StringUtils.isEmpty(lprConfig.getSampleTypeInGui())) {
      return;
    }

    try {
      SampleType sampleType = SampleType.valueOf(lprConfig.getSampleTypeInGui());
      if (sampleType == SampleType.None) {
        return;
      }

      final int sampleSize = Const.toInt(pipeline.resolve(lprConfig.getSampleSize()), 100);
      if (sampleSize <= 0) {
        return;
      }

      outputRowsMap = new HashMap<>();
      final Random random = new Random();

      for (final String transformName : pipelineMeta.getTransformNames()) {
        IEngineComponent component = pipeline.findComponent(transformName, 0);
        if (component != null) {
          component.addRowListener(
              new RowAdapter() {
                int nrRows = 0;

                @Override
                public void rowWrittenEvent(IRowMeta rowMeta, Object[] row)
                    throws HopTransformException {
                  RowBuffer rowBuffer = outputRowsMap.get(transformName);
                  if (rowBuffer == null) {
                    rowBuffer = new RowBuffer(rowMeta);
                    outputRowsMap.put(transformName, rowBuffer);

                    // Linked list for faster adding and removing at the front and end of the list
                    //
                    if (sampleType == SampleType.Last) {
                      rowBuffer.setBuffer(Collections.synchronizedList(new LinkedList<>()));
                    } else {
                      rowBuffer.setBuffer(Collections.synchronizedList(new ArrayList<>()));
                    }
                  }

                  // Clone the row to make sure we capture the correct values
                  //
                  if (sampleType != SampleType.None) {
                    try {
                      row = rowMeta.cloneRow(row);
                    } catch (HopValueException e) {
                      throw new HopTransformException("Error copying row for preview purposes", e);
                    }
                  }

                  switch (sampleType) {
                    case First:
                      {
                        if (rowBuffer.size() < sampleSize) {
                          rowBuffer.addRow(row);
                        }
                      }
                      break;
                    case Last:
                      {
                        rowBuffer.addRow(0, row);
                        if (rowBuffer.size() > sampleSize) {
                          rowBuffer.removeRow(rowBuffer.size() - 1);
                        }
                      }
                      break;
                    case Random:
                      {
                        // Reservoir sampling
                        //
                        nrRows++;
                        if (rowBuffer.size() < sampleSize) {
                          rowBuffer.addRow(row);
                        } else {
                          int randomIndex = random.nextInt(nrRows);
                          if (randomIndex < sampleSize) {
                            rowBuffer.setRow(randomIndex, row);
                          }
                        }
                      }
                      break;
                  }
                }
              });
        }
      }

    } catch (Exception e) {
      // Ignore : simply not recognized or empty
    }
  }

  public void showSaveFileMessage() {
    modalMessageDialog(
        BaseMessages.getString(PKG, "PipelineLog.Dialog.SavePipelineBeforeRunning.Title"),
        BaseMessages.getString(PKG, "PipelineLog.Dialog.SavePipelineBeforeRunning.Message"),
        SWT.OK | SWT.ICON_WARNING);
  }

  public void addAllTabs() {

    pipelineLogDelegate.addPipelineLog();
    pipelineGridDelegate.addPipelineGrid();
    pipelineCheckDelegate.addPipelineCheck();
    if (extraViewTabFolder.getSelectionIndex() == -1) {
      extraViewTabFolder.setSelection(0);
    }

    if (!EnvironmentUtils.getInstance().isWeb()) {
      ToolItem item = toolBarWidgets.findToolItem(TOOLBAR_ITEM_SHOW_EXECUTION_RESULTS);
      item.setImage(GuiResource.getInstance().getImageHideResults());
      item.setToolTipText(BaseMessages.getString(PKG, "HopGui.Tooltip.HideExecutionResults"));
    }
  }

  public synchronized void debug(
      PipelineExecutionConfiguration executionConfiguration,
      final PipelineDebugMeta pipelineDebugMeta) {
    if (!isRunning()) {
      try {
        this.lastPipelineDebugMeta = pipelineDebugMeta;

        log.setLogLevel(executionConfiguration.getLogLevel());
        if (log.isDetailed()) {
          log.logDetailed(BaseMessages.getString(PKG, "PipelineLog.Log.DoPreview"));
        }

        // Do we need to clear the log before running?
        //
        if (executionConfiguration.isClearingLog()) {
          pipelineLogDelegate.clearLog();
        }

        // Do we have a previous execution to clean up in the logging registry?
        //
        if (pipeline != null) {
          HopLogStore.discardLines(pipeline.getLogChannelId(), false);
          LoggingRegistry.getInstance().removeIncludingChildren(pipeline.getLogChannelId());
        }

        // Create a new pipeline to execution
        //
        pipeline = new LocalPipelineEngine(pipelineMeta, variables, hopGui.getLoggingObject());
        pipeline.setPreview(true);
        pipeline.setVariable(IPipelineEngine.PIPELINE_IN_PREVIEW_MODE, "Y");
        pipeline.setMetadataProvider(hopGui.getMetadataProvider());

        // Set the variables from the execution configuration
        // These are values set by the user in the execution dialog
        //
        Map<String, String> variablesMap = executionConfiguration.getVariablesMap();
        Set<String> variableKeys = variablesMap.keySet();
        for (String key : variableKeys) {
          String value = variablesMap.get(key);
          if (StringUtils.isNotEmpty(value)) {
            pipeline.setVariable(key, value);
          }
        }

        // Copy over the parameter definitions
        //
        pipeline.copyParametersFromDefinitions(pipelineMeta);

        // Set the named parameters
        //
        Map<String, String> parametersMap = executionConfiguration.getParametersMap();
        Set<String> parametersKeys = parametersMap.keySet();
        for (String key : parametersKeys) {
          String value = Const.NVL(parametersMap.get(key), "");
          pipeline.setParameterValue(key, value);
          pipeline.setVariable(key, value);
        }

        try {
          ExtensionPointHandler.callExtensionPoint(
              log, variables, HopExtensionPoint.HopGuiPipelineBeforeStart.id, pipeline);
        } catch (HopException e) {
          log.logError(e.getMessage(), pipelineMeta.getFilename());
        }

        pipeline.prepareExecution();

        // Add the row listeners to the allocated threads
        //
        pipelineDebugMeta.addRowListenersToPipeline(pipeline);

        // What method should we call back when a break-point is hit?

        pipelineDebugMeta.addBreakPointListers(this::showPreview);

        // Start the threads for the transforms...
        //
        startThreads();

        debug = true;

        // Show the execution results view...
        //
        hopDisplay().asyncExec(this::addAllTabs);
      } catch (Exception e) {
        new ErrorDialog(
            hopShell(),
            BaseMessages.getString(PKG, "PipelineLog.Dialog.UnexpectedErrorDuringPreview.Title"),
            BaseMessages.getString(PKG, "PipelineLog.Dialog.UnexpectedErrorDuringPreview.Message"),
            e);
      }
    } else {
      modalMessageDialog(
          BaseMessages.getString(PKG, "PipelineLog.Dialog.DoNoPreviewWhileRunning.Title"),
          BaseMessages.getString(PKG, "PipelineLog.Dialog.DoNoPreviewWhileRunning.Message"),
          SWT.OK | SWT.ICON_WARNING);
    }
    checkErrorVisuals();
  }

  public synchronized void showPreview(
      final PipelineDebugMeta pipelineDebugMeta,
      final TransformDebugMeta transformDebugMeta,
      final IRowMeta rowBufferMeta,
      final List<Object[]> rowBuffer) {
    hopDisplay()
        .asyncExec(
            () -> {
              if (isDisposed()) {
                return;
              }

              updateGui();
              checkErrorVisuals();

              PreviewRowsDialog previewRowsDialog =
                  new PreviewRowsDialog(
                      hopShell(),
                      variables,
                      SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.APPLICATION_MODAL | SWT.SHEET,
                      transformDebugMeta.getTransformMeta().getName(),
                      rowBufferMeta,
                      rowBuffer);
              previewRowsDialog.setProposingToGetMoreRows(true);
              previewRowsDialog.setProposingToStop(true);
              previewRowsDialog.open();

              if (previewRowsDialog.isAskingForMoreRows()) {
                // clear the row buffer.
                // That way if you click resume, you get the next N rows for the transform :-)
                //
                rowBuffer.clear();

                // Resume running: find more rows...
                //
                pauseResume();
              }

              if (previewRowsDialog.isAskingToStop()) {
                // Stop running
                //
                stop();
              }
            });
  }

  private String[] convertArguments(Map<String, String> arguments) {
    String[] argumentNames = arguments.keySet().toArray(new String[arguments.size()]);
    Arrays.sort(argumentNames);

    String[] args = new String[argumentNames.length];
    for (int i = 0; i < args.length; i++) {
      String argumentName = argumentNames[i];
      args[i] = arguments.get(argumentName);
    }
    return args;
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_STOP,
      // label = "Stop",
      toolTip = "i18n::PipelineGraph.Toolbar.Stop.Tooltip",
      image = "ui/images/stop.svg")
  @Override
  public void stop() {
    if (safeStopping) {
      modalMessageDialog(
          BaseMessages.getString(PKG, "PipelineLog.Log.SafeStopAlreadyStarted.Title"),
          BaseMessages.getString(PKG, "PipelineLog.Log.SafeStopAlreadyStarted"),
          SWT.ICON_ERROR | SWT.OK);
      return;
    }
    if ((isRunning() && !halting)) {
      halting = true;
      pipeline.stopAll();
      log.logBasic(BaseMessages.getString(PKG, "PipelineLog.Log.ProcessingOfPipelineStopped"));

      halted = false;
      halting = false;
    }
    updateGui();
  }

  public synchronized void pauseResume() {
    if (isRunning()) {
      // Get the pause toolbar item
      //
      if (!pipeline.isPaused()) {
        pipeline.pauseExecution();
        updateGui();
      } else {
        pipeline.resumeExecution();
        updateGui();
      }
    }
  }

  private synchronized void preparePipeline(final Thread parentThread) {
    Runnable runnable =
        () -> {
          try {
            pipeline.prepareExecution();

            // Refresh tool bar buttons and so on
            //
            updateGui();

            // When running locally in the GUI, sample rows in every transform to show in the user
            // interface...
            //
            addRowsSamplerToPipeline(pipeline);

            initialized = true;
          } catch (HopException e) {
            log.logError(
                pipeline.getPipelineMeta().getName() + ": preparing pipeline execution failed", e);
            checkErrorVisuals();
          }

          halted = pipeline.hasHaltedComponents();
          if (pipeline.isReadyToStart()) {
            checkStartThreads(); // After init, launch the threads.
          } else {
            initialized = false;
            checkErrorVisuals();
          }
        };
    Thread thread = new Thread(runnable);
    thread.start();
  }

  private void checkStartThreads() {
    if (initialized && !isRunning() && pipeline != null) {
      startThreads();
    }
  }

  private synchronized void startThreads() {
    try {
      // Add a listener to the pipeline.
      // If the pipeline is done, we want to do the end processing, etc.
      //
      pipeline.addExecutionFinishedListener(
          pipeline -> {
            checkPipelineEnded();
            checkErrorVisuals();
            stopRedrawTimer();
          });

      hopGui
          .getDisplay()
          .asyncExec(
              () ->
                  new Thread(
                          () -> {
                            try {
                              pipeline.startThreads();
                              pipeline.waitUntilFinished();
                            } catch (Exception e) {
                              log.logError("Error starting transform threads", e);
                              checkErrorVisuals();
                              stopRedrawTimer();
                            }
                          })
                      .start());

      startRedrawTimer();

      updateGui();
    } catch (Exception e) {
      log.logError("Error starting transform threads", e);
      checkErrorVisuals();
      stopRedrawTimer();
    }
  }

  private void startRedrawTimer() {

    redrawTimer = new Timer("HopGuiPipelineGraph: redraw timer");
    TimerTask timtask =
        new TimerTask() {
          @Override
          public void run() {
            if (!hopDisplay().isDisposed()) {
              hopDisplay()
                  .asyncExec(
                      () -> {
                        if (!HopGuiPipelineGraph.this.canvas.isDisposed()) {
                          if (perspective.isActive() && HopGuiPipelineGraph.this.isVisible()) {
                            HopGuiPipelineGraph.this.canvas.redraw();
                            HopGuiPipelineGraph.this.updateGui();
                          }
                        }
                      });
            }
          }
        };

    redrawTimer.schedule(timtask, 0L, ConstUi.INTERVAL_MS_PIPELINE_CANVAS_REFRESH);
  }

  protected void stopRedrawTimer() {
    ExecutorUtil.cleanup(redrawTimer);
    pipelineGridDelegate.stopRefreshMetricsTimer();
    redrawTimer = null;
  }

  private void checkPipelineEnded() {
    if (pipeline != null) {
      if (pipeline.isFinished() && (isRunning() || halted)) {
        log.logBasic(BaseMessages.getString(PKG, "PipelineLog.Log.PipelineHasFinished"));

        initialized = false;
        halted = false;
        halting = false;
        safeStopping = false;

        updateGui();

        // OK, also see if we had a debugging session going on.
        // If so and we didn't hit a breakpoint yet, display the show
        // preview dialog...
        //
        if (debug
            && lastPipelineDebugMeta != null
            && lastPipelineDebugMeta.getTotalNumberOfHits() == 0) {
          debug = false;
          showLastPreviewResults();
        }
        debug = false;

        checkErrorVisuals();

        hopDisplay().asyncExec(this::updateGui);
      }
    }
  }

  private void checkErrorVisuals() {
    if (pipeline.getErrors() > 0) {
      // Get the logging text and filter it out. Store it in the transformLogMap...
      //
      transformLogMap = new HashMap<>();
      for (IEngineComponent component : pipeline.getComponents()) {
        if (component.getErrors() > 0) {
          String logText = component.getLogText();
          transformLogMap.put(component.getName(), logText);
        }
      }

    } else {
      transformLogMap = null;
    }
    // Redraw the canvas to show the error icons etc.
    //
    hopDisplay().asyncExec(() -> redraw());
  }

  public synchronized void showLastPreviewResults() {
    if (lastPipelineDebugMeta == null
        || lastPipelineDebugMeta.getTransformDebugMetaMap().isEmpty()) {
      return;
    }

    final List<String> transformnames = new ArrayList<>();
    final List<IRowMeta> rowMetas = new ArrayList<>();
    final List<List<Object[]>> rowBuffers = new ArrayList<>();

    // Assemble the buffers etc in the old style...
    //
    for (TransformMeta transformMeta : lastPipelineDebugMeta.getTransformDebugMetaMap().keySet()) {
      TransformDebugMeta transformDebugMeta =
          lastPipelineDebugMeta.getTransformDebugMetaMap().get(transformMeta);

      transformnames.add(transformMeta.getName());
      rowMetas.add(transformDebugMeta.getRowBufferMeta());
      rowBuffers.add(transformDebugMeta.getRowBuffer());
    }

    hopDisplay()
        .asyncExec(
            () -> {
              EnterPreviewRowsDialog dialog =
                  new EnterPreviewRowsDialog(
                      hopShell(), SWT.NONE, transformnames, rowMetas, rowBuffers);
              dialog.open();
            });
  }

  public boolean isRunning() {
    if (pipeline == null) {
      return false;
    }
    if (pipeline.isStopped()) {
      return false;
    }
    if (pipeline.isPreparing()) {
      return true;
    }
    if (pipeline.isRunning()) {
      return true;
    }
    return false;
  }

  @Override
  public IHasLogChannel getLogChannelProvider() {
    return () -> getPipeline() != null ? getPipeline().getLogChannel() : LogChannel.GENERAL;
  }

  @GuiContextAction(
      id = "pipeline-graph-transform-12000-sniff-output",
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Info,
      name = "i18n::HopGuiPipelineGraph.PipelineAction.SniffOutput.Name",
      tooltip = "i18n::HopGuiPipelineGraph.PipelineAction.SniffOutput.Tooltip",
      image = "ui/images/preview.svg",
      category = "Preview",
      categoryOrder = "3")
  public void sniff(HopGuiPipelineTransformContext context) {
    TransformMeta transformMeta = context.getTransformMeta();

    if (pipeline == null) {
      MessageBox messageBox = new MessageBox(hopShell(), SWT.ICON_INFORMATION | SWT.OK);
      messageBox.setText(
          BaseMessages.getString(PKG, "PipelineGraph.SniffTestingAvailableWhenRunning.Title"));
      messageBox.setMessage(
          BaseMessages.getString(PKG, "PipelineGraph.SniffTestingAvailableWhenRunning.Message"));
      messageBox.open();
      return;
    }
    if (pipeline.isFinished()) {
      // Show collected sample data...
      //

    } else {
      try {
        pipeline.retrieveComponentOutput(
            hopGui.getVariables(),
            transformMeta.getName(),
            0,
            50,
            ((pipelineEngine, rowBuffer) ->
                hopDisplay()
                    .asyncExec(
                        () -> {
                          PreviewRowsDialog dialog =
                              new PreviewRowsDialog(
                                  hopShell(),
                                  hopGui.getVariables(),
                                  SWT.NONE,
                                  transformMeta.getName(),
                                  rowBuffer.getRowMeta(),
                                  rowBuffer.getBuffer());
                          dialog.open();
                        })));
      } catch (HopException e) {
        new ErrorDialog(hopShell(), CONST_ERROR, "Error sniffing rows", e);
      }
    }
  }

  @Override
  public ILogChannel getLogChannel() {
    return log;
  }

  /**
   * Edit the transform of the given pipeline
   *
   * @param pipelineMeta
   * @param transformMeta
   */
  public void editTransform(PipelineMeta pipelineMeta, TransformMeta transformMeta) {
    pipelineTransformDelegate.editTransform(pipelineMeta, transformMeta);
  }

  public String buildTabName() throws HopException {
    String tabName = null;
    String realFilename = variables.resolve(pipelineMeta.getFilename());
    if (StringUtils.isEmpty(realFilename)) {
      tabName = pipelineMeta.getName();
    } else {
      try {
        FileObject fileObject = HopVfs.getFileObject(pipelineMeta.getFilename());
        FileName fileName = fileObject.getName();
        tabName = fileName.getBaseName();
      } catch (Exception e) {
        throw new HopException(
            "Unable to get information from file name '" + pipelineMeta.getFilename() + "'", e);
      }
    }
    return tabName;
  }

  /**
   * Handle if pipeline filename is set and changed saved
   *
   * <p>Prompt auto save feature...
   *
   * @param pipelineMeta
   * @return true if pipeline meta has name and if changed is saved
   * @throws HopException
   */
  public boolean handlePipelineMetaChanges(PipelineMeta pipelineMeta) throws HopException {
    if (pipelineMeta.hasChanged()) {
      if (StringUtils.isNotEmpty(pipelineMeta.getFilename()) && hopGui.getProps().getAutoSave()) {
        save();
      } else {
        MessageDialogWithToggle md =
            new MessageDialogWithToggle(
                hopShell(),
                BaseMessages.getString(PKG, "PipelineLog.Dialog.FileHasChanged.Title"),
                BaseMessages.getString(PKG, "PipelineLog.Dialog.FileHasChanged1.Message")
                    + Const.CR
                    + BaseMessages.getString(PKG, "PipelineLog.Dialog.FileHasChanged2.Message")
                    + Const.CR,
                SWT.ICON_QUESTION,
                new String[] {
                  BaseMessages.getString(PKG, "System.Button.Yes"),
                  BaseMessages.getString(PKG, "System.Button.No")
                },
                BaseMessages.getString(PKG, "PipelineLog.Dialog.Option.AutoSavePipeline"),
                hopGui.getProps().getAutoSave());
        int answer = md.open();
        if (answer == 0) { // Yes, save
          String filename = pipelineMeta.getFilename();
          if (StringUtils.isEmpty(filename)) {
            // Ask for the filename: saveAs
            //
            filename =
                BaseDialog.presentFileDialog(
                    true,
                    hopGui.getShell(),
                    fileType.getFilterExtensions(),
                    fileType.getFilterNames(),
                    true);
            if (filename != null) {
              filename = hopGui.getVariables().resolve(filename);
              saveAs(filename);
            }
          } else {
            save();
          }
        }
        hopGui.getProps().setAutoSave(md.getToggleState());
      }
    }

    return StringUtils.isNotEmpty(pipelineMeta.getFilename()) && !pipelineMeta.hasChanged();
  }

  private TransformMeta lastChained = null;

  public void addTransformToChain(IPlugin transformPlugin, boolean shift) {
    // Is the lastChained entry still valid?
    //
    if (lastChained != null && pipelineMeta.findTransform(lastChained.getName()) == null) {
      lastChained = null;
    }

    // If there is exactly one selected transform, pick that one as last chained.
    //
    List<TransformMeta> sel = pipelineMeta.getSelectedTransforms();
    if (sel.size() == 1) {
      lastChained = sel.get(0);
    }

    // Where do we add this?

    Point p = null;
    if (lastChained == null) {
      p = pipelineMeta.getMaximum();
      p.x -= 100;
    } else {
      p = new Point(lastChained.getLocation().x, lastChained.getLocation().y);
    }

    p.x += 200;

    // Which is the new transform?

    TransformMeta newTransform =
        pipelineTransformDelegate.newTransform(
            pipelineMeta,
            transformPlugin.getIds()[0],
            transformPlugin.getName(),
            transformPlugin.getName(),
            false,
            true,
            p);
    if (newTransform == null) {
      return;
    }
    PropsUi.setLocation(newTransform, p.x, p.y);

    if (lastChained != null) {
      PipelineHopMeta hop = new PipelineHopMeta(lastChained, newTransform);
      pipelineHopDelegate.newHop(pipelineMeta, hop);
    }

    lastChained = newTransform;

    if (shift) {
      editTransform(newTransform);
    }

    pipelineMeta.unselectAll();
    newTransform.setSelected(true);

    updateGui();
  }

  public HopGui getHopGui() {
    return hopGui;
  }

  public void setHopGui(HopGui hopGui) {
    this.hopGui = hopGui;
  }

  @Override
  public Object getSubject() {
    return pipelineMeta;
  }

  private void setHopEnabled(PipelineHopMeta hop, boolean enabled) {
    hop.setEnabled(enabled);
    pipelineMeta.clearCaches();
  }

  private void modalMessageDialog(String title, String message, int swtFlags) {
    MessageBox messageBox = new MessageBox(hopShell(), swtFlags);
    messageBox.setMessage(message);
    messageBox.setText(title);
    messageBox.open();
  }

  /**
   * Gets fileType
   *
   * @return value of fileType
   */
  @Override
  public HopPipelineFileType<PipelineMeta> getFileType() {
    return fileType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HopGuiPipelineGraph that = (HopGuiPipelineGraph) o;
    return Objects.equals(pipelineMeta, that.pipelineMeta) && Objects.equals(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipelineMeta, id);
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_UNDO_ID,
      // label = "Undo",
      toolTip = "i18n:org.apache.hop.ui.hopgui:HopGui.Toolbar.Undo.Tooltip",
      image = "ui/images/undo.svg",
      separator = true)
  @GuiKeyboardShortcut(control = true, key = 'z')
  @GuiOsxKeyboardShortcut(command = true, key = 'z')
  @Override
  public void undo() {
    pipelineUndoDelegate.undoPipelineAction(this, pipelineMeta);
    forceFocus();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_REDO_ID,
      // label = "Redo",
      toolTip = "i18n:org.apache.hop.ui.hopgui:HopGui.Toolbar.Redo.Tooltip",
      image = "ui/images/redo.svg")
  @GuiKeyboardShortcut(control = true, shift = true, key = 'z')
  @GuiOsxKeyboardShortcut(command = true, shift = true, key = 'z')
  @Override
  public void redo() {
    pipelineUndoDelegate.redoPipelineAction(this, pipelineMeta);
    forceFocus();
  }

  /**
   * Update the representation, toolbar, menus and so on. This is needed after a file, context or
   * capabilities changes
   */
  @Override
  public void updateGui() {
    if (hopGui == null || toolBarWidgets == null || toolBar == null || toolBar.isDisposed()) {
      return;
    }

    hopDisplay()
        .asyncExec(
            () -> {
              setZoomLabel();

              // Enable/disable the undo/redo toolbar buttons...
              //
              toolBarWidgets.enableToolbarItem(
                  TOOLBAR_ITEM_UNDO_ID, pipelineMeta.viewThisUndo() != null);
              toolBarWidgets.enableToolbarItem(
                  TOOLBAR_ITEM_REDO_ID, pipelineMeta.viewNextUndo() != null);

              boolean running = isRunning();
              boolean paused = running && pipeline.isPaused();
              toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_START, !running || paused);
              toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_STOP, running);
              toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_PAUSE, running && !paused);

              hopGui.setUndoMenu(pipelineMeta);
              hopGui.handleFileCapabilities(fileType, pipelineMeta.hasChanged(), running, paused);

              // Enable the align/distribute toolbar menus if one or more transforms are selected.
              //
              super.enableSnapAlignDistributeMenuItems(
                  fileType, !pipelineMeta.getSelectedTransforms().isEmpty());

              try {
                ExtensionPointHandler.callExtensionPoint(
                    LogChannel.UI,
                    variables,
                    HopGuiExtensionPoint.HopGuiPipelineGraphUpdateGui.id,
                    this);
              } catch (Exception xe) {
                LogChannel.UI.logError("Error handling extension point 'HopGuiFileOpenDialog'", xe);
              }

              HopGuiPipelineGraph.super.redraw();
            });
  }

  @Override
  public boolean forceFocus() {
    return canvas.forceFocus();
  }

  @GuiKeyboardShortcut(control = true, key = 'a')
  @GuiOsxKeyboardShortcut(command = true, key = 'a')
  @Override
  public void selectAll() {
    pipelineMeta.selectAll();
    updateGui();
  }

  @GuiKeyboardShortcut(key = SWT.ESC)
  @Override
  public void unselectAll() {
    clearSettings();
    updateGui();
  }

  @GuiKeyboardShortcut(control = true, key = 'c')
  @GuiOsxKeyboardShortcut(command = true, key = 'c')
  @Override
  public void copySelectedToClipboard() {
    if (pipelineLogDelegate.hasSelectedText()) {
      pipelineLogDelegate.copySelected();
    } else {
      pipelineClipboardDelegate.copySelected(
          pipelineMeta, pipelineMeta.getSelectedTransforms(), pipelineMeta.getSelectedNotes());
    }
  }

  @GuiKeyboardShortcut(control = true, key = 'x')
  @GuiOsxKeyboardShortcut(command = true, key = 'x')
  @Override
  public void cutSelectedToClipboard() {
    pipelineClipboardDelegate.copySelected(
        pipelineMeta, pipelineMeta.getSelectedTransforms(), pipelineMeta.getSelectedNotes());
    pipelineTransformDelegate.delTransforms(pipelineMeta, pipelineMeta.getSelectedTransforms());
    notePadDelegate.deleteNotes(pipelineMeta, pipelineMeta.getSelectedNotes());
  }

  @GuiKeyboardShortcut(key = SWT.DEL)
  @Override
  public void deleteSelected() {
    delSelected(null);
    updateGui();
  }

  @GuiKeyboardShortcut(control = true, key = 'v')
  @GuiOsxKeyboardShortcut(command = true, key = 'v')
  @Override
  public void pasteFromClipboard() {
    pasteFromClipboard(new Point(currentMouseX, currentMouseY));
  }

  public void pasteFromClipboard(Point location) {
    final String clipboard = pipelineClipboardDelegate.fromClipboard();
    pipelineClipboardDelegate.pasteXml(pipelineMeta, clipboard, location);
  }

  @GuiContextAction(
      id = "pipeline-graph-transform-10200-past-from-clipboard",
      parentId = HopGuiPipelineContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiPipelineGraph.PipelineAction.PasteFromClipboard.Name",
      tooltip = "i18n::HopGuiPipelineGraph.PipelineAction.PasteFromClipboard.Tooltip",
      image = "ui/images/paste.svg",
      category = "Basic",
      categoryOrder = "1")
  public void pasteFromClipboard(HopGuiPipelineContext context) {
    pasteFromClipboard(context.getClick());
  }

  @GuiContextAction(
      id = "pipeline-graph-transform-10010-copy-transform-to-clipboard",
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Custom,
      name = "i18n::HopGuiPipelineGraph.PipelineAction.CopyToClipboard.Name",
      tooltip = "i18n::HopGuiPipelineGraph.PipelineAction.CopyToClipboard.Tooltip",
      image = "ui/images/copy.svg",
      category = "Basic",
      categoryOrder = "1")
  public void copyTransformToClipboard(HopGuiPipelineTransformContext context) {
    pipelineClipboardDelegate.copySelected(
        pipelineMeta, Arrays.asList(context.getTransformMeta()), Collections.emptyList());
  }

  @GuiKeyboardShortcut(key = ' ')
  @GuiOsxKeyboardShortcut(key = ' ')
  public void showOutputFields() {
    if (lastMove != null) {

      // Hide the tooltip!
      hideToolTips();

      // Find the transform
      TransformMeta transformMeta = pipelineMeta.getTransform(lastMove.x, lastMove.y, iconSize);
      if (transformMeta != null) {
        // Show the output fields...
        //
        inputOutputFields(transformMeta, false);
      }
    }
  }

  @GuiKeyboardShortcut(key = 'z')
  @GuiOsxKeyboardShortcut(key = 'z')
  public void openReferencedObject() {
    if (lastMove != null) {

      // Hide the tooltip!
      hideToolTips();

      // Find the transform
      TransformMeta transformMeta = pipelineMeta.getTransform(lastMove.x, lastMove.y, iconSize);
      if (transformMeta != null) {

        // Open referenced object...
        //
        ITransformMeta iTransformMeta = transformMeta.getTransform();
        String[] objectDescriptions = iTransformMeta.getReferencedObjectDescriptions();
        if (objectDescriptions == null || objectDescriptions.length == 0) {
          return;
        }
        // Only one reference?: open immediately
        //
        if (objectDescriptions.length == 1) {
          HopGuiPipelineTransformContext.openReferencedObject(
              pipelineMeta, variables, iTransformMeta, objectDescriptions[0], 0);
        } else {
          // Show Selection dialog...
          //
          EnterSelectionDialog dialog =
              new EnterSelectionDialog(
                  getShell(),
                  objectDescriptions,
                  BaseMessages.getString(
                      PKG, "HopGuiPipelineGraph.OpenReferencedObject.Selection.Title"),
                  BaseMessages.getString(
                      PKG, "HopGuiPipelineGraph.OpenReferencedObject.Selection.Message"));
          String answer = dialog.open(0);
          if (answer != null) {
            int index = dialog.getSelectionNr();
            HopGuiPipelineTransformContext.openReferencedObject(
                pipelineMeta, variables, iTransformMeta, answer, index);
          }
        }
      }
    }
  }

  @Override
  public List<IGuiContextHandler> getContextHandlers() {
    return new ArrayList<>();
  }

  @GuiContextAction(
      id = "pipeline-graph-navigate-to-execution-info",
      parentId = HopGuiPipelineContext.CONTEXT_ID,
      type = GuiActionType.Info,
      name = "i18n::HopGuiPipelineGraph.ContextualAction.NavigateToExecutionInfo.Text",
      tooltip = "i18n::HopGuiPipelineGraph.ContextualAction.NavigateToExecutionInfo.Tooltip",
      image = "ui/images/execution.svg",
      category = "Basic",
      categoryOrder = "1")
  public void navigateToExecutionInfo(HopGuiPipelineContext context) {
    navigateToExecutionInfo();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_TO_EXECUTION_INFO,
      toolTip = "i18n:org.apache.hop.ui.hopgui:HopGui.Toolbar.ToExecutionInfo",
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/execution.svg")
  public void navigateToExecutionInfo() {
    try {
      // Is there an active IPipeline?
      //
      ExecutionPerspective ep = HopGui.getExecutionPerspective();

      if (pipeline != null) {
        IExecutionViewer viewer = ep.findViewer(pipeline.getLogChannelId(), pipelineMeta.getName());
        if (viewer != null) {
          ep.setActiveViewer(viewer);
          ep.activate();
          return;
        } else {
          // We know the location, look it up
          //
          ep.refresh();

          // Get the location
          String locationName =
              variables.resolve(
                  pipeline.getPipelineRunConfiguration().getExecutionInfoLocationName());
          if (StringUtils.isNotEmpty(locationName)) {
            ExecutionInfoLocation location = ep.getLocationMap().get(locationName);
            IExecutionInfoLocation iLocation = location.getExecutionInfoLocation();
            Execution execution = iLocation.getExecution(pipeline.getLogChannelId());
            if (execution != null) {
              ExecutionState executionState =
                  location.getExecutionInfoLocation().getExecutionState(execution.getId());
              ep.createExecutionViewer(locationName, execution, executionState);
              ep.activate();
              return;
            }
          }
        }
      }

      MultiMetadataProvider metadataProvider = hopGui.getMetadataProvider();

      // As a fallback, try to open the last execution info for this pipeline
      //
      IHopMetadataSerializer<ExecutionInfoLocation> serializer =
          metadataProvider.getSerializer(ExecutionInfoLocation.class);
      List<String> locationNames = serializer.listObjectNames();
      if (locationNames.isEmpty()) {
        return;
      }
      ExecutionInfoLocation location;
      if (locationNames.size() == 1) {
        // No need to ask which location, just pick this one
        location = serializer.load(locationNames.get(0));
      } else {
        EnterSelectionDialog dialog =
            new EnterSelectionDialog(
                getShell(),
                locationNames.toArray(new String[0]),
                "Select location",
                "Select the execution information location to query");
        String locationName = dialog.open();
        if (locationName != null) {
          location = serializer.load(locationName);
        } else {
          return;
        }
      }

      ep.createLastExecutionView(
          location.getName(), ExecutionType.Pipeline, pipelineMeta.getName());
      ep.activate();
    } catch (Exception e) {
      new ErrorDialog(
          getShell(),
          CONST_ERROR,
          "Error navigating to the latest execution information for this pipeline",
          e);
    }
  }

  @Override
  public void reload() {
    try {
      pipelineMeta.loadXml(getFilename(), hopGui.getMetadataProvider(), hopGui.getVariables());
    } catch (HopXmlException | HopMissingPluginsException e) {
      LogChannel.GENERAL.logError("Error reloading pipeline xml file", e);
    }
    redraw();
    updateGui();
  }

  @GuiContextAction(
      id = ACTION_ID_PIPELINE_GRAPH_TRANSFORM_VIEW_EXECUTION_INFO,
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Info,
      name = "i18n::HopGuiPipelineGraph.TransformAction.ViewExecutionInfo.Name",
      tooltip = "i18n::HopGuiPipelineGraph.TransformAction.ViewExecutionInfo.Tooltip",
      image = "ui/images/execution.svg",
      category = "Basic",
      categoryOrder = "1")
  public void viewTransformExecutionInfo(HopGuiPipelineTransformContext context) {
    try {
      if (pipeline == null) {
        return;
      }
      PipelineRunConfiguration runConfiguration = pipeline.getPipelineRunConfiguration();
      String locationName = variables.resolve(runConfiguration.getExecutionInfoLocationName());
      if (StringUtils.isEmpty(locationName)) {
        return;
      }

      ExecutionPerspective executionPerspective = HopGui.getExecutionPerspective();
      executionPerspective.refresh();

      ExecutionInfoLocation location = executionPerspective.getLocationMap().get(locationName);
      if (location == null) {
        throw new HopException(
            "Unable to find execution information location '"
                + locationName
                + "' in the execution information perspective");
      }
      IExecutionInfoLocation iLocation = location.getExecutionInfoLocation();

      TransformMeta transformMeta = context.getTransformMeta();
      List<IEngineComponent> components = pipeline.getComponentCopies(transformMeta.getName());
      if (components.isEmpty()) {
        throw new HopException("Hop couldn't find any running copies of this transform.");
      }

      // We select the first copy and execution.
      // Later we can look up all executions of this transform and allow the user to select one.
      //
      IEngineComponent component = components.get(0);
      String transformId = component.getLogChannelId();

      List<Execution> executions = iLocation.findExecutions(transformId);
      if (!executions.isEmpty()) {
        Execution execution = executions.get(0);
        ExecutionState executionState = iLocation.getExecutionState(execution.getId());
        executionPerspective.createExecutionViewer(locationName, execution, executionState);
        executionPerspective.activate();
      }
    } catch (Exception e) {
      new ErrorDialog(getShell(), CONST_ERROR, "Error looking up execution information", e);
    }
  }
}
