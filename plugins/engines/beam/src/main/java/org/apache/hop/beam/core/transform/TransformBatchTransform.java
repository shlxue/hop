package org.apache.hop.beam.core.transform;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.metastore.SerializableMetaStore;
import org.apache.hop.beam.core.shared.VariableValue;
import org.apache.hop.beam.core.util.HopBeamUtil;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.SingleThreadedPipelineExecutor;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.IRowListener;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class TransformBatchTransform extends TransformTransform {

  public TransformBatchTransform() {
    super();
  }

  public TransformBatchTransform( List<VariableValue> variableValues, String metastoreJson, List<String> transformPluginClasses, List<String> xpPluginClasses,
                                  int batchSize, int flushIntervalMs, String transformName, String stepPluginId, String stepMetaInterfaceXml, String inputRowMetaJson, boolean inputStep,
                                  List<String> targetSteps, List<String> infoSteps, List<String> infoRowMetaJsons, List<PCollectionView<List<HopRow>>> infoCollectionViews ) {
    super(variableValues, metastoreJson, transformPluginClasses, xpPluginClasses, batchSize, flushIntervalMs, transformName, stepPluginId,
      stepMetaInterfaceXml, inputRowMetaJson, inputStep, targetSteps, infoSteps, infoRowMetaJsons, infoCollectionViews);
  }

  @Override public PCollectionTuple expand( PCollection<HopRow> input ) {
    try {
      // Only initialize once on this node/vm
      //
      BeamHop.init( transformPluginClasses, xpPluginClasses );

      // Similar for the output : treate a TupleTag list for the target transforms...
      //
      TupleTag<HopRow> mainOutputTupleTag = new TupleTag<HopRow>( HopBeamUtil.createMainOutputTupleId( transformName ) ) {
      };
      List<TupleTag<HopRow>> targetTupleTags = new ArrayList<>();
      TupleTagList targetTupleTagList = null;
      for ( String targetStep : targetSteps ) {
        String tupleId = HopBeamUtil.createTargetTupleId( transformName, targetStep );
        TupleTag<HopRow> tupleTag = new TupleTag<HopRow>( tupleId ) {
        };
        targetTupleTags.add( tupleTag );
        if ( targetTupleTagList == null ) {
          targetTupleTagList = TupleTagList.of( tupleTag );
        } else {
          targetTupleTagList = targetTupleTagList.and( tupleTag );
        }
      }
      if ( targetTupleTagList == null ) {
        targetTupleTagList = TupleTagList.empty();
      }

      // Create a new transform function, initializes the transform
      //
      StepBatchFn stepBatchFn = new StepBatchFn( variableValues, metastoreJson, transformPluginClasses, xpPluginClasses,
        transformName, stepPluginId, stepMetaInterfaceXml, inputRowMetaJson, inputStep,
        targetSteps, infoSteps, infoRowMetaJsons );

      // The actual transform functionality
      //
      ParDo.SingleOutput<HopRow, HopRow> parDoStepFn = ParDo.of( stepBatchFn );

      // Add optional side inputs...
      //
      if ( infoCollectionViews.size() > 0 ) {
        parDoStepFn = parDoStepFn.withSideInputs( infoCollectionViews );
      }

      // Specify the main output and targeted outputs
      //
      ParDo.MultiOutput<HopRow, HopRow> multiOutput = parDoStepFn.withOutputTags( mainOutputTupleTag, targetTupleTagList );

      // Apply the multi output parallel do transform function to the main input stream
      //
      PCollectionTuple collectionTuple = input.apply( multiOutput );

      // In the tuple is everything we need to find.
      // Just make sure to retrieve the PCollections using the correct Tuple ID
      // Use HopBeamUtil.createTargetTupleId()... to make sure
      //
      return collectionTuple;
    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error transforming data in transform '" + transformName + "'", e );
      throw new RuntimeException( "Error transforming data in transform", e );
    }

  }

  private class StepBatchFn extends DoFn<HopRow, HopRow> {

    private static final long serialVersionUID = 95700000000000002L;

    public static final String INJECTOR_STEP_NAME = "_INJECTOR_";

    protected List<VariableValue> variableValues;
    protected String metastoreJson;
    protected List<String> transformPluginClasses;
    protected List<String> xpPluginClasses;
    protected String transformName;
    protected String stepPluginId;
    protected String stepMetaInterfaceXml;
    protected String inputRowMetaJson;
    protected List<String> targetSteps;
    protected List<String> infoSteps;
    protected List<String> infoRowMetaJsons;
    protected boolean inputStep;
    protected boolean initialize;

    protected List<PCollection<HopRow>> infoCollections;

    // Log and count parse errors.
    private final Counter numErrors = Metrics.counter( "main", "TransformProcessErrors" );

    private transient PipelineMeta pipelineMeta;
    private transient TransformMeta transformMeta;
    private transient IRowMeta inputRowMeta;
    private transient IRowMeta outputRowMeta;
    private transient List<TransformMetaDataCombi> stepCombis;
    private transient LocalPipelineEngine pipeline;
    private transient RowProducer rowProducer;
    private transient IRowListener rowListener;
    private transient List<Object[]> resultRows;
    private transient List<List<Object[]>> targetResultRowsList;
    private transient List<IRowMeta> targetRowMetas;
    private transient List<IRowMeta> infoRowMetas;
    private transient List<RowProducer> infoRowProducers;

    private transient TupleTag<HopRow> mainTupleTag;
    private transient List<TupleTag<HopRow>> tupleTagList;

    private transient Counter initCounter;
    private transient Counter readCounter;
    private transient Counter writtenCounter;
    private transient Counter flushBufferCounter;

    private transient SingleThreadedPipelineExecutor executor;

    private transient Queue<HopRow> rowBuffer;
    private transient BoundedWindow batchWindow;

    private transient AtomicLong lastTimerCheck;
    private transient Timer timer;

    public StepBatchFn() {
    }


    // I created a private class because instances of this one need access to infoCollectionViews
    //

    public StepBatchFn( List<VariableValue> variableValues, String metastoreJson, List<String> transformPluginClasses, List<String> xpPluginClasses, String transformName, String stepPluginId,
                        String stepMetaInterfaceXml, String inputRowMetaJson, boolean inputStep,
                        List<String> targetSteps, List<String> infoSteps, List<String> infoRowMetaJsons ) {
      this();
      this.variableValues = variableValues;
      this.metastoreJson = metastoreJson;
      this.transformPluginClasses = transformPluginClasses;
      this.xpPluginClasses = xpPluginClasses;
      this.transformName = transformName;
      this.stepPluginId = stepPluginId;
      this.stepMetaInterfaceXml = stepMetaInterfaceXml;
      this.inputRowMetaJson = inputRowMetaJson;
      this.inputStep = inputStep;
      this.targetSteps = targetSteps;
      this.infoSteps = infoSteps;
      this.infoRowMetaJsons = infoRowMetaJsons;
      this.initialize = true;
    }

    /**
     * Reset the row buffer every time we start a new bundle to prevent the output of double rows
     *
     * @param startBundleContext
     */
    @StartBundle
    public void startBundle( StartBundleContext startBundleContext ) {
      Metrics.counter( "startBundle", transformName ).inc();
      if ( "ScriptValueMod".equals( stepPluginId ) && pipeline != null ) {
        initialize = true;
      }
    }

    @Setup
    public void setup() {
      try {
        rowBuffer = new ConcurrentLinkedQueue();
      } catch ( Exception e ) {
        numErrors.inc();
        LOG.info( "Transform '" + transformName + "' : setup error :" + e.getMessage() );
        throw new RuntimeException( "Unable to set up transform " + transformName, e );
      }
    }

    @Teardown
    public void tearDown() {
      if ( timer != null ) {
        timer.cancel();
      }
    }

    @ProcessElement
    public void processElement( ProcessContext context, BoundedWindow window ) {

      try {

        if ( initialize ) {
          initialize = false;

          // Initialize Hop and load extra plugins as well
          //
          BeamHop.init( transformPluginClasses, xpPluginClasses );

          // The content of the metastore is JSON serialized and inflated below.
          //
          IMetaStore metaStore = new SerializableMetaStore( metastoreJson );

          // Create a very simple new transformation to run single threaded...
          // Single threaded...
          //
          pipelineMeta = new PipelineMeta();
          pipelineMeta.setPipelineType( PipelineMeta.PipelineType.SingleThreaded );
          pipelineMeta.setMetaStore( metaStore );

          // When the first row ends up in the buffer we start the timer.
          // If the rows are flushed out we reset back to -1;
          //
          lastTimerCheck = new AtomicLong( -1L );

          // Give transforms variables from above
          //
          for ( VariableValue variableValue : variableValues ) {
            if ( StringUtils.isNotEmpty( variableValue.getVariable() ) ) {
              pipelineMeta.setVariable( variableValue.getVariable(), variableValue.getValue() );
            }
          }

          // Input row metadata...
          //
          inputRowMeta = JsonRowMeta.fromJson( inputRowMetaJson );
          infoRowMetas = new ArrayList<>();
          for ( String infoRowMetaJson : infoRowMetaJsons ) {
            IRowMeta infoRowMeta = JsonRowMeta.fromJson( infoRowMetaJson );
            infoRowMetas.add( infoRowMeta );
          }

          // Create an Injector transform with the right row layout...
          // This will help all transforms see the row layout statically...
          //
          TransformMeta mainInjectorStepMeta = null;
          if ( !inputStep ) {
            mainInjectorStepMeta = createInjectorStep( pipelineMeta, INJECTOR_STEP_NAME, inputRowMeta, 200, 200 );
          }

          // Our main transform writes to a bunch of targets
          // Add a dummy transform for each one so the transform can target them
          //
          int targetLocationY = 200;
          List<TransformMeta> targetStepMetas = new ArrayList<>();
          for ( String targetStep : targetSteps ) {
            DummyMeta dummyMeta = new DummyMeta();
            TransformMeta targetStepMeta = new TransformMeta( targetStep, dummyMeta );
            targetStepMeta.setLocation( 600, targetLocationY );
            targetLocationY += 150;

            targetStepMetas.add( targetStepMeta );
            pipelineMeta.addTransform( targetStepMeta );
          }

          // The transform might read information from info transforms
          // Steps like "Stream Lookup" or "Validator"
          // They read all the data on input from a side input
          //
          List<List<HopRow>> infoDataSets = new ArrayList<>();
          List<TransformMeta> infoStepMetas = new ArrayList<>();
          for ( int i = 0; i < infoSteps.size(); i++ ) {
            String infoStep = infoSteps.get( i );
            PCollectionView<List<HopRow>> cv = infoCollectionViews.get( i );

            // Get the data from the side input, from the info transform(s)
            //
            List<HopRow> infoDataSet = context.sideInput( cv );
            infoDataSets.add( infoDataSet );

            IRowMeta infoRowMeta = infoRowMetas.get( i );

            // Add an Injector transform for every info transform so the transform can read from it
            //
            TransformMeta infoStepMeta = createInjectorStep( pipelineMeta, infoStep, infoRowMeta, 200, 350 + 150 * i );
            infoStepMetas.add( infoStepMeta );
          }

          stepCombis = new ArrayList<>();

          // The main transform inflated from XML metadata...
          //
          PluginRegistry registry = PluginRegistry.getInstance();
          ITransformMeta iTransformMeta = registry.loadClass( TransformPluginType.class, stepPluginId, ITransformMeta.class );
          if ( iTransformMeta == null ) {
            throw new HopException( "Unable to load transform plugin with ID " + stepPluginId + ", this plugin isn't in the plugin registry or classpath" );
          }

          HopBeamUtil.loadTransformMetadataFromXml( transformName, iTransformMeta, stepMetaInterfaceXml, pipelineMeta.getMetaStore() );

          transformMeta = new TransformMeta( transformName, iTransformMeta );
          transformMeta.setTransformPluginId( stepPluginId );
          transformMeta.setLocation( 400, 200 );
          pipelineMeta.addTransform( transformMeta );
          if ( !inputStep ) {
            pipelineMeta.addPipelineHop( new PipelineHopMeta( mainInjectorStepMeta, transformMeta ) );
          }
          // The target hops as well
          //
          for ( TransformMeta targetStepMeta : targetStepMetas ) {
            pipelineMeta.addPipelineHop( new PipelineHopMeta( transformMeta, targetStepMeta ) );
          }

          // And the info hops...
          //
          for ( TransformMeta infoStepMeta : infoStepMetas ) {
            pipelineMeta.addPipelineHop( new PipelineHopMeta( infoStepMeta, transformMeta ) );
          }

          iTransformMeta.searchInfoAndTargetTransforms( pipelineMeta.getTransforms() );

          // Create the transformation...
          //
          pipeline = new LocalPipelineEngine( pipelineMeta, new LoggingObject( "apache-beam-transform" ) );
          pipeline.setLogLevel( LogLevel.ERROR );
          pipeline.setMetaStore( pipelineMeta.getMetaStore() );
          pipeline.prepareExecution();

          // Create producers so we can efficiently pass data
          //
          rowProducer = null;
          if ( !inputStep ) {
            rowProducer = pipeline.addRowProducer( INJECTOR_STEP_NAME, 0 );
          }
          infoRowProducers = new ArrayList<>();
          for ( String infoStep : infoSteps ) {
            RowProducer infoRowProducer = pipeline.addRowProducer( infoStep, 0 );
            infoRowProducers.add( infoRowProducer );
          }

          // Find the right interfaces for execution later...
          //
          if ( !inputStep ) {
            TransformMetaDataCombi injectorCombi = findCombi( pipeline, INJECTOR_STEP_NAME );
            stepCombis.add( injectorCombi );
          }

          TransformMetaDataCombi stepCombi = findCombi( pipeline, transformName );
          stepCombis.add( stepCombi );
          outputRowMeta = pipelineMeta.getTransformFields( transformName );

          if ( targetSteps.isEmpty() ) {
            rowListener = new RowAdapter() {
              @Override public void rowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
                resultRows.add( row );
              }
            };
            stepCombi.transform.addRowListener( rowListener );
          }

          // Create a list of TupleTag to direct the target rows
          //
          mainTupleTag = new TupleTag<HopRow>( HopBeamUtil.createMainOutputTupleId( transformName ) ) {
          };
          tupleTagList = new ArrayList<>();

          // The lists in here will contain all the rows that ended up in the various target transforms (if any)
          //
          targetRowMetas = new ArrayList<>();
          targetResultRowsList = new ArrayList<>();

          for ( String targetStep : targetSteps ) {
            TransformMetaDataCombi targetCombi = findCombi( pipeline, targetStep );
            stepCombis.add( targetCombi );
            targetRowMetas.add( pipelineMeta.getTransformFields( stepCombi.transformName ) );

            String tupleId = HopBeamUtil.createTargetTupleId( transformName, targetStep );
            TupleTag<HopRow> tupleTag = new TupleTag<HopRow>( tupleId ) {
            };
            tupleTagList.add( tupleTag );
            final List<Object[]> targetResultRows = new ArrayList<>();
            targetResultRowsList.add( targetResultRows );

            targetCombi.transform.addRowListener( new RowAdapter() {
              @Override public void rowReadEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
                // We send the target row to a specific list...
                //
                targetResultRows.add( row );
              }
            } );
          }

          executor = new SingleThreadedPipelineExecutor( pipeline );

          // Initialize the transforms...
          //
          executor.init();

          initCounter = Metrics.counter( Pipeline.METRIC_NAME_INIT, transformName );
          readCounter = Metrics.counter( Pipeline.METRIC_NAME_READ, transformName );
          writtenCounter = Metrics.counter( Pipeline.METRIC_NAME_WRITTEN, transformName );
          flushBufferCounter = Metrics.counter( Pipeline.METRIC_NAME_FLUSH_BUFFER, transformName );

          initCounter.inc();

          // Doesn't really start the threads in single threaded mode
          // Just sets some flags all over the place
          //
          pipeline.startThreads();

          resultRows = new ArrayList<>();

          // Copy the info data sets to the info transforms...
          // We do this only once so all subsequent rows can use this.
          //
          for ( int i = 0; i < infoSteps.size(); i++ ) {
            RowProducer infoRowProducer = infoRowProducers.get( i );
            List<HopRow> infoDataSet = infoDataSets.get( i );
            TransformMetaDataCombi combi = findCombi( pipeline, infoSteps.get( i ) );
            IRowMeta infoRowMeta = infoRowMetas.get( i );

            // Pass and process the rows in the info transforms
            //
            for ( HopRow infoRowData : infoDataSet ) {
              infoRowProducer.putRow( infoRowMeta, infoRowData.getRow() );
              combi.transform.processRow();
            }

            // By calling finished() transforms like Stream Lookup know no more rows are going to come
            // and they can start to work with the info data set
            //
            infoRowProducer.finished();

            // Call once more to flag input as done, transform as finished.
            //
            combi.transform.processRow();
          }

          // Install a timer to check every second if the buffer is stale and needs to be flushed...
          //
          if ( flushIntervalMs > 0 ) {
            TimerTask timerTask = new TimerTask() {
              @Override public void run() {
                // Check on the state of the buffer, flush if needed...
                //
                synchronized ( rowBuffer ) {
                  long difference = System.currentTimeMillis() - lastTimerCheck.get();
                  if ( lastTimerCheck.get()<=0 || difference > flushIntervalMs ) {
                    try {
                      emptyRowBuffer( new StepProcessContext( context ) );
                    } catch ( Exception e ) {
                      throw new RuntimeException( "Unable to flush row buffer when it got stale after " + difference + " ms", e );
                    }
                    lastTimerCheck.set( System.currentTimeMillis() );
                  }
                }
              }
            };
            timer = new Timer( "Flush timer of transform " + transformName );
            timer.schedule( timerTask, 100, 100 );
          }
        }

        // Get one row from the context main input and make a copy so we can change it.
        //
        HopRow originalInputRow = context.element();
        HopRow inputRow = HopBeamUtil.copyHopRow( originalInputRow, inputRowMeta );
        readCounter.inc();

        // Take care of the age of the buffer...
        //
        if ( flushIntervalMs > 0 && rowBuffer.isEmpty() ) {
          lastTimerCheck.set( System.currentTimeMillis() );
        }

        // Add the row to the buffer.
        //
        synchronized ( rowBuffer ) {
          rowBuffer.add( inputRow );
          batchWindow = window;

          synchronized ( rowBuffer ) {
            if ( rowBuffer.size() >= batchSize ) {
              emptyRowBuffer( new StepProcessContext( context ) );
            }
          }
        }
      } catch ( Exception e ) {
        numErrors.inc();
        LOG.info( "Transform execution error :" + e.getMessage() );
        throw new RuntimeException( "Error executing TransformBatchFn", e );
      }
    }

    @FinishBundle
    public void finishBundle( FinishBundleContext context ) {
      try {
        synchronized ( rowBuffer ) {
          if ( !rowBuffer.isEmpty() ) {
            // System.out.println( "Finishing bundle with " + rowBuffer.size() + " rows in the buffer" );
            emptyRowBuffer( new StepFinishBundleContext( context, batchWindow ) );
          }
        }
      } catch ( Exception e ) {
        numErrors.inc();
        LOG.info( "Transform finishing bundle error :" + e.getMessage() );
        throw new RuntimeException( "Error finalizing bundle of transform '" + transformName + "'", e );
      }
    }

    private transient int maxInputBufferSize = 0;
    private transient int minInputBufferSize = Integer.MAX_VALUE;

    /**
     * Attempt to empty the row buffer
     *
     * @param context
     * @throws HopException
     */
    private synchronized void emptyRowBuffer( TupleOutputContext<HopRow> context ) throws HopException {
      synchronized ( rowBuffer ) {

        List<HopRow> buffer = new ArrayList<>();

        // Copy the data to avoid race conditions
        //
        int size = rowBuffer.size();
        for ( int i = 0; i < size; i++ ) {
          HopRow hopRow = rowBuffer.poll();
          buffer.add( hopRow );
        }

        // Only do something if we have work to do
        //
        if ( buffer.isEmpty() ) {
          return;
        }

        if ( !rowBuffer.isEmpty() ) {
          System.err.println( "Async action detected on rowBuffer" );
        }

        // Empty all the row buffers for another iteration
        //
        resultRows.clear();
        for ( int t = 0; t < targetSteps.size(); t++ ) {
          targetResultRowsList.get( t ).clear();
        }

        // Pass the rows in the rowBuffer to the input RowSet
        //
        if ( !inputStep ) {
          int bufferSize = buffer.size();
          if ( maxInputBufferSize < bufferSize ) {
            Metrics.counter( "maxInputSize", transformName ).inc( bufferSize - maxInputBufferSize );
            maxInputBufferSize = bufferSize;
          }
          if ( minInputBufferSize > bufferSize ) {
            if ( minInputBufferSize == Integer.MAX_VALUE ) {
              Metrics.counter( "minInputSize", transformName ).inc( bufferSize );
            } else {
              Metrics.counter( "minInputSize", transformName ).dec( bufferSize - minInputBufferSize );
            }
            minInputBufferSize = bufferSize;
          }

          for ( HopRow inputRow : buffer ) {
            rowProducer.putRow( inputRowMeta, inputRow.getRow() );
          }
        }

        // Execute all transforms in the transformation
        //
        executor.oneIteration();

        // Evaluate the results...
        //

        // Pass all rows in the output to the process context
        //
        for ( Object[] resultRow : resultRows ) {

          // Pass the row to the process context
          //
          context.output( mainTupleTag, new HopRow( resultRow ) );
          writtenCounter.inc();
        }

        // Pass whatever ended up on the target nodes
        //
        for ( int t = 0; t < targetResultRowsList.size(); t++ ) {
          List<Object[]> targetRowsList = targetResultRowsList.get( t );
          TupleTag<HopRow> tupleTag = tupleTagList.get( t );

          for ( Object[] targetRow : targetRowsList ) {
            context.output( tupleTag, new HopRow( targetRow ) );
          }
        }

        flushBufferCounter.inc();
        buffer.clear(); // gc
        lastTimerCheck.set( System.currentTimeMillis() ); // No need to check sooner
      }
    }

    private TransformMeta createInjectorStep( PipelineMeta pipelineMeta, String injectorStepName, IRowMeta injectorRowMeta, int x, int y ) {
      InjectorMeta injectorMeta = new InjectorMeta();
      injectorMeta.allocate( injectorRowMeta.size() );
      for ( int i = 0; i < injectorRowMeta.size(); i++ ) {
        IValueMeta valueMeta = injectorRowMeta.getValueMeta( i );
        injectorMeta.getFieldname()[ i ] = valueMeta.getName();
        injectorMeta.getType()[ i ] = valueMeta.getType();
        injectorMeta.getLength()[ i ] = valueMeta.getLength();
        injectorMeta.getPrecision()[ i ] = valueMeta.getPrecision();
      }
      TransformMeta injectorStepMeta = new TransformMeta( injectorStepName, injectorMeta );
      injectorStepMeta.setLocation( x, y );
      pipelineMeta.addTransform( injectorStepMeta );

      return injectorStepMeta;
    }

    private TransformMetaDataCombi findCombi( Pipeline pipeline, String transformName ) {
      for ( TransformMetaDataCombi combi : pipeline.getTransforms() ) {
        if ( combi.transformName.equals( transformName ) ) {
          return combi;
        }
      }
      throw new RuntimeException( "Configuration error, transform '" + transformName + "' not found in transformation" );
    }
  }

  private interface TupleOutputContext<T> {
    void output( TupleTag<T> tupleTag, T output );
  }

  private class StepProcessContext implements TupleOutputContext<HopRow> {

    private DoFn.ProcessContext context;

    public StepProcessContext( DoFn.ProcessContext processContext ) {
      this.context = processContext;
    }

    @Override public void output( TupleTag<HopRow> tupleTag, HopRow output ) {
      context.output( tupleTag, output );
    }
  }

  private class StepFinishBundleContext implements TupleOutputContext<HopRow> {

    private DoFn.FinishBundleContext context;
    private BoundedWindow batchWindow;

    public StepFinishBundleContext( DoFn.FinishBundleContext context, BoundedWindow batchWindow ) {
      this.context = context;
      this.batchWindow = batchWindow;
    }

    @Override public void output( TupleTag<HopRow> tupleTag, HopRow output ) {
      context.output( tupleTag, output, Instant.now(), batchWindow );
    }
  }
}
