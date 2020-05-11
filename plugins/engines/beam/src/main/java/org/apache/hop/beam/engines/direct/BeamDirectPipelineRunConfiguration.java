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

package org.apache.hop.beam.engines.direct;

import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.engines.BeamPipelineRunConfiguration;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.metastore.RunnerType;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metastore.persist.MetaStoreAttribute;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;

@GuiPlugin
public class BeamDirectPipelineRunConfiguration extends BeamPipelineRunConfiguration implements IBeamPipelineEngineRunConfiguration, IVariables, Cloneable {

  @GuiWidgetElement(
    order = "20000-direct-options",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "Number of workers",
    toolTip =  "Controls the amount of target parallelism the DirectRunner will use. Defaults to"
      + " the greater of the number of available processors and 3. Must be a value greater"
      + " than zero."
  )
  @MetaStoreAttribute(key="number_of_workers")
  private String numberOfWorkers;

  public BeamDirectPipelineRunConfiguration( String numberOfWorkers ) {
    this();
    this.numberOfWorkers = numberOfWorkers;
  }

  public BeamDirectPipelineRunConfiguration() {
    super();
    this.tempLocation = "file://"+System.getProperty( "java.io.tmpdir" );
    this.numberOfWorkers = "";
  }

  public BeamDirectPipelineRunConfiguration( BeamDirectPipelineRunConfiguration config ) {
    super( config );
    this.numberOfWorkers = config.numberOfWorkers;
  }

  public BeamDirectPipelineRunConfiguration clone() {
    return new BeamDirectPipelineRunConfiguration(this);
  }

  @Override public RunnerType getRunnerType() {
    return RunnerType.Direct;
  }

  @Override public PipelineOptions getPipelineOptions() {
    DirectOptions directOptions = PipelineOptionsFactory.as( DirectOptions.class );
    directOptions.setBlockOnRun( !isRunningAsynchronous() );
    if ( StringUtils.isNotEmpty(numberOfWorkers)) {
      int targetParallelism = Const.toInt(environmentSubstitute( numberOfWorkers),  1);
      directOptions.setTargetParallelism(targetParallelism);
    }

    return directOptions;
  }

  @Override public boolean isRunningAsynchronous() {
    return false;
  }

  /**
   * Gets numberOfWorkers
   *
   * @return value of numberOfWorkers
   */
  public String getNumberOfWorkers() {
    return numberOfWorkers;
  }

  /**
   * @param numberOfWorkers The numberOfWorkers to set
   */
  public void setNumberOfWorkers( String numberOfWorkers ) {
    this.numberOfWorkers = numberOfWorkers;
  }
}
