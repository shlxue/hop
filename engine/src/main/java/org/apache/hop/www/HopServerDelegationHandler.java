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

package org.apache.hop.www;

import org.apache.hop.job.IDelegationListener;
import org.apache.hop.job.Job;
import org.apache.hop.job.JobConfiguration;
import org.apache.hop.job.JobExecutionConfiguration;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineConfiguration;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;

/**
 * A handler for registering sub-jobs and sub-pipelines on the carte maps. The trick here is that listeners are
 * added recursively down as long as the listener methods are called.
 *
 * @author matt
 */
public class HopServerDelegationHandler implements IDelegationListener {

  protected PipelineMap pipelineMap;
  protected JobMap jobMap;

  public HopServerDelegationHandler( PipelineMap pipelineMap, JobMap jobMap ) {
    super();
    this.pipelineMap = pipelineMap;
    this.jobMap = jobMap;
  }

  @Override
  public synchronized void jobDelegationStarted( Job delegatedJob,
                                                 JobExecutionConfiguration jobExecutionConfiguration ) {

    JobConfiguration jc = new JobConfiguration( delegatedJob.getJobMeta(), jobExecutionConfiguration );
    jobMap.registerJob( delegatedJob, jc );

    delegatedJob.addDelegationListener( this );
  }

  @Override
  public synchronized void pipelineDelegationStarted( Pipeline delegatedPipeline,
                                                            PipelineExecutionConfiguration pipelineExecutionConfiguration ) {
    PipelineConfiguration tc = new PipelineConfiguration( delegatedPipeline.getPipelineMeta(), pipelineExecutionConfiguration );
    pipelineMap.registerPipeline( delegatedPipeline, tc );
    delegatedPipeline.addDelegationListener( this );

  }

}
