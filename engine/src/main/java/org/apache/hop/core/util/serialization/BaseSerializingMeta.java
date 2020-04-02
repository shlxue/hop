/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.core.util.serialization;

import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.w3c.dom.Node;

import static org.apache.hop.core.util.serialization.MetaXmlSerializer.deserialize;
import static org.apache.hop.core.util.serialization.MetaXmlSerializer.serialize;
import static org.apache.hop.core.util.serialization.TransformMetaProps.from;

/**
 * Handles serialization of meta by implementing getXML/loadXML, readRep/saveRep.
 * <p>
 * Uses {@link MetaXmlSerializer} for generically
 * handling child classes meta.
 */
public abstract class BaseSerializingMeta<Main extends ITransform, Data extends ITransformData>
  extends BaseTransformMeta implements ITransformMeta<Main, Data> {

  @Override public String getXML() {
    return serialize( from( this ) );
  }

  @Override public void loadXML(
    Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    deserialize( transformNode ).to( this );
  }

  /**
   * Creates a copy of this transformMeta with variables globally substituted.
   */
  public ITransformMeta withVariables( IVariables variables ) {
    return TransformMetaProps
      .from( this )
      .withVariables( variables )
      .to( (ITransformMeta) this.copyObject() );
  }

  /**
   * This is intended to act the way clone should (return a fully independent copy of the original).  This method
   * name was chosen in order to force any subclass that wants to use withVariables to implement a proper clone
   * override, but let others ignore it.
   *
   * @return a copy of this object
   */
  public BaseSerializingMeta copyObject() {
    throw new UnsupportedOperationException( "This method must be overridden if you use withVariables." );
  }
}
