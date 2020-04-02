package org.apache.hop.ui.hopgui.file.empty;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.IHopFileType;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class EmptyFileType implements IHopFileType {
  @Override public String getName() {
    return null;
  }

  @Override public String[] getFilterExtensions() {
    return new String[ 0 ];
  }

  @Override public String[] getFilterNames() {
    return new String[ 0 ];
  }

  @Override public Properties getCapabilities() {
    return new Properties();
  }

  @Override public boolean hasCapability( String capability ) {
    return false;
  }

  @Override public IHopFileTypeHandler openFile( HopGui hopGui, String filename, IVariables parentVariableSpace ) throws HopException {
    return new EmptyHopFileTypeHandler();
  }

  @Override public IHopFileTypeHandler newFile( HopGui hopGui, IVariables parentVariableSpace ) throws HopException {
    return new EmptyHopFileTypeHandler();
  }

  @Override public boolean isHandledBy( String filename, boolean checkContent ) throws HopException {
    return false;
  }

  @Override public boolean supportsFile( IHasFilename metaObject ) {
    return false;
  }

  @Override public List<IGuiContextHandler> getContextHandlers() {
    List<IGuiContextHandler> handlers = new ArrayList<>();
    return handlers;
  }
}
