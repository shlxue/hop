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

package org.apache.hop.job.entries.ftpput;

import com.enterprisedt.net.ftp.FTPClient;
import com.enterprisedt.net.ftp.FTPConnectMode;
import com.enterprisedt.net.ftp.FTPException;
import com.enterprisedt.net.ftp.FTPFileFactory;
import com.enterprisedt.net.ftp.FTPFileParser;
import com.enterprisedt.net.ftp.FTPTransferType;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.JobEntry;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entries.ftp.MVSFileParser;
import org.apache.hop.job.entry.IJobEntry;
import org.apache.hop.job.entry.JobEntryBase;
import org.apache.hop.job.entry.validator.AndValidator;
import org.apache.hop.job.entry.validator.JobEntryValidatorUtils;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This defines an FTP put job entry.
 *
 * @author Samatar
 * @since 15-09-2007
 */

@JobEntry(
  id = "FTP_PUT",
  i18nPackageName = "org.apache.hop.job.entries.ftpput",
  name = "JobEntryFTPPut.Name",
  description = "JobEntryFTPPut.Description",
  image = "FTPPut.svg",
  categoryDescription = "i18n:org.apache.hop.job:JobCategory.Category.FileTransfer"
)
public class JobEntryFTPPut extends JobEntryBase implements Cloneable, IJobEntry {
  private static Class<?> PKG = JobEntryFTPPut.class; // for i18n purposes, needed by Translator!!

  public static final int FTP_DEFAULT_PORT = 21;

  private String serverName;
  private String serverPort;
  private String userName;
  private String password;
  private String remoteDirectory;
  private String localDirectory;
  private String wildcard;
  private boolean binaryMode;
  private int timeout;
  private boolean remove;
  private boolean onlyPuttingNewFiles; /* Don't overwrite files */
  private boolean activeConnection;
  private String controlEncoding; /* how to convert list of filenames e.g. */
  private String proxyHost;

  private String proxyPort; /* string to allow variable substitution */

  private String proxyUsername;

  private String proxyPassword;

  private String socksProxyHost;
  private String socksProxyPort;
  private String socksProxyUsername;
  private String socksProxyPassword;

  /**
   * Implicit encoding used before PDI v2.4.1
   */
  private static final String LEGACY_CONTROL_ENCODING = "US-ASCII";

  /**
   * Default encoding when making a new ftp job entry instance.
   */
  private static final String DEFAULT_CONTROL_ENCODING = "ISO-8859-1";

  public JobEntryFTPPut( String n ) {
    super( n, "" );
    serverName = null;
    serverPort = "21";
    socksProxyPort = "1080";
    remoteDirectory = null;
    localDirectory = null;
    setControlEncoding( DEFAULT_CONTROL_ENCODING );
  }

  public JobEntryFTPPut() {
    this( "" );
  }

  public Object clone() {
    JobEntryFTPPut je = (JobEntryFTPPut) super.clone();
    return je;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 450 ); // 365 characters in spaces and tag names alone

    retval.append( super.getXML() );

    retval.append( "      " ).append( XMLHandler.addTagValue( "servername", serverName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "serverport", serverPort ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "username", userName ) );
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "password", Encr.encryptPasswordIfNotUsingVariables( getPassword() ) ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "remoteDirectory", remoteDirectory ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "localDirectory", localDirectory ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "wildcard", wildcard ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "binary", binaryMode ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "timeout", timeout ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "remove", remove ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "only_new", onlyPuttingNewFiles ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "active", activeConnection ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "control_encoding", controlEncoding ) );

    retval.append( "      " ).append( XMLHandler.addTagValue( "proxy_host", proxyHost ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "proxy_port", proxyPort ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "proxy_username", proxyUsername ) );
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "proxy_password", Encr.encryptPasswordIfNotUsingVariables( proxyPassword ) ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "socksproxy_host", socksProxyHost ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "socksproxy_port", socksProxyPort ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "socksproxy_username", socksProxyUsername ) );
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "socksproxy_password", Encr
        .encryptPasswordIfNotUsingVariables( socksProxyPassword ) ) );

    return retval.toString();
  }

  public void loadXML( Node entrynode,
                       IMetaStore metaStore ) throws HopXMLException {
    try {
      super.loadXML( entrynode );
      serverName = XMLHandler.getTagValue( entrynode, "servername" );
      serverPort = XMLHandler.getTagValue( entrynode, "serverport" );
      userName = XMLHandler.getTagValue( entrynode, "username" );
      password = Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( entrynode, "password" ) );
      remoteDirectory = XMLHandler.getTagValue( entrynode, "remoteDirectory" );
      localDirectory = XMLHandler.getTagValue( entrynode, "localDirectory" );
      wildcard = XMLHandler.getTagValue( entrynode, "wildcard" );
      binaryMode = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "binary" ) );
      timeout = Const.toInt( XMLHandler.getTagValue( entrynode, "timeout" ), 10000 );
      remove = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "remove" ) );
      onlyPuttingNewFiles = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "only_new" ) );
      activeConnection = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "active" ) );
      controlEncoding = XMLHandler.getTagValue( entrynode, "control_encoding" );

      proxyHost = XMLHandler.getTagValue( entrynode, "proxy_host" );
      proxyPort = XMLHandler.getTagValue( entrynode, "proxy_port" );
      proxyUsername = XMLHandler.getTagValue( entrynode, "proxy_username" );
      proxyPassword =
        Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( entrynode, "proxy_password" ) );
      socksProxyHost = XMLHandler.getTagValue( entrynode, "socksproxy_host" );
      socksProxyPort = XMLHandler.getTagValue( entrynode, "socksproxy_port" );
      socksProxyUsername = XMLHandler.getTagValue( entrynode, "socksproxy_username" );
      socksProxyPassword =
        Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( entrynode, "socksproxy_password" ) );

      if ( controlEncoding == null ) {
        // if we couldn't retrieve an encoding, assume it's an old instance and
        // put in the the encoding used before v 2.4.0
        controlEncoding = LEGACY_CONTROL_ENCODING;
      }
    } catch ( HopXMLException xe ) {
      throw new HopXMLException( BaseMessages.getString( PKG, "JobFTPPUT.Log.UnableToLoadFromXml" ), xe );
    }
  }

  /**
   * @return Returns the binaryMode.
   */
  public boolean isBinaryMode() {
    return binaryMode;
  }

  /**
   * @param binaryMode The binaryMode to set.
   */
  public void setBinaryMode( boolean binaryMode ) {
    this.binaryMode = binaryMode;
  }

  /**
   * @param timeout The timeout to set.
   */
  public void setTimeout( int timeout ) {
    this.timeout = timeout;
  }

  /**
   * @return Returns the timeout.
   */
  public int getTimeout() {
    return timeout;
  }

  /**
   * @return Returns the onlyGettingNewFiles.
   */
  public boolean isOnlyPuttingNewFiles() {
    return onlyPuttingNewFiles;
  }

  /**
   * @param onlyPuttingNewFiles Only transfer new files to the remote host
   */
  public void setOnlyPuttingNewFiles( boolean onlyPuttingNewFiles ) {
    this.onlyPuttingNewFiles = onlyPuttingNewFiles;
  }

  /**
   * Get the control encoding to be used for ftp'ing
   *
   * @return the used encoding
   */
  public String getControlEncoding() {
    return controlEncoding;
  }

  /**
   * Set the encoding to be used for ftp'ing. This determines how names are translated in dir e.g. It does impact the
   * contents of the files being ftp'ed.
   *
   * @param encoding The encoding to be used.
   */
  public void setControlEncoding( String encoding ) {
    this.controlEncoding = encoding;
  }

  /**
   * @return Returns the remoteDirectory.
   */
  public String getRemoteDirectory() {
    return remoteDirectory;
  }

  /**
   * @param directory The remoteDirectory to set.
   */
  public void setRemoteDirectory( String directory ) {
    this.remoteDirectory = directory;
  }

  /**
   * @return Returns the password.
   */
  public String getPassword() {
    return password;
  }

  /**
   * @param password The password to set.
   */
  public void setPassword( String password ) {
    this.password = password;
  }

  /**
   * @return Returns the serverName.
   */
  public String getServerName() {
    return serverName;
  }

  /**
   * @param serverName The serverName to set.
   */
  public void setServerName( String serverName ) {
    this.serverName = serverName;
  }

  /**
   * @return Returns the userName.
   */
  public String getUserName() {
    return userName;
  }

  /**
   * @param userName The userName to set.
   */
  public void setUserName( String userName ) {
    this.userName = userName;
  }

  /**
   * @return Returns the wildcard.
   */
  public String getWildcard() {
    return wildcard;
  }

  /**
   * @param wildcard The wildcard to set.
   */
  public void setWildcard( String wildcard ) {
    this.wildcard = wildcard;
  }

  /**
   * @return Returns the localDirectory.
   */
  public String getLocalDirectory() {
    return localDirectory;
  }

  /**
   * @param directory The localDirectory to set.
   */
  public void setLocalDirectory( String directory ) {
    this.localDirectory = directory;
  }

  /**
   * @param remove The remove to set.
   */
  public void setRemove( boolean remove ) {
    this.remove = remove;
  }

  /**
   * @return Returns the remove.
   */
  public boolean getRemove() {
    return remove;
  }

  public String getServerPort() {
    return serverPort;
  }

  public void setServerPort( String serverPort ) {
    this.serverPort = serverPort;
  }

  /**
   * @return the activeConnection
   */
  public boolean isActiveConnection() {
    return activeConnection;
  }

  /**
   * @param activeConnection set to true to get an active FTP connection
   */
  public void setActiveConnection( boolean activeConnection ) {
    this.activeConnection = activeConnection;
  }

  /**
   * @return Returns the hostname of the ftp-proxy.
   */
  public String getProxyHost() {
    return proxyHost;
  }

  /**
   * @param proxyHost The hostname of the proxy.
   */
  public void setProxyHost( String proxyHost ) {
    this.proxyHost = proxyHost;
  }

  /**
   * @return Returns the password which is used to authenticate at the proxy.
   */
  public String getProxyPassword() {
    return proxyPassword;
  }

  /**
   * @param proxyPassword The password which is used to authenticate at the proxy.
   */
  public void setProxyPassword( String proxyPassword ) {
    this.proxyPassword = proxyPassword;
  }

  /**
   * @return Returns the port of the ftp-proxy.
   */
  public String getProxyPort() {
    return proxyPort;
  }

  /**
   * @param proxyPort The port of the ftp-proxy.
   */
  public void setProxyPort( String proxyPort ) {
    this.proxyPort = proxyPort;
  }

  /**
   * @return Returns the username which is used to authenticate at the proxy.
   */
  public String getProxyUsername() {
    return proxyUsername;
  }

  /**
   * @param socksProxyHost The socks proxy host to set
   */
  public void setSocksProxyHost( String socksProxyHost ) {
    this.socksProxyHost = socksProxyHost;
  }

  /**
   * @param socksProxyPort The socks proxy port to set
   */
  public void setSocksProxyPort( String socksProxyPort ) {
    this.socksProxyPort = socksProxyPort;
  }

  /**
   * @param socksProxyUsername The socks proxy username to set
   */
  public void setSocksProxyUsername( String socksProxyUsername ) {
    this.socksProxyUsername = socksProxyUsername;
  }

  /**
   * @param socksProxyPassword The socks proxy password to set
   */
  public void setSocksProxyPassword( String socksProxyPassword ) {
    this.socksProxyPassword = socksProxyPassword;
  }

  /**
   * @return The sox proxy host name
   */
  public String getSocksProxyHost() {
    return this.socksProxyHost;
  }

  /**
   * @return The socks proxy port
   */
  public String getSocksProxyPort() {
    return this.socksProxyPort;
  }

  /**
   * @return The socks proxy username
   */
  public String getSocksProxyUsername() {
    return this.socksProxyUsername;
  }

  /**
   * @return The socks proxy password
   */
  public String getSocksProxyPassword() {
    return this.socksProxyPassword;
  }

  /**
   * @param proxyUsername The username which is used to authenticate at the proxy.
   */
  public void setProxyUsername( String proxyUsername ) {
    this.proxyUsername = proxyUsername;
  }

  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    result.setResult( false );
    long filesput = 0;

    if ( log.isDetailed() ) {
      logDetailed( BaseMessages.getString( PKG, "JobFTPPUT.Log.Starting" ) );
    }

    FTPClient ftpclient = null;
    try {
      // Create ftp client to host:port ...
      ftpclient = createAndSetUpFtpClient();

      // login to ftp host ...
      String realUsername = environmentSubstitute( userName );
      String realPassword = Encr.decryptPasswordOptionallyEncrypted( environmentSubstitute( password ) );
      ftpclient.connect();
      ftpclient.login( realUsername, realPassword );

      // set BINARY
      if ( binaryMode ) {
        ftpclient.setType( FTPTransferType.BINARY );
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "JobFTPPUT.Log.BinaryMode" ) );
        }
      }

      // Remove password from logging, you don't know where it ends up.
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "JobFTPPUT.Log.Logged", realUsername ) );
      }

      // Fix for PDI-2534 - add auxilliary FTP File List parsers to the ftpclient object.
      this.hookInOtherParsers( ftpclient );

      // move to spool dir ...
      String realRemoteDirectory = environmentSubstitute( remoteDirectory );
      if ( !Utils.isEmpty( realRemoteDirectory ) ) {
        ftpclient.chdir( realRemoteDirectory );
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "JobFTPPUT.Log.ChangedDirectory", realRemoteDirectory ) );
        }
      }

      String realLocalDirectory = environmentSubstitute( localDirectory );
      if ( realLocalDirectory == null ) {
        throw new FTPException( BaseMessages.getString( PKG, "JobFTPPUT.LocalDir.NotSpecified" ) );
      } else {
        // handle file:/// prefix
        if ( realLocalDirectory.startsWith( "file:" ) ) {
          realLocalDirectory = new URI( realLocalDirectory ).getPath();
        }
      }

      final List<String> files;
      File localFiles = new File( realLocalDirectory );
      File[] children = localFiles.listFiles();
      if ( children == null ) {
        files = Collections.emptyList();
      } else {
        files = new ArrayList<>( children.length );
        for ( File child : children ) {
          // Get filename of file or directory
          if ( !child.isDirectory() ) {
            files.add( child.getName() );
          }
        }
      }
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString(
          PKG, "JobFTPPUT.Log.FoundFileLocalDirectory", "" + files.size(), realLocalDirectory ) );
      }

      String realWildcard = environmentSubstitute( wildcard );
      Pattern pattern;
      if ( !Utils.isEmpty( realWildcard ) ) {
        pattern = Pattern.compile( realWildcard );
      } else {
        pattern = null;
      }

      for ( String file : files ) {
        if ( parentJob.isStopped() ) {
          break;
        }

        boolean toBeProcessed = true;

        // First see if the file matches the regular expression!
        if ( pattern != null ) {
          Matcher matcher = pattern.matcher( file );
          toBeProcessed = matcher.matches();
        }

        if ( toBeProcessed ) {
          // File exists?
          boolean fileExist = false;
          try {
            fileExist = ftpclient.exists( file );
          } catch ( Exception e ) {
            // Assume file does not exist !!
          }

          if ( log.isDebug() ) {
            if ( fileExist ) {
              logDebug( BaseMessages.getString( PKG, "JobFTPPUT.Log.FileExists", file ) );
            } else {
              logDebug( BaseMessages.getString( PKG, "JobFTPPUT.Log.FileDoesNotExists", file ) );
            }
          }

          if ( !fileExist || !onlyPuttingNewFiles ) {
            if ( log.isDebug() ) {
              logDebug( BaseMessages.getString(
                PKG, "JobFTPPUT.Log.PuttingFileToRemoteDirectory", file, realRemoteDirectory ) );
            }

            String localFilename = realLocalDirectory + Const.FILE_SEPARATOR + file;
            ftpclient.put( localFilename, file );

            filesput++;

            // Delete the file if this is needed!
            if ( remove ) {
              new File( localFilename ).delete();
              if ( log.isDetailed() ) {
                logDetailed( BaseMessages.getString( PKG, "JobFTPPUT.Log.DeletedFile", localFilename ) );
              }
            }
          }
        }
      }

      result.setResult( true );
      if ( log.isDetailed() ) {
        logDebug( BaseMessages.getString( PKG, "JobFTPPUT.Log.WeHavePut", "" + filesput ) );
      }
    } catch ( Exception e ) {
      result.setNrErrors( 1 );
      logError( BaseMessages.getString( PKG, "JobFTPPUT.Log.ErrorPuttingFiles", e.getMessage() ) );
      logError( Const.getStackTracker( e ) );
    } finally {
      if ( ftpclient != null && ftpclient.connected() ) {
        try {
          ftpclient.quit();
        } catch ( Exception e ) {
          logError( BaseMessages.getString( PKG, "JobFTPPUT.Log.ErrorQuitingFTP", e.getMessage() ) );
        }
      }

      FTPClient.clearSOCKS();
    }

    return result;
  }

  // package-local visibility for testing purposes
  FTPClient createAndSetUpFtpClient() throws IOException, FTPException {
    String realServerName = environmentSubstitute( serverName );
    String realServerPort = environmentSubstitute( serverPort );

    FTPClient ftpClient = createFtpClient();
    ftpClient.setRemoteAddr( InetAddress.getByName( realServerName ) );
    if ( !Utils.isEmpty( realServerPort ) ) {
      ftpClient.setRemotePort( Const.toInt( realServerPort, FTP_DEFAULT_PORT ) );
    }

    if ( !Utils.isEmpty( proxyHost ) ) {
      String realProxyHost = environmentSubstitute( proxyHost );
      ftpClient.setRemoteAddr( InetAddress.getByName( realProxyHost ) );
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "JobEntryFTPPUT.OpenedProxyConnectionOn", realProxyHost ) );
      }

      // FIXME: Proper default port for proxy
      int port = Const.toInt( environmentSubstitute( proxyPort ), FTP_DEFAULT_PORT );
      if ( port != 0 ) {
        ftpClient.setRemotePort( port );
      }
    } else {
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "JobEntryFTPPUT.OpenConnection", realServerName ) );
      }
    }

    // set activeConnection connectmode ...
    if ( activeConnection ) {
      ftpClient.setConnectMode( FTPConnectMode.ACTIVE );
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "JobFTPPUT.Log.SetActiveConnection" ) );
      }
    } else {
      ftpClient.setConnectMode( FTPConnectMode.PASV );
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "JobFTPPUT.Log.SetPassiveConnection" ) );
      }
    }

    // Set the timeout
    if ( timeout > 0 ) {
      ftpClient.setTimeout( timeout );
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "JobFTPPUT.Log.SetTimeout", "" + timeout ) );
      }
    }

    ftpClient.setControlEncoding( controlEncoding );
    if ( log.isDetailed() ) {
      logDetailed( BaseMessages.getString( PKG, "JobFTPPUT.Log.SetEncoding", controlEncoding ) );
    }

    // If socks proxy server was provided
    if ( !Utils.isEmpty( socksProxyHost ) ) {
      // if a port was provided
      if ( !Utils.isEmpty( socksProxyPort ) ) {
        FTPClient.initSOCKS( environmentSubstitute( socksProxyPort ), environmentSubstitute( socksProxyHost ) );
      } else { // looks like we have a host and no port
        throw new FTPException( BaseMessages.getString(
          PKG, "JobFTPPUT.SocksProxy.PortMissingException", environmentSubstitute( socksProxyHost ) ) );
      }
      // now if we have authentication information
      if ( !Utils.isEmpty( socksProxyUsername )
        && Utils.isEmpty( socksProxyPassword ) || Utils.isEmpty( socksProxyUsername )
        && !Utils.isEmpty( socksProxyPassword ) ) {
        // we have a username without a password or vica versa
        throw new FTPException( BaseMessages.getString(
          PKG, "JobFTPPUT.SocksProxy.IncompleteCredentials", environmentSubstitute( socksProxyHost ),
          getName() ) );
      }
    }

    return ftpClient;
  }

  // package-local visibility for testing purposes
  FTPClient createFtpClient() {
    return new PDIFTPClient( log );
  }

  public boolean evaluates() {
    return true;
  }

  public List<ResourceReference> getResourceDependencies( JobMeta jobMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( jobMeta );
    if ( !Utils.isEmpty( serverName ) ) {
      String realServerName = jobMeta.environmentSubstitute( serverName );
      ResourceReference reference = new ResourceReference( this );
      reference.getEntries().add( new ResourceEntry( realServerName, ResourceType.SERVER ) );
      references.add( reference );
    }
    return references;
  }

  @Override
  public void check( List<ICheckResult> remarks, JobMeta jobMeta, IVariables variables,
                     IMetaStore metaStore ) {
    JobEntryValidatorUtils.andValidator().validate( this, "serverName", remarks,
      AndValidator.putValidators( JobEntryValidatorUtils.notBlankValidator() ) );
    JobEntryValidatorUtils.andValidator().validate(
      this, "localDirectory", remarks, AndValidator.putValidators(
        JobEntryValidatorUtils.notBlankValidator(), JobEntryValidatorUtils.fileExistsValidator() ) );
    JobEntryValidatorUtils.andValidator().validate( this, "userName", remarks,
      AndValidator.putValidators( JobEntryValidatorUtils.notBlankValidator() ) );
    JobEntryValidatorUtils.andValidator().validate( this, "password", remarks,
      AndValidator.putValidators( JobEntryValidatorUtils.notNullValidator() ) );
    JobEntryValidatorUtils.andValidator().validate( this, "serverPort", remarks,
      AndValidator.putValidators( JobEntryValidatorUtils.integerValidator() ) );
  }

  /**
   * Hook in known parsers, and then those that have been specified in the variable ftp.file.parser.class.names
   *
   * @param ftpClient
   * @throws FTPException
   * @throws IOException
   */
  protected void hookInOtherParsers( FTPClient ftpClient ) throws FTPException, IOException {
    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "JobEntryFTP.DEBUG.Hooking.Parsers" ) );
    }
    String system = ftpClient.system();
    MVSFileParser parser = new MVSFileParser( log );
    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "JobEntryFTP.DEBUG.Created.MVS.Parser" ) );
    }
    FTPFileFactory factory = new FTPFileFactory( system );
    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "JobEntryFTP.DEBUG.Created.Factory" ) );
    }
    factory.addParser( parser );
    ftpClient.setFTPFileFactory( factory );
    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "JobEntryFTP.DEBUG.Get.Variable.Space" ) );
    }
    IVariables vs = this.getVariables();
    if ( vs != null ) {
      if ( log.isDebug() ) {
        logDebug( BaseMessages.getString( PKG, "JobEntryFTP.DEBUG.Getting.Other.Parsers" ) );
      }
      String otherParserNames = vs.getVariable( "ftp.file.parser.class.names" );
      if ( otherParserNames != null ) {
        if ( log.isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "JobEntryFTP.DEBUG.Creating.Parsers" ) );
        }
        String[] parserClasses = otherParserNames.split( "|" );
        String cName = null;
        Class<?> clazz = null;
        Object parserInstance = null;
        for ( int i = 0; i < parserClasses.length; i++ ) {
          cName = parserClasses[ i ].trim();
          if ( cName.length() > 0 ) {
            try {
              clazz = Class.forName( cName );
              parserInstance = clazz.newInstance();
              if ( parserInstance instanceof FTPFileParser ) {
                if ( log.isDetailed() ) {
                  logDetailed( BaseMessages.getString( PKG, "JobEntryFTP.DEBUG.Created.Other.Parser", cName ) );
                }
                factory.addParser( (FTPFileParser) parserInstance );
              }
            } catch ( Exception ignored ) {
              if ( log.isDebug() ) {
                ignored.printStackTrace();
                logError( BaseMessages.getString( PKG, "JobEntryFTP.ERROR.Creating.Parser", cName ) );
              }
            }
          }
        }
      }
    }
  }
}
