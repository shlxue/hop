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

package org.apache.hop.workflow.actions.dostounix;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.AllFileSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/** This defines a 'Dos to Unix' action. */
@Action(
    id = "DOS_UNIX_CONVERTER",
    name = "i18n::ActionDosToUnix.Name",
    description = "i18n::ActionDosToUnix.Description",
    image = "DosToUnix.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
    keywords = "i18n::ActionDosToUnix.keyword",
    documentationUrl = "/workflow/actions/dostounix.html")
@SuppressWarnings("java:S1104")
@Getter
@Setter
public class ActionDosToUnix extends ActionBase {
  private static final int LF = 0x0a;
  private static final int CR = 0x0d;

  private static final Class<?> PKG = ActionDosToUnix.class;
  private static final String CONST_SUCCESS_CONDITION_BROKEN =
      "ActionDosToUnix.Error.SuccessConditionbroken";

  public static final String[] ConversionTypeDesc =
      new String[] {
        BaseMessages.getString(PKG, "ActionDosToUnix.ConversionType.Guess.Label"),
        BaseMessages.getString(PKG, "ActionDosToUnix.ConversionType.DosToUnix.Label"),
        BaseMessages.getString(PKG, "ActionDosToUnix.ConversionType.UnixToDos.Label")
      };
  public static final String[] ConversionTypeCode =
      new String[] {"guess", "dostounix", "unixtodos"};

  public static final int CONVERTION_TYPE_GUESS = 0;
  public static final int CONVERTION_TYPE_DOS_TO_UNIX = 1;
  public static final int CONVERTION_TYPE_UNIX_TO_DOS = 2;

  private static final int TYPE_DOS_FILE = 0;
  private static final int TYPE_UNIX_FILE = 1;
  private static final int TYPE_BINAY_FILE = 2;

  public static final String ADD_NOTHING = "nothing";
  public static final String SUCCESS_IF_AT_LEAST_X_FILES_PROCESSED = "success_when_at_least";
  public static final String SUCCESS_IF_ERROR_FILES_LESS = "success_if_error_files_less";
  public static final String SUCCESS_IF_NO_ERRORS = "success_if_no_errors";

  public static final String ADD_ALL_FILENAMES = "all_filenames";
  public static final String ADD_PROCESSED_FILES_ONLY = "only_processed_filenames";
  public static final String ADD_ERROR_FILES_ONLY = "only_error_filenames";

  @HopMetadataProperty(key = "arg_from_previous")
  public boolean argFromPrevious;

  @HopMetadataProperty(key = "include_subfolders")
  public boolean includeSubFolders;

  @HopMetadataProperty(key = "nr_errors_less_than")
  private String nrErrorsLessThan;

  @HopMetadataProperty(key = "success_condition")
  private String successCondition;

  @HopMetadataProperty(key = "resultfilenames")
  private String resultFilenames;

  @HopMetadataProperty(key = "field", groupKey = "fields")
  private List<Fields> fields;

  int nrAllErrors = 0;
  int nrErrorFiles = 0;
  int nrProcessedFiles = 0;
  int limitFiles = 0;
  int nrErrors = 0;

  boolean successConditionBroken = false;
  boolean successConditionBrokenExit = false;

  private static String tempFolder;

  public ActionDosToUnix(String n) {
    super(n, "");
    resultFilenames = ADD_ALL_FILENAMES;
    argFromPrevious = false;
    fields = new ArrayList<>();
    includeSubFolders = false;
    nrErrorsLessThan = "10";
    successCondition = SUCCESS_IF_NO_ERRORS;
  }

  public ActionDosToUnix() {
    this("");
  }

  public static String getConversionTypeCode(int i) {
    if (i < 0 || i >= ConversionTypeCode.length) {
      return ConversionTypeCode[0];
    }
    return ConversionTypeCode[i];
  }

  public static String getConversionTypeDesc(int i) {
    if (i < 0 || i >= ConversionTypeDesc.length) {
      return ConversionTypeDesc[0];
    }
    return ConversionTypeDesc[i];
  }

  public static int getConversionTypeByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < ConversionTypeDesc.length; i++) {
      if (ConversionTypeDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getConversionTypeByCode(tt);
  }

  private static int getConversionTypeByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < ConversionTypeCode.length; i++) {
      if (ConversionTypeCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  @Override
  public Result execute(Result previousResult, int nr) throws HopException {
    Result result = previousResult;
    result.setNrErrors(1);
    result.setResult(false);

    List<RowMetaAndData> rows = previousResult.getRows();
    RowMetaAndData resultRow = null;

    nrErrors = 0;
    nrProcessedFiles = 0;
    nrErrorFiles = 0;
    limitFiles = Const.toInt(resolve(getNrErrorsLessThan()), 10);
    successConditionBroken = false;
    successConditionBrokenExit = false;
    tempFolder = resolve("%%java.io.tmpdir%%");

    // Get source and destination files, also wildcard
    String[] vSourceFileFolder = new String[fields.size()];
    String[] vWildcard = new String[fields.size()];
    Integer[] vConversionTypes = new Integer[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      vSourceFileFolder[i] = fields.get(i).sourceFileFolder;
      vWildcard[i] = fields.get(i).wildcard;
      vConversionTypes[i] = getConversionTypeByCode(fields.get(i).conversionTypes);
    }

    if (argFromPrevious && isDetailed()) {
      logDetailed(
          BaseMessages.getString(
              PKG,
              "JobDosToUnix.Log.ArgFromPrevious.Found",
              (rows != null ? rows.size() : 0) + ""));
    }
    if (argFromPrevious && rows != null) {
      // Copy the input row to the (command line) arguments
      for (int iteration = 0; iteration < rows.size() && !parentWorkflow.isStopped(); iteration++) {
        if (successConditionBroken) {
          if (!successConditionBrokenExit) {
            logError(BaseMessages.getString(PKG, CONST_SUCCESS_CONDITION_BROKEN, "" + nrAllErrors));
            successConditionBrokenExit = true;
          }
          result.setEntryNr(nrAllErrors);
          result.setNrLinesRejected(nrErrorFiles);
          result.setNrLinesWritten(nrProcessedFiles);
          return result;
        }

        resultRow = rows.get(iteration);

        // Get source and destination file names, also wildcard
        String vSourceFileFolderPrevious = resultRow.getString(0, null);
        String vWildcardPrevious = resultRow.getString(1, null);
        int conversionType = ActionDosToUnix.getConversionTypeByCode(resultRow.getString(2, null));

        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  "JobDosToUnix.Log.ProcessingRow",
                  vSourceFileFolderPrevious,
                  vWildcardPrevious));
        }

        processFileFolder(
            vSourceFileFolderPrevious, vWildcardPrevious, conversionType, parentWorkflow, result);
      }
    } else if (vSourceFileFolder != null) {
      for (int i = 0; i < vSourceFileFolder.length && !parentWorkflow.isStopped(); i++) {
        if (successConditionBroken) {
          if (!successConditionBrokenExit) {
            logError(BaseMessages.getString(PKG, CONST_SUCCESS_CONDITION_BROKEN, "" + nrAllErrors));
            successConditionBrokenExit = true;
          }
          result.setEntryNr(nrAllErrors);
          result.setNrLinesRejected(nrErrorFiles);
          result.setNrLinesWritten(nrProcessedFiles);
          return result;
        }

        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionDosToUnix.Log.ProcessingRow", vSourceFileFolder[i], vWildcard[i]));
        }

        processFileFolder(
            vSourceFileFolder[i], vWildcard[i], vConversionTypes[i], parentWorkflow, result);
      }
    }

    // Success Condition
    result.setNrErrors(nrAllErrors);
    result.setNrLinesRejected(nrErrorFiles);
    result.setNrLinesWritten(nrProcessedFiles);
    if (getSuccessStatus()) {
      result.setNrErrors(0);
      result.setResult(true);
    }

    displayResults();

    return result;
  }

  private void displayResults() {
    if (isDetailed()) {
      logDetailed("=======================================");
      logDetailed(BaseMessages.getString(PKG, "ActionDosToUnix.Log.Info.Errors", nrErrors));
      logDetailed(BaseMessages.getString(PKG, "ActionDosToUnix.Log.Info.ErrorFiles", nrErrorFiles));
      logDetailed(
          BaseMessages.getString(PKG, "ActionDosToUnix.Log.Info.FilesProcessed", nrProcessedFiles));
      logDetailed("=======================================");
    }
  }

  private boolean checkIfSuccessConditionBroken() {
    return (nrAllErrors > 0 && getSuccessCondition().equals(SUCCESS_IF_NO_ERRORS))
        || (nrErrorFiles >= limitFiles
            && getSuccessCondition().equals(SUCCESS_IF_ERROR_FILES_LESS));
  }

  private boolean getSuccessStatus() {
    return (nrAllErrors == 0 && getSuccessCondition().equals(SUCCESS_IF_NO_ERRORS))
        || (nrProcessedFiles >= limitFiles
            && getSuccessCondition().equals(SUCCESS_IF_AT_LEAST_X_FILES_PROCESSED))
        || (nrErrorFiles < limitFiles && getSuccessCondition().equals(SUCCESS_IF_ERROR_FILES_LESS));
  }

  private void updateErrors() {
    nrErrors++;
    updateAllErrors();
    if (checkIfSuccessConditionBroken()) {
      // Success condition was broken
      successConditionBroken = true;
    }
  }

  private void updateAllErrors() {
    nrAllErrors = nrErrors + nrErrorFiles;
  }

  private static int getFileType(FileObject file) throws Exception {
    int aCount = 0; // occurences of LF
    int dCount = 0; // occurences of CR

    try (FileInputStream in = new FileInputStream(file.getName().getPathDecoded())) {
      while (in.available() > 0) {
        int b = in.read();
        if (b == CR) {
          dCount++;
          if (in.available() > 0) {
            b = in.read();
            if (b == LF) {
              aCount++;
            } else {
              return TYPE_BINAY_FILE;
            }
          }
        } else if (b == LF) {
          aCount++;
        }
      }
    }

    if (aCount == dCount) {
      return TYPE_DOS_FILE;
    } else {
      return TYPE_UNIX_FILE;
    }
  }

  @VisibleForTesting
  boolean convert(FileObject file, boolean toUnix) {
    boolean retval = false;
    // CR = CR
    // LF = LF
    try {
      String localfilename = HopVfs.getFilename(file);
      File source = new File(localfilename);
      if (isDetailed()) {
        if (toUnix) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionDosToUnix.Log.ConvertingFileToUnix", source.getAbsolutePath()));
        } else {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionDosToUnix.Log.ConvertingFileToDos", source.getAbsolutePath()));
        }
      }
      File tempFile = new File(tempFolder, source.getName() + ".tmp");

      if (isDebug()) {
        logDebug(
            BaseMessages.getString(
                PKG, "ActionDosToUnix.Log.CreatingTempFile", tempFile.getAbsolutePath()));
      }

      final int FOUR_KB = 4 * 1024;
      byte[] buffer = new byte[FOUR_KB];
      try (FileOutputStream out = new FileOutputStream(tempFile);
          FileInputStream in = new FileInputStream(localfilename)) {

        ConversionAutomata automata = new ConversionAutomata(out, toUnix);
        int read;
        while ((read = in.read(buffer)) > 0) {
          automata.convert(buffer, read);
        }
      }

      if (isDebug()) {
        logDebug(
            BaseMessages.getString(PKG, "ActionDosToUnix.Log.DeletingSourceFile", localfilename));
      }
      if (isDebug()) {
        logDebug(
            BaseMessages.getString(
                PKG,
                "JobDosToUnix.Log.RenamingTempFile",
                tempFile.getAbsolutePath(),
                source.getAbsolutePath()));
      }
      Files.move(tempFile.toPath(), source.toPath(), StandardCopyOption.REPLACE_EXISTING);
      retval = true;

    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG, "ActionDosToUnix.Log.ErrorConvertingFile", file.toString(), e.getMessage()));
    }

    return retval;
  }

  private boolean processFileFolder(
      String sourcefilefoldername,
      String wildcard,
      int convertion,
      IWorkflowEngine<WorkflowMeta> parentWorkflow,
      Result result) {
    boolean entrystatus = false;
    FileObject sourcefilefolder = null;
    FileObject currentFile = null;

    // Get real source file and wilcard
    String realSourceFilefoldername = resolve(sourcefilefoldername);
    if (Utils.isEmpty(realSourceFilefoldername)) {
      logError(
          BaseMessages.getString(PKG, "ActionDosToUnix.log.FileFolderEmpty", sourcefilefoldername));
      // Update Errors
      updateErrors();

      return entrystatus;
    }
    String realWildcard = resolve(wildcard);

    try {
      sourcefilefolder = HopVfs.getFileObject(realSourceFilefoldername);

      if (sourcefilefolder.exists()) {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionDosToUnix.Log.FileExists", sourcefilefolder.toString()));
        }
        if (sourcefilefolder.getType() == FileType.FILE) {
          entrystatus = convertOneFile(sourcefilefolder, convertion, result, parentWorkflow);

        } else if (sourcefilefolder.getType() == FileType.FOLDER) {
          FileObject[] fileObjects =
              sourcefilefolder.findFiles(
                  new AllFileSelector() {
                    @Override
                    public boolean traverseDescendents(FileSelectInfo info) {
                      return info.getDepth() == 0 || includeSubFolders;
                    }

                    @Override
                    public boolean includeFile(FileSelectInfo info) {

                      FileObject fileObject = info.getFile();
                      try {
                        if (fileObject == null) {
                          return false;
                        }
                        if (fileObject.getType() != FileType.FILE) {
                          return false;
                        }
                      } catch (Exception ex) {
                        // Upon error don't process the file.
                        return false;
                      } finally {
                        if (fileObject != null) {
                          try {
                            fileObject.close();
                          } catch (IOException ex) {
                            /* Ignore */
                          }
                        }
                      }
                      return true;
                    }
                  });

          if (fileObjects != null) {
            for (int j = 0; j < fileObjects.length && !parentWorkflow.isStopped(); j++) {
              if (successConditionBroken) {
                if (!successConditionBrokenExit) {
                  logError(
                      BaseMessages.getString(
                          PKG, CONST_SUCCESS_CONDITION_BROKEN, "" + nrAllErrors));
                  successConditionBrokenExit = true;
                }
                return false;
              }
              // Fetch files in list one after one ...
              currentFile = fileObjects[j];

              if (!currentFile.getParent().toString().equals(sourcefilefolder.toString())) {
                // Not in the Base Folder..Only if include sub folders
                if (includeSubFolders && getFileWildcard(currentFile.toString(), realWildcard)) {
                  convertOneFile(currentFile, convertion, result, parentWorkflow);
                }

              } else {
                // In the base folder
                if (getFileWildcard(currentFile.toString(), realWildcard)) {
                  convertOneFile(currentFile, convertion, result, parentWorkflow);
                }
              }
            }
          }
        } else {
          logError(
              BaseMessages.getString(
                  PKG, "ActionDosToUnix.Error.UnknowFileFormat", sourcefilefolder.toString()));
          // Update Errors
          updateErrors();
        }
      } else {
        logError(
            BaseMessages.getString(
                PKG, "ActionDosToUnix.Error.SourceFileNotExists", realSourceFilefoldername));
        // Update Errors
        updateErrors();
      }
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG,
              "JobDosToUnix.Error.Exception.Processing",
              realSourceFilefoldername,
              e.getMessage()));
      // Update Errors
      updateErrors();
    } finally {
      if (sourcefilefolder != null) {
        try {
          sourcefilefolder.close();
        } catch (IOException ex) {
          /* Ignore */
        }
      }
      if (currentFile != null) {
        try {
          currentFile.close();
        } catch (IOException ex) {
          /* Ignore */
        }
      }
    }
    return entrystatus;
  }

  private boolean convertOneFile(
      FileObject file, int convertion, Result result, IWorkflowEngine<WorkflowMeta> parentWorkflow)
      throws HopException {
    boolean retval = false;
    try {
      // We deal with a file..

      boolean convertToUnix = true;

      if (convertion == CONVERTION_TYPE_GUESS) {
        // Get file Type
        int fileType = getFileType(file);
        // File type is DOS
        // We need to convert it to UNIX
        // File type is not DOS
        // so let's convert it to DOS
        convertToUnix = fileType == TYPE_DOS_FILE;
      } else convertToUnix = convertion == CONVERTION_TYPE_DOS_TO_UNIX;

      retval = convert(file, convertToUnix);

      if (!retval) {
        logError(
            BaseMessages.getString(PKG, "ActionDosToUnix.Error.FileNotConverted", file.toString()));
        // Update Bad files number
        updateBadFormed();
        if (resultFilenames.equals(ADD_ALL_FILENAMES)
            || resultFilenames.equals(ADD_ERROR_FILES_ONLY)) {
          addFileToResultFilenames(file, result, parentWorkflow);
        }
      } else {
        if (isDetailed()) {
          logDetailed("---------------------------");
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  "ActionDosToUnix.Error.FileConverted",
                  file,
                  convertToUnix ? "UNIX" : "DOS"));
        }
        // Update processed files number
        updateProcessedFormed();
        if (resultFilenames.equals(ADD_ALL_FILENAMES)
            || resultFilenames.equals(ADD_PROCESSED_FILES_ONLY)) {
          addFileToResultFilenames(file, result, parentWorkflow);
        }
      }

    } catch (Exception e) {
      throw new HopException("Unable to convert file '" + file.toString() + "'", e);
    }
    return retval;
  }

  private void updateProcessedFormed() {
    nrProcessedFiles++;
  }

  private void updateBadFormed() {
    nrErrorFiles++;
    updateAllErrors();
  }

  private void addFileToResultFilenames(
      FileObject fileaddentry, Result result, IWorkflowEngine<WorkflowMeta> parentWorkflow) {
    try {
      ResultFile resultFile =
          new ResultFile(
              ResultFile.FILE_TYPE_GENERAL,
              fileaddentry,
              parentWorkflow.getWorkflowName(),
              toString());
      result.getResultFiles().put(resultFile.getFile().toString(), resultFile);

      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG, "ActionDosToUnix.Log.FileAddedToResultFilesName", fileaddentry));
      }

    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG,
              "JobDosToUnix.Error.AddingToFilenameResult",
              fileaddentry.toString(),
              e.getMessage()));
    }
  }

  /**********************************************************
   *
   * @param selectedfile
   * @param wildcard
   * @return True if the selectedfile matches the wildcard
   **********************************************************/
  private boolean getFileWildcard(String selectedfile, String wildcard) {
    Pattern pattern = null;
    boolean getIt = true;

    if (!Utils.isEmpty(wildcard)) {
      pattern = Pattern.compile(wildcard);
      // First see if the file matches the regular expression!
      if (pattern != null) {
        Matcher matcher = pattern.matcher(selectedfile);
        getIt = matcher.matches();
      }
    }

    return getIt;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  private static class ConversionAutomata {

    private final OutputStream os;
    private final boolean toUnix;
    private byte state;

    ConversionAutomata(OutputStream os, boolean toUnix) {
      this.os = os;
      this.toUnix = toUnix;
      this.state = 0;
    }

    void convert(byte[] input, int amount) throws IOException {
      if (toUnix) {
        toUnix(input, amount);
      } else {
        toDos(input, amount);
      }
    }

    private void toUnix(byte[] input, int amount) throws IOException {
      // [0]:
      //     read CR -> goto [1];
      //     read __ -> write __;
      // [1]:
      //     read LF -> write LF;           goto [0];
      //     read CR -> write CR;                    // two CRs in a row -- write the first and hold
      // the second
      //     read __ -> write CR; write __; goto [0];

      int index = 0;
      while (index < amount) {
        int b = input[index++];
        switch (state) {
          case 0:
            if (b == CR) {
              state = 1;
            } else {
              os.write(b);
            }
            break;
          case 1:
            if (b == LF) {
              os.write(LF);
              state = 0;
            } else {
              os.write(CR);
              if (b != CR) {
                os.write(b);
                state = 0;
              }
            }
            break;
          default:
            throw unknownStateException();
        }
      }
    }

    private void toDos(byte[] input, int amount) throws IOException {
      // [0]:
      //     read CR -> goto [1];
      //     read LF -> write CR; write LF;
      //     read __ -> write __;
      // [1]:
      //     read LF -> write CR; write LF; goto [0]; // read CR,LF -> write them
      //     read CR -> write CR;
      //     read __ -> write CR; write __; goto [0];

      int index = 0;
      while (index < amount) {
        int b = input[index++];
        switch (state) {
          case 0:
            if (b == CR) {
              state = 1;
            } else if (b == LF) {
              os.write(CR);
              os.write(LF);
            } else {
              os.write(b);
            }
            break;
          case 1:
            os.write(CR);
            if (b != CR) {
              os.write(b);
              state = 0;
            }
            break;
          default:
            throw unknownStateException();
        }
      }
    }

    private IllegalStateException unknownStateException() {
      return new IllegalStateException("Unknown state: " + state);
    }
  }

  @Getter
  @Setter
  public static final class Fields {

    @HopMetadataProperty(key = "source_filefolder")
    private String sourceFileFolder;

    @HopMetadataProperty(key = "wildcard")
    private String wildcard;

    @HopMetadataProperty(key = "ConversionType")
    private String conversionTypes;
  }
}
