/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.mail.workflow.actions.mailvalidator;

import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;

@Action(
    id = "MAIL_VALIDATOR",
    name = "i18n::ActionMailValidator.Name",
    description = "i18n::ActionMailValidator.Description",
    image = "MailValidator.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Mail",
    documentationUrl = "/workflow/actions/mailvalidator.html")
public class ActionMailValidator extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionMailValidator.class;

  @HopMetadataProperty private boolean smtpCheck;
  @HopMetadataProperty private String timeout;
  @HopMetadataProperty private String defaultSMTP;
  @HopMetadataProperty private String emailSender;
  @HopMetadataProperty private String emailAddress;

  public ActionMailValidator(String n, String scr) {
    super(n, "");
    emailAddress = null;
    smtpCheck = false;
    timeout = "0";
    defaultSMTP = null;
    emailSender = "noreply@domain.com";
  }

  public ActionMailValidator() {
    this("", "");
  }

  public void setSmtpCheck(boolean smtpcheck) {
    this.smtpCheck = smtpcheck;
  }

  public boolean isSmtpCheck() {
    return smtpCheck;
  }

  public String getEmailAddress() {
    return this.emailAddress;
  }

  public void setEmailAddress(String emailAddress) {
    this.emailAddress = emailAddress;
  }

  /**
   * @return Returns the timeout.
   */
  public String getTimeout() {
    return timeout;
  }

  /**
   * @param timeout The timeout to set.
   */
  public void setTimeout(String timeout) {
    this.timeout = timeout;
  }

  /**
   * @return Returns the defaultSMTP.
   */
  public String getDefaultSMTP() {
    return defaultSMTP;
  }

  /**
   * @param defaultSMTP The defaultSMTP to set.
   */
  public void setDefaultSMTP(String defaultSMTP) {
    this.defaultSMTP = defaultSMTP;
  }

  /**
   * @return Returns the emailSender.
   */
  public String getEmailSender() {
    return emailSender;
  }

  /**
   * @param emailSender The emailSender to set.
   */
  public void setEmailSender(String emailSender) {
    this.emailSender = emailSender;
  }

  /**
   * Execute this action and return the result. In this case it means, just set the result boolean
   * in the Result class.
   *
   * @param previousResult The result of the previous execution
   * @return The Result of the execution.
   */
  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setNrErrors(1);
    result.setResult(false);

    String realEmailAddress = resolve(emailAddress);
    if (Utils.isEmpty(realEmailAddress)) {
      logError(BaseMessages.getString(PKG, "ActionMailValidator.Error.EmailEmpty"));
      return result;
    }
    String realSender = resolve(emailSender);
    if (smtpCheck && Utils.isEmpty(realSender)) {
      // check sender
      logError(BaseMessages.getString(PKG, "ActionMailValidator.Error.EmailSenderEmpty"));
      return result;
    }

    String realDefaultSMTP = resolve(defaultSMTP);
    int timeOut = Const.toInt(resolve(timeout), 0);

    // Split the mail-address: separated by variables
    String[] mailsCheck = realEmailAddress.split(" ");
    boolean exitloop = false;
    boolean mailIsValid = false;
    String mailError = null;
    for (int i = 0; i < mailsCheck.length && !exitloop; i++) {
      String email = mailsCheck[i];
      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "ActionMailValidator.CheckingMail", email));
      }

      // Check if address is valid
      MailValidationResult resultValidator =
          MailValidation.isAddressValid(
              getLogChannel(), email, realSender, realDefaultSMTP, timeOut, smtpCheck);

      mailIsValid = resultValidator.isValide();
      mailError = resultValidator.getErrorMessage();

      if (isDetailed()) {
        if (mailIsValid) {
          logDetailed(BaseMessages.getString(PKG, "ActionMailValidator.MailValid", email));
        } else {
          logDetailed(BaseMessages.getString(PKG, "ActionMailValidator.MailNotValid", email));
          logDetailed(mailError);
        }
      }
      // invalid mail? exit loop
      if (!resultValidator.isValide()) {
        exitloop = true;
      }
    }

    result.setResult(mailIsValid);
    if (mailIsValid) {
      result.setNrErrors(0);
    }

    // return result

    return result;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    ActionValidatorUtils.andValidator()
        .validate(
            this,
            "emailAddress",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
    ActionValidatorUtils.andValidator()
        .validate(
            this,
            "emailSender",
            remarks,
            AndValidator.putValidators(
                ActionValidatorUtils.notBlankValidator(), ActionValidatorUtils.emailValidator()));

    if (isSmtpCheck()) {
      ActionValidatorUtils.andValidator()
          .validate(
              this,
              "defaultSMTP",
              remarks,
              AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
    }
  }
}
