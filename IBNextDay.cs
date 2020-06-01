using Bankpro.EAI.Utility;
using Microsoft.EAIServer;
using Microsoft.Service;
using Microsoft.Service.Xml;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Xml;
using System.Xml.Linq;

namespace ESunBank.Gateway.BPM
{
    public class IBNextDay : XmlBaseBpmAdapter
    {
        private static readonly Logger m_log = LogManager.GetLogger("ESUNBank.Gateway.BPM.IBNextDay");
        private const string spN_GW = "GW";
        private const string custId_GW = "GW";
        private string m_FilePath = Environment.ExpandEnvironmentVariables(ProjectConfig.GetInstance().DropFilesPath);

        protected override AppXmlExecResult RunImpl(EaiContext context, string correlationID, string txID, HostTxDef txDef, XmlDocument requestXml)
        {
            switch (txID)
            {
                case "INTER.NEXTDAY.TRANS":  //企業行內轉帳(次日) 
                    return Do_NextDay_Trans_Process(context, correlationID, txID, txDef, requestXml);
                case "IB.INCOPR.TFR":        //企業行內轉帳(當日)
                    return Do_IB_INCOPR_TFR_Process(context, correlationID, txID, txDef, requestXml);
                case "INTER.NEXTDAY.QUERY":  //企業行內轉帳查詢(次日)
                    return Do_NextDay_Query_Process(context, correlationID, txID, txDef, requestXml);
                case "INTER.NEXTDAY.CANCEL": //企業行內轉帳撤銷(次日)
                    return Do_NextDay_Cancel_Process(context, correlationID, txID, txDef, requestXml);
                default:
                    return null;
            }
        }

        private AppXmlExecResult Do_NextDay_Trans_Process(EaiContext context, string correlationID, string txID, HostTxDef txDef, XmlDocument requestXml)
        {
            try
            {
                string mbRs = string.Empty;
                string msgId = "IB.INCOPR.TFR";
                string msmqLabel = "IBNextDay";
                string msmqBody = string.Empty;
                XmlHelper xmlHelper = XmlHelper.GetInstance(requestXml);

                #region 從次日行內轉帳電文，擷取FT扣帳成功後要存進DB的欄位
                string RecId = string.Empty; //扣帳時T24的RSP_TXN_ID
                string DebAcctNo = xmlHelper.GetXPath(requestXml, "//C_PAYER_ACCT_NO").Trim(); //000281000012504 
                string DebCusNo = string.Empty;  //DEBIT_CUSTOMER(RS)
                string CreAcctNo = xmlHelper.GetXPath(requestXml, "//C_PAYEE_ACCT_NO").Trim(); //000281000000077
                string IntAcctNo = xmlHelper.GetXPath(requestXml, "//CREDIT_ACCT_NO").Trim();  //CNY1406100010002
                string CardIntAcct = string.Empty;  //銀聯卡對應的內部帳號(個銀時才填入)，此時帶空值;
                string TxnDate = DateTime.Now.ToString("yyyyMMdd"); //扣帳當下日期
                string ValueDate = string.Empty; //DEBIT_VALUE_DATE(RS)
                decimal Amt = Convert.ToDecimal(xmlHelper.GetXPath(requestXml, "//DEBIT_AMOUNT").Trim());
                string CreCur = xmlHelper.GetXPath(requestXml, "//CREDIT_CURRENCY").Trim();
                string DebCur = xmlHelper.GetXPath(requestXml, "//DEBIT_CURRENCY").Trim();
                string TransGate = xmlHelper.GetXPath(requestXml, "//L_TRANS_GATE").Trim();
                string Remark = xmlHelper.GetXPath(requestXml, "//C_REMARK").Trim();
                string Remarks = xmlHelper.GetXPath(requestXml, "//C_REMARKS").Trim();
                string PayerName = xmlHelper.GetXPath(requestXml, "//C_PAYER_NAME").Trim(); //APPLE
                string PayeeName = xmlHelper.GetXPath(requestXml, "//C_PAYEE_NAME").Trim(); //00028100011000281000000077S
                string Ref = xmlHelper.GetXPath(requestXml, "//DEBIT_THEIR_REF").Trim();
                string RecTxn = string.Empty; //入帳時T24的RSP_TXN_ID
                int PayMethod = Convert.ToInt32(xmlHelper.GetXPath(requestXml, "//L_IB_PMT_M").Trim());
                int Flag = (int)CorpFlag.CORPORATION; //1:CORPORATION(企業)
                int Status = (int)IBNextIntTransStatus.ALLOW_REVERSE; //0:ALLOW_REVERSE(可撤銷)
                #endregion

                string strT24DataXmlRQ = RemoveNamespace(xmlHelper.SelectSingleNode(requestXml, "//T24_DATA").OuterXml);
                m_log.Info("Do_NextDay_Trans_Process strT24DataXmlRQ : {0} ", strT24DataXmlRQ);

                #region 發到T24進行扣帳
                AppXmlExecResult t24_result = SendMsgToEAIProcess(context, strT24DataXmlRQ, msgId, true);
                XmlHelper xmlHelperT24RS = XmlHelper.GetInstance(t24_result.ResponseXml);

                string rs_Code = xmlHelperT24RS.GetXPath(t24_result.ResponseXml, "//ITF_RETURN_CODE");
                string rs_Msg = xmlHelperT24RS.GetXPath(t24_result.ResponseXml, "//ITF_RETURN_MSG");
                string proc_ret = xmlHelperT24RS.GetXPath(t24_result.ResponseXml, "//RSP_PROC_RET");

                XmlNodeList xNodeT24DataRS = t24_result.ResponseXml.GetElementsByTagName("T24_DATA");
                string strT24DataXmlRS = (xNodeT24DataRS.Count > 0) ? xNodeT24DataRS[0].InnerXml : string.Empty;
                mbRs = RemoveNamespace(strT24DataXmlRS);
                m_log.Info("Do_NextDay_Trans_Process strT24DataXmlRS : {0} ", strT24DataXmlRS);

                //扣帳成功後
                if (rs_Code == "E-000000" && proc_ret == "SUCC")
                {
                    RecId = xmlHelperT24RS.GetXPath(t24_result.ResponseXml, "//RSP_TXN_ID");
                    DebCusNo = xmlHelperT24RS.GetXPath(t24_result.ResponseXml, "//DEBIT_CUSTOMER");
                    ValueDate = xmlHelperT24RS.GetXPath(t24_result.ResponseXml, "//DEBIT_VALUE_DATE");
                    Guid btID = Guid.NewGuid();
                    // 1.寫入IBNextIntTrans
                    int ibNextTransCnt = DBLog.InsertIBNextIntTrans(RecId, DebAcctNo, DebCusNo, CreAcctNo, IntAcctNo, CardIntAcct, TxnDate, ValueDate, Amt, CreCur, DebCur, TransGate, Remark, Remarks, PayerName, PayeeName, Ref, PayMethod, Flag, RecTxn, Status, btID);
                    // 2.寫入BroadcastMSMQTalk，於隔日透過MSMQ發起交易
                    #region 
                    xmlHelper.SetMultipleXPath(requestXml, "//MsgId", msgId); // 將次日轉帳MsgId轉換成當日
                    xmlHelper.SetMultipleXPath(requestXml, "//REQ_TXN_ID", RecId); // 將扣帳後的FT流水號置入原次日電文的REQ_TXN_ID中，才能在MSMQ發起隔日交易時取用該流水號
                    msmqBody = requestXml.OuterXml;
                    #endregion
                    int ibNextTransMSMQCnt = this.InsertToMSMQTalk(msmqLabel, msmqBody, btID);

                    XmlDocument responseXml = base.TransformCommMsg("0", "Info", "交易完成", mbRs);
                    return base.BuildExecResult(context, responseXml);
                }
                else
                {
                    XmlDocument responseXml = base.TransformCommMsg(t24_result.EaiRs.EaiErrCode, t24_result.EaiRs.EaiErrText, "交易完成", mbRs);
                    return base.BuildExecResult(context, responseXml);
                }
                #endregion
            }
            catch (Exception ex)
            {
                m_log.ErrorException(string.Format("Do_NextDay_Trans_Process Error ! TXID=[{0}] ", txID) + ex.ToString(), ex);
                XmlDocument responseXml = base.TransformCommMsg("99999", "Error", ex.Message, "");
                return base.BuildExecResult(context, responseXml);
            }
        }

        private AppXmlExecResult Do_IB_INCOPR_TFR_Process(EaiContext context, string correlationID, string txID, HostTxDef txDef, XmlDocument requestXml)
        {
            try
            {
                string mbRs = string.Empty;
                string msgId = txID;
                XmlHelper xmlHelper = XmlHelper.GetInstance(requestXml);

                #region 擷取存於MSMQ_Body中的IB.INCOPR.TFR電文，並置換借方貸方帳號
                //1.擷取欄位
                string IntAcctNo = xmlHelper.GetXPath(requestXml, "//CREDIT_ACCT_NO").Trim();  //CNY1406100010002
                string DebAcctNo = xmlHelper.GetXPath(requestXml, "//C_PAYER_ACCT_NO").Trim(); //000281000012504
                string CreAcctNo = xmlHelper.GetXPath(requestXml, "//C_PAYEE_ACCT_NO").Trim(); //000281000000077
                string RecId = xmlHelper.GetXPath(requestXml, "//REQ_TXN_ID").Trim();          //取扣帳時回覆的RSP_TXN_ID(FT流水號)
                string RecTxn = string.Empty;                                                  //存入帳時回覆的RSP_TXN_ID(FT流水號)
                int Status = (int)IBNextIntTransStatus.NOT_REVERSE; // 1:NOT_REVERSE(不可撤銷)
                //2.置換借貸方
                xmlHelper.SetMultipleXPath(requestXml, "//REQ_TXN_ID", "");            //將原先扣帳時暫存在REQ_TXN_ID的FT流水號清空
                xmlHelper.SetMultipleXPath(requestXml, "//DEBIT_ACCT_NO", IntAcctNo);  //CNY1406100010002
                xmlHelper.SetMultipleXPath(requestXml, "//CREDIT_ACCT_NO", CreAcctNo); //000281000000077
                #endregion

                string strT24DataXmlRQ = RemoveNamespace(xmlHelper.SelectSingleNode(requestXml, "//T24_DATA").OuterXml);
                m_log.Info("Do_IB_INCOPR_TFR_Process strT24DataXmlRQ : {0} ", strT24DataXmlRQ);

                #region 發到T24進行入帳
                AppXmlExecResult t24_result = SendMsgToEAIProcess(context, strT24DataXmlRQ, msgId, true);
                XmlHelper xmlHelperT24RS = XmlHelper.GetInstance(t24_result.ResponseXml);

                string rs_Code = xmlHelperT24RS.GetXPath(t24_result.ResponseXml, "//ITF_RETURN_CODE");
                string rs_Msg = xmlHelperT24RS.GetXPath(t24_result.ResponseXml, "//ITF_RETURN_MSG");
                string proc_ret = xmlHelperT24RS.GetXPath(t24_result.ResponseXml, "//RSP_PROC_RET");

                XmlNodeList xNodeT24DataRS = t24_result.ResponseXml.GetElementsByTagName("T24_DATA");
                string strT24DataXmlRS = (xNodeT24DataRS.Count > 0) ? xNodeT24DataRS[0].InnerXml : string.Empty;
                mbRs = RemoveNamespace(strT24DataXmlRS);
                m_log.Info("Do_NextDay_Trans_Process strT24DataXmlRS : {0} ", strT24DataXmlRS);

                if (rs_Code == "E-000000" && proc_ret == "SUCC")
                {
                    //入帳成功後更新IBNextIntTrans
                    RecTxn = xmlHelperT24RS.GetXPath(t24_result.ResponseXml, "//RSP_TXN_ID");
                    DBLog.UpdateAfterNextIntTrans(RecId, RecTxn, Status); //更新:RecTxn(入帳FT流水號)&Status(不可撤銷)

                    XmlDocument responseXml = base.TransformCommMsg("0", "Info", "交易完成", mbRs);
                    return base.BuildExecResult(context, responseXml);
                }
                else
                {
                    // 若交易失敗，貸方帳號CREDIT_ACCT_NO改帶DebAcctNo(000281000012504)
                    xmlHelper.SetMultipleXPath(requestXml, "//CREDIT_ACCT_NO", DebAcctNo);
                    strT24DataXmlRQ = RemoveNamespace(xmlHelper.SelectSingleNode(requestXml, "//T24_DATA").OuterXml);
                    m_log.Info("Do_IB_INCOPR_TFR_Process Change CREDIT_ACCT_NO strT24DataXmlRQ : {0} ", strT24DataXmlRQ);

                    #region 發到T24進行入帳
                    t24_result = SendMsgToEAIProcess(context, strT24DataXmlRQ, msgId, true);
                    xmlHelperT24RS = XmlHelper.GetInstance(t24_result.ResponseXml);

                    rs_Code = xmlHelperT24RS.GetXPath(t24_result.ResponseXml, "//ITF_RETURN_CODE");
                    rs_Msg = xmlHelperT24RS.GetXPath(t24_result.ResponseXml, "//ITF_RETURN_MSG");
                    proc_ret = xmlHelperT24RS.GetXPath(t24_result.ResponseXml, "//RSP_PROC_RET");

                    xNodeT24DataRS = t24_result.ResponseXml.GetElementsByTagName("T24_DATA");
                    strT24DataXmlRS = (xNodeT24DataRS.Count > 0) ? xNodeT24DataRS[0].InnerXml : string.Empty;
                    mbRs = RemoveNamespace(strT24DataXmlRS);
                    m_log.Info("Do_NextDay_Trans_Process Change CREDIT_ACCT_NO strT24DataXmlRS : {0} ", strT24DataXmlRS);

                    if (rs_Code == "E-000000" && proc_ret == "SUCC")
                    {
                        //入帳成功後更新IBNextIntTrans
                        RecTxn = xmlHelperT24RS.GetXPath(t24_result.ResponseXml, "//RSP_TXN_ID");
                        DBLog.UpdateAfterNextIntTrans(RecId, RecTxn, Status);

                        XmlDocument responseXml = base.TransformCommMsg("0", "Info", "交易完成", mbRs);
                        return base.BuildExecResult(context, responseXml);
                    }
                    else
                    {
                        // 1.SendErrorMail
                        new SendMail().Send(string.Format("{0} Do_IB_INCOPR_TFR_Process Error", txID), "", string.Format("{0} Do_IB_INCOPR_TFR_Process Error, PLS Check RQ:[{1}]", txID, context.RequestXml.OuterXml));
                        m_log.Error("Do_IB_INCOPR_TFR_Process Fail !!! rs_Code=[{0}] proc_ret=[{1}]", rs_Code, proc_ret);

                        // 2.回覆失敗報文
                        XmlDocument responseXml = base.TransformCommMsg(t24_result.EaiRs.EaiErrCode, t24_result.EaiRs.EaiErrText, "交易完成", mbRs);
                        return base.BuildExecResult(context, responseXml);
                    }
                    #endregion
                }
                #endregion
            }
            catch (Exception ex)
            {
                m_log.ErrorException(string.Format("Do_IB_INCOPR_TFR_Process Error ! TXID=[{0}] ", txID) + ex.ToString(), ex);
                XmlDocument responseXml = base.TransformCommMsg("99999", "Error", ex.Message, "");
                return base.BuildExecResult(context, responseXml);
            }
        }

        private AppXmlExecResult Do_NextDay_Query_Process(EaiContext context, string correlationID, string txID, HostTxDef txDef, XmlDocument requestXml)
        {
            try
            {
                string mbRs = string.Empty;
                XmlHelper xmlHelper = XmlHelper.GetInstance(requestXml);
                string DebAcctNo = xmlHelper.GetXPath(requestXml, "//DEBIT_ACCT_NO").Trim();
                string StartDate = xmlHelper.GetXPath(requestXml, "//START_DATE").Trim();
                string EndDate = xmlHelper.GetXPath(requestXml, "//END_DATE").Trim();

                DataSet dataSet = DBLog.SelectIBNextIntTransQuery(DebAcctNo, StartDate, EndDate);
                List<Dictionary<string, string>> nextDayDataRowList = DatasetToDiclist(dataSet);
                mbRs = GetNextDayQueryRS(nextDayDataRowList);

                XmlDocument responseXml = base.TransformCommMsg("0", "Info", "交易完成", mbRs);
                return base.BuildExecResult(context, responseXml);
            }
            catch (Exception ex)
            {
                m_log.ErrorException(string.Format("Do_NextDay_Query_Process Error ! TXID=[{0}] ", txID) + ex.ToString(), ex);
                XmlDocument responseXml = base.TransformCommMsg("99999", "Error", ex.Message, "");
                return base.BuildExecResult(context, responseXml);
            }
        }

        private AppXmlExecResult Do_NextDay_Cancel_Process(EaiContext context, string correlationID, string txID, HostTxDef txDef, XmlDocument requestXml)
        {
            try
            {
                string mbRs = string.Empty;
                string msgId = "IB.INCOPR.TFR";
                #region 擷取撤銷電文欄位
                string C_FT_RefNo = string.Empty;
                string IntAcctNo = string.Empty;
                string DebAcctNo = string.Empty;
                string CoCode = string.Empty;
                #endregion 擷取撤銷電文欄位
                #region 要從IBNextIntTrans取的欄位
                string RecId = string.Empty;
                string Status = string.Empty;
                string ValueDate = string.Empty;
                string MSMQ_BTID = string.Empty;
                #endregion 要從IBNextIntTrans取的欄位
                #region 扣帳交易Rsponse欄位
                string RsRecId = string.Empty;
                string RsDebCusNo = string.Empty;
                string RsDebAcctNo = string.Empty;
                string RsCreAcctNo = string.Empty;
                string RsDebCur = string.Empty;
                string RsAmt = string.Empty;
                string RsValueDate = string.Empty;
                string RsReference = string.Empty;
                string RsTransGate = string.Empty;
                #endregion 扣帳交易回傳值
                #region 交易結果回覆&錯誤訊息
                string RtSuccCode = "E-000000";
                string RtFailCode = "E-005000";
                string RtFailMsg = "FAIL";
                string RtSuccMsg = "SUCC";
                string RtFailMsg1 = "撤销失败，状态不可撤销";
                string RtFailMsg2 = "撤销失败，帐务交易不成功";
                string RtFailMsg3 = "撤销失败，系统错误";
                #endregion 交易結果回覆&錯誤訊息

                XmlHelper xmlHelper = XmlHelper.GetInstance(requestXml);
                xmlHelper.SetMultipleXPath(requestXml, "//MsgId", msgId); // 將轉帳撤銷MsgId做轉換

                C_FT_RefNo = xmlHelper.GetXPath(requestXml, "//C_ORG_FT_REF_NO").Trim(); //此欄會帶之前扣帳的流水號
                IntAcctNo = xmlHelper.GetXPath(requestXml, "//CREDIT_ACCT_NO").Trim();  //CNY1406100010002
                DebAcctNo = xmlHelper.GetXPath(requestXml, "//C_PAYER_ACCT_NO").Trim(); //000281000012504
                CoCode = xmlHelper.GetXPath(requestXml, "//SIGN_ON_BRH").Trim();

                DataSet dataSet = DBLog.SelectIBNextIntTransAll(C_FT_RefNo);
                List<Dictionary<string, string>> nextDayDataRowList = DatasetToDiclist(dataSet);
                Dictionary<string, string> nextDayDataRow = new Dictionary<string, string>();
                if (nextDayDataRowList.Count > 0)
                    nextDayDataRow = nextDayDataRowList[0];

                nextDayDataRow.TryGetValue("RecId", out RecId);
                nextDayDataRow.TryGetValue("Status", out Status);
                nextDayDataRow.TryGetValue("ValueDate", out ValueDate);
                nextDayDataRow.TryGetValue("MSMQ_BTID", out MSMQ_BTID);
                int tbStatus = Convert.ToInt32(Status);

                #region 判斷IBNextIntTrans之Status為0或1(0:繼續流程/1:回覆撤銷失敗訊息1)
                if (tbStatus == (int)IBNextIntTransStatus.ALLOW_REVERSE)
                {
                    #region 1.將Status由0改為1，若更新失敗(updateSuccess = 0)則回覆訊息3
                    int updateIBNextStatus = DBLog.UpdateNextIntTransStatus((int)IBNextIntTransStatus.NOT_REVERSE, RecId);
                    int updateMSMQStatus = DBLog.UpdateNextDayMSMQStatus((int)NextDayMSMQStatus.CANCEL, MSMQ_BTID);
                    if (updateIBNextStatus == 0 || updateMSMQStatus == 0)
                    {
                        //回覆撤銷失敗，系統錯誤(訊息3)
                        mbRs = GetNextDayCancelRS(RtFailMsg, RtFailCode, RtFailMsg3);
                        XmlDocument responseXml = base.TransformCommMsg("0", "Info", "交易完成", mbRs);
                        return base.BuildExecResult(context, responseXml);
                    }
                    #endregion

                    #region 2.將原電文欄位調整後送T24扣帳
                    xmlHelper.SetMultipleXPath(requestXml, "//MsgId", msgId);
                    xmlHelper.SetMultipleXPath(requestXml, "//DEBIT_ACCT_NO", IntAcctNo);
                    xmlHelper.SetMultipleXPath(requestXml, "//CREDIT_ACCT_NO", DebAcctNo);

                    string strT24DataXmlRQ = RemoveNamespace(xmlHelper.SelectSingleNode(requestXml, "//T24_DATA").OuterXml);
                    m_log.Info("Do_NextDay_Cancel_Process>>>>>>IB.INCOPR.TFR>>>>>>strT24DataXmlRQ : {0} ", strT24DataXmlRQ);

                    AppXmlExecResult t24_result = SendMsgToEAIProcess(context, strT24DataXmlRQ, msgId, true);
                    XmlHelper xmlHelperT24RS = XmlHelper.GetInstance(t24_result.ResponseXml);

                    string rs_Code = xmlHelperT24RS.GetXPath(t24_result.ResponseXml, "//ITF_RETURN_CODE");
                    string rs_Msg = xmlHelperT24RS.GetXPath(t24_result.ResponseXml, "//ITF_RETURN_MSG");
                    string proc_ret = xmlHelperT24RS.GetXPath(t24_result.ResponseXml, "//RSP_PROC_RET");

                    XmlNodeList xNodeT24DataRS = t24_result.ResponseXml.GetElementsByTagName("T24_DATA");
                    string strT24DataXmlRS = (xNodeT24DataRS.Count > 0) ? xNodeT24DataRS[0].InnerXml : string.Empty;
                    m_log.Info("Do_NextDay_Cancel_Process>>>>>>IB.INCOPR.TFR>>>>>>strT24DataXmlRS : {0} ", strT24DataXmlRS);

                    #region a.扣帳成功，依交易日期判斷是否發限額限筆
                    if (rs_Code == "E-000000" && proc_ret == "SUCC")
                    {
                        RsValueDate = xmlHelperT24RS.GetXPath(t24_result.ResponseXml, "//DEBIT_VALUE_DATE"); //扣帳回覆之交易日期

                        #region a-1.若扣帳RS的交易日期與表中ValueDate為同日，需另發限額限筆
                        if (ValueDate == RsValueDate)
                        {
                            RsReference = xmlHelperT24RS.GetXPath(t24_result.ResponseXml, "//RSP_TXN_ID").Trim(); //扣帳回覆之FT流水號
                            RsDebCusNo = xmlHelperT24RS.GetXPath(t24_result.ResponseXml, "//DEBIT_CUSTOMER").Trim();
                            RsDebAcctNo = xmlHelperT24RS.GetXPath(t24_result.ResponseXml, "//DEBIT_ACCT_NO").Trim();
                            RsCreAcctNo = xmlHelperT24RS.GetXPath(t24_result.ResponseXml, "//CREDIT_ACCT_NO").Trim();
                            RsAmt = xmlHelperT24RS.GetXPath(t24_result.ResponseXml, "//DEBIT_AMOUNT").Trim();
                            RsDebCur = xmlHelperT24RS.GetXPath(t24_result.ResponseXml, "//DEBIT_CURRENCY").Trim();
                            RsTransGate = GetTransGateMapping(xmlHelperT24RS.GetXPath(t24_result.ResponseXml, "//L_TRANS_GATE").Trim());
                            RsRecId = string.Format("{0}.{1}.{2}", RsDebAcctNo, RsValueDate, RsReference);  //限額限筆紀錄的RECID

                            //組ESCN.BP.DEBIT.LIMIT(限額限筆)報文
                            string t24DataXmlDebLimRQ = GetT24_DebLim_Content(CoCode, "I", RsRecId, RsDebCusNo, RsDebAcctNo, RsCreAcctNo, RsDebCur, RsAmt, RsValueDate, RsReference, RsTransGate);
                            m_log.Info("ESCN.BP.DEBIT.LIMIT報文 : {0} ", t24DataXmlDebLimRQ);

                            #region 發送限額限筆紀錄
                            AppXmlExecResult t24_DebLimResult = SendMsgToEAIProcess(context, t24DataXmlDebLimRQ, "ESCN.BP.DEBIT.LIMIT", true);
                            XmlHelper xmlHelperT24DebLimRS = XmlHelper.GetInstance(t24_DebLimResult.ResponseXml);
                            string debLim_RsCode = xmlHelperT24DebLimRS.GetXPath(t24_DebLimResult.ResponseXml, "//ITF_RETURN_CODE");
                            string debLim_RsMsg = xmlHelperT24DebLimRS.GetXPath(t24_DebLimResult.ResponseXml, "//ITF_RETURN_MSG");
                            string debLim_ProcRet = xmlHelperT24DebLimRS.GetXPath(t24_DebLimResult.ResponseXml, "//RSP_PROC_RET");

                            if (debLim_RsCode != "E-000000" && debLim_ProcRet != "SUCC")
                            {
                                //寄信 SendErrorMail
                                new SendMail().Send(string.Format("{0} Do_NextDay_Cancel_Process >>>>>> Do ESCN.BP.DEBIT.LIMIT Error", txID), "", string.Format("{0} Error, PLS Check RQ:[{1}]", txID, context.RequestXml.OuterXml));
                                m_log.Error("Do_NextDay_Cancel_Process Fail >>>>>> Do ESCN.BP.DEBIT.LIMIT Error!!! debLim_RsCode=[{0}] debLim_ProcRet=[{1}]", debLim_RsCode, debLim_ProcRet);
                            }
                            #endregion
                            //回覆撤銷成功
                            mbRs = GetNextDayCancelRS(RtSuccMsg, RtSuccCode, RtSuccMsg);
                            XmlDocument responseXml = base.TransformCommMsg("0", "Info", "交易完成", mbRs);
                            return base.BuildExecResult(context, responseXml);
                        }
                        #endregion

                        #region a-2.無須限額限筆，直接回覆撤銷成功
                        else
                        {
                            //回覆撤銷成功
                            mbRs = GetNextDayCancelRS(RtSuccMsg, RtSuccCode, RtSuccMsg);
                            XmlDocument responseXml = base.TransformCommMsg("0", "Info", "交易完成", mbRs);
                            return base.BuildExecResult(context, responseXml);
                        }
                        #endregion 無須限額限筆，直接回覆撤銷成功
                    }
                    #endregion a.扣帳成功，依交易日期判斷是否發限額限筆

                    #region b.扣帳失敗，無須發限額限筆(將Status從1改回0)
                    else
                    {
                        //1.將IBNextIntTrans>Status由1改為0，BroadcastMSMQTalk>Status由990改為999，回覆撤銷失敗，帳務交易不成功(訊息2)          
                        updateIBNextStatus = DBLog.UpdateNextIntTransStatus((int)IBNextIntTransStatus.ALLOW_REVERSE, RecId);
                        updateMSMQStatus = DBLog.UpdateNextDayMSMQStatus((int)NextDayMSMQStatus.TRANS, MSMQ_BTID);
                        mbRs = GetNextDayCancelRS(RtFailMsg, RtFailCode, RtFailMsg2);
                        if (updateIBNextStatus == 0 || updateMSMQStatus == 0)
                        {
                            //交易失敗且Status更新失敗
                            new SendMail().Send(string.Format("{0} Do_NextDay_Cancel_Process >>>>>> IB.INCOPR.TFR & UpdateTable Error", txID), "", string.Format("{0} Error, PLS Check RQ:[{1}]", txID, context.RequestXml.OuterXml));
                            m_log.Error("Do_NextDay_Cancel_Process Fail >>>>>> IB.INCOPR.TFR & UpdateTable Error!!! updateIBNextStatus=[{0}] updateMSMQStatus=[{1}]", updateIBNextStatus, updateMSMQStatus);
                            mbRs = GetNextDayCancelRS(RtFailMsg, RtFailCode, RtFailMsg3);
                        }
                        XmlDocument responseXml = base.TransformCommMsg("0", "Info", "交易完成", mbRs);
                        return base.BuildExecResult(context, responseXml);
                    }
                    #endregion b.扣帳失敗，無須發限額限筆(將Status從1改回0)

                    #endregion 2.將原電文欄位調整後送T24扣帳
                }
                else
                {
                    //查回的Status已經為1，回覆撤銷失敗，系統錯誤(訊息1)
                    mbRs = GetNextDayCancelRS(RtFailMsg, RtFailCode, RtFailMsg1);
                    XmlDocument responseXml = base.TransformCommMsg("0", "Info", "交易完成", mbRs);
                    return base.BuildExecResult(context, responseXml);
                }
                #endregion 判斷IBNextIntTrans之Status為0或1(0:繼續流程/1:回覆撤銷失敗訊息1)
            }
            catch (Exception ex)
            {
                m_log.ErrorException(string.Format("Do_NextDay_Cancel_Process Error ! TXID=[{0}] ", txID) + ex.ToString(), ex);
                XmlDocument responseXml = base.TransformCommMsg("99999", "Error", ex.Message, "");
                return base.BuildExecResult(context, responseXml);
            }
        }

        /// <summary>
        /// 將扣帳回覆的TransGate欄位做對應轉換
        /// </summary>
        /// <param name="strTransGate"></param>
        /// <returns></returns>
        private string GetTransGateMapping(string strTransGate)
        {
            switch (strTransGate)
            {
                case "PIB":
                    return "01";
                case "CIB":
                    return "01";
                case "PHONE":
                    return "02";
                default:
                    return strTransGate;
            }
        }

        /// <summary>
        /// 組企業行內轉帳查詢(次日)回覆
        /// </summary>
        /// <param name="nextDayTransDicList"></param>
        /// <returns></returns>
        private string GetNextDayQueryRS(List<Dictionary<string, string>> nextDayTransDicList)
        {
            #region 格式
            //<RSP_MSG_DATA no="1">
            //  <RSP_ENQ_CODE></RSP_ENQ_CODE>
            //  <RSP_PROC_RET>SUCC</RSP_PROC_RET>
            //  <RSP_MSG_ROW row="1">
            //    <RSP_TXN_ID>FT16125H08FH</RSP_TXN_ID>
            //    <DEBIT_ACCT_NO>000281000012504</DEBIT_ACCT_NO>
            //    <CREDIT_ACCT_NO>000281000000077</CREDIT_ACCT_NO>
            //    <VALUE_DATE>20200408</VALUE_DATE>
            //    <L_TRANS_GATE mp="1" sp="1">CIB</L_TRANS_GATE>
            //    <C_PAYEE_NAME mp="1" sp="1">ABC</C_PAYEE_NAME>
            //    <DEBIT_THEIR_REF mp="1" sp="1">貨款</DEBIT_THEIR_REF>
            //    <C_REMARKS mp="1" sp="1">FOR TEST</C_REMARKS>
            //    <L_IB_PMT_M mp="1" sp="1">2</L_IB_PMT_M>
            //    <STATUS mp="1" sp="1">0</STATUS>
            //  </RSP_MSG_ROW>
            //  <RSP_MSG_ROW row="2">
            //    <RSP_TXN_ID>FT16125H08FH</RSP_TXN_ID>
            //    <DEBIT_ACCT_NO>000281000012504</DEBIT_ACCT_NO>
            //    <CREDIT_ACCT_NO>000281000000077</CREDIT_ACCT_NO>
            //    <VALUE_DATE>20200408</VALUE_DATE>
            //    <L_TRANS_GATE mp="1" sp="1">CIB</L_TRANS_GATE>
            //    <C_PAYEE_NAME mp="1" sp="1">ABC</C_PAYEE_NAME>
            //    <DEBIT_THEIR_REF mp="1" sp="1">貨款</DEBIT_THEIR_REF>
            //    <C_REMARKS mp="1" sp="1">FOR TEST</C_REMARKS>
            //    <L_IB_PMT_M mp="1" sp="1">2</L_IB_PMT_M>
            //    <STATUS mp="1" sp="1">0</STATUS>
            //  </RSP_MSG_ROW>
            //</RSP_MSG_DATA>
            #endregion 格式

            string rtMsg = (nextDayTransDicList.Count == 0) ? "查无资料" : "交易成功";

            XElement xDoc = new XElement("RSP_MSG_DATA", new XAttribute("row", 1),
                            new XElement("RSP_ENQ_CODE", "SUCC"),
                            new XElement("RSP_PROC_RET", rtMsg));

            for (int i = 0; i <= nextDayTransDicList.Count - 1; i++)
            {
                var nextDayTransDic = nextDayTransDicList[i];

                xDoc.Add(new XElement("RSP_MSG_ROW", new XAttribute("row", i + 1),
                            new XElement("RSP_TXN_ID", nextDayTransDic["RecId"]),
                            new XElement("DEBIT_ACCT_NO", nextDayTransDic["DebAcctNo"]),
                            new XElement("CREDIT_ACCT_NO", nextDayTransDic["CreAcctNo"]),
                            new XElement("VALUE_DATE", nextDayTransDic["TxnDate"]),
                            new XElement("DEBIT_AMOUNT", nextDayTransDic["Amt"]),
                            new XElement("DEBIT_CURRENCY", nextDayTransDic["DebCur"]),
                            new XElement("L_TRANS_GATE", nextDayTransDic["TransGate"], new XAttribute("mp", 1), new XAttribute("sp", 1)),
                            new XElement("C_REMARKS", nextDayTransDic["Remarks"], new XAttribute("mp", 1), new XAttribute("sp", 1)),
                            new XElement("C_PAYEE_NAME", nextDayTransDic["PayeeName"], new XAttribute("mp", 1), new XAttribute("sp", 1)),
                            new XElement("DEBIT_THEIR_REF", nextDayTransDic["Ref"], new XAttribute("mp", 1), new XAttribute("sp", 1)),
                            new XElement("L_IB_PMT_M", nextDayTransDic["PayMethod"], new XAttribute("mp", 1), new XAttribute("sp", 1)),
                            new XElement("STATUS", nextDayTransDic["Status"], new XAttribute("mp", 1), new XAttribute("sp", 1))
                            ));
            }

            XElement xT24_EAI = new XElement("T24_EAI",
                    new XElement("T24_EAI_HDR", new XElement("MSG_SYS_ID"), new XElement("HDR_MD5_DES")),
                    new XElement("T24_EAI_MSG", new XElement("RSP_PROC_INFO", new XElement("UNQ_REF_ID"), new XElement("RSP_PROC_SYS"), new XElement("PROC_RESULT"), new XElement("DATE_TIME_REQ"), new XElement("DATE_TIME_RSP")),
                    new XElement("RSP_MSG_GRP", xDoc)));

            return xT24_EAI.ToString();
        }

        /// <summary>
        /// 組企業行內轉帳撤銷(次日)回覆
        /// </summary>
        /// <param name="rtCode"></param>
        /// <param name="rtMsg"></param>
        /// <returns></returns>
        private string GetNextDayCancelRS(string rsProcRet, string rtCode, string rtMsg)
        {
            XElement xDoc = new XElement("RSP_MSG_DATA", new XAttribute("row", 1),
                new XElement("RSP_PROC_RET", rsProcRet),
                new XElement("ITF_RETURN_CODE", rtCode, new XAttribute("mp", 1), new XAttribute("sp", 1)),
                new XElement("ITF_RETURN_MSG", rtMsg, new XAttribute("mp", 1), new XAttribute("sp", 1)));

            XElement xT24_EAI = new XElement("T24_EAI",
                new XElement("T24_EAI_HDR", new XElement("MSG_SYS_ID"), new XElement("HDR_MD5_DES")),
                new XElement("T24_EAI_MSG", new XElement("RSP_PROC_INFO", new XElement("UNQ_REF_ID"), new XElement("RSP_PROC_SYS"), new XElement("PROC_RESULT"), new XElement("DATE_TIME_REQ"), new XElement("DATE_TIME_RSP")),
                new XElement("RSP_MSG_GRP", xDoc)));

            return xT24_EAI.ToString();
        }

        /// <summary>
        /// 扣帳完成後將資訊存至BroadcastMSMQTalk(Status=999為MSMQ發起條件)
        /// </summary>
        /// <param name="msmqLabel"></param>
        /// <param name="body"></param>
        /// <returns></returns>
        private int InsertToMSMQTalk(string msmqLabel, string msmqBody, Guid btID)
        {
            //Guid btID = Guid.NewGuid();
            string strLocationID = string.Format("Location[{0}]", ProjectConfig.GetInstance().LocationCount);
            string strReceiveQueuePath = ProjectConfig.GetInstance().GetReceiveQueuePath(strLocationID);
            string strSendURL = ProjectConfig.GetInstance().Send_URL;

            string MSMQ_Label = msmqLabel;                         //訊息標題
            string MSMQ_Body = msmqBody;                           //訊息內容
            string MSMQ_Type = "Normal";                           //訊息型態
            string MSMQ_Path = strReceiveQueuePath;                //MSMQueue路徑
            string MachineName = System.Environment.MachineName;   //目前的機器名稱
            string URL = strSendURL;                               //傳送的URL路徑
            string CreaterUser = "Gateway.BPM.IBNextDay";          //由這支程式及FT流水號建立
            int MSMQ_Size = MSMQ_Body.Length;                      //訊息長度
            int MSMQ_Priority = 3;                                 //訊息優先順序   
            int Status = (int)NextDayMSMQStatus.TRANS;             //for MSMQTalk WorkMen發動次日轉帳之條件參數

            return DBLog.InsBroadcastMSMQTalk(btID, MSMQ_Label, MSMQ_Body, MSMQ_Priority, MSMQ_Type, MSMQ_Size, MSMQ_Path, MachineName, URL, CreaterUser, Status);
        }

        private XDocument RemoveNamespace(XDocument xdoc)
        {
            foreach (XElement e in xdoc.Root.DescendantsAndSelf())
            {
                if (e.Name.Namespace != XNamespace.None)
                    e.Name = XNamespace.None.GetName(e.Name.LocalName);
                if (e.Attributes().Where(a => a.IsNamespaceDeclaration || a.Name.Namespace != XNamespace.None).Any())
                    e.ReplaceAttributes(e.Attributes().Select(a => a.IsNamespaceDeclaration ? null : a.Name.Namespace != XNamespace.None ? new XAttribute(XNamespace.None.GetName(a.Name.LocalName), a.Value) : a));
            }
            return xdoc;
        }

        private string RemoveNamespace(string oriXml)
        {
            string removeNameSpaceXml = string.Empty;
            if (!string.IsNullOrEmpty(oriXml))
            {
                XmlDocument t24DataXmlDoc = new XmlDocument();
                t24DataXmlDoc.LoadXml(oriXml);
                XDocument t24DataXDoc = RemoveNamespace(XDocument.Load(new XmlNodeReader(t24DataXmlDoc)));
                t24DataXmlDoc.Load(t24DataXDoc.CreateReader());
                removeNameSpaceXml = t24DataXmlDoc.InnerXml;
            }
            return removeNameSpaceXml;
        }

        private AppXmlExecResult SendMsgToEAIProcess(EaiContext context, string body, string eAI_MsgKey, bool SendToEAI)
        {
            string msgContent = string.Empty;
            if (SendToEAI)
            { msgContent = base.SendToEAIProcess(body, eAI_MsgKey, spN_GW, custId_GW); }
            else
            { msgContent = base.SendToEAIProcess(body, eAI_MsgKey); }
            m_log.Info("msgConetent{0}:", msgContent);
            XmlDocument rq = new XmlDocument();
            rq.LoadXml(msgContent);
            XmlDocument subRqXml = null;
            if (SendToEAI)
            { subRqXml = CopyToNewDocument(rq, spN_GW, custId_GW, eAI_MsgKey, Guid.NewGuid().ToString()); }
            else
            { subRqXml = CopyToNewDocument(rq, eAI_MsgKey, Guid.NewGuid().ToString()); }
            m_log.Debug("AppXmlExecResult RunImpl.{0}.RQ={1}", eAI_MsgKey, subRqXml.InnerXml);
            AppXmlExecResult result = Send1Recv1(m_log, context, subRqXml, SendToEAI);
            m_log.Debug("AppXmlExecResult RunImpl.{0}.RS={1}", eAI_MsgKey, result.ResponseXml.InnerXml);

            return result;
        }

        public List<Dictionary<string, string>> DatasetToDiclist(DataSet dataset)
        {
            List<Dictionary<string, string>> lstdata = new List<Dictionary<string, string>>();
            if (dataset != null && dataset.Tables.Count > 0)
            {
                foreach (DataRow drow in dataset.Tables[0].Rows)
                {
                    Dictionary<string, string> tempData = new Dictionary<string, string>();

                    for (int i = 0; i <= drow.ItemArray.Length - 1; i++)
                    {
                        tempData.Add(dataset.Tables[0].Columns[i].ColumnName, drow.ItemArray[i].ToString());
                    }
                    lstdata.Add(tempData);
                }
            }
            return lstdata;
        }

        /// <summary>
        /// 限額限筆報文格式
        /// </summary>
        /// <param name="co_Code"></param>
        /// <param name="proc_FUNC"></param>
        /// <param name="recId"></param>
        /// <param name="debCustNo"></param>
        /// <param name="debAcctNo"></param>
        /// <param name="creAcctNo"></param>
        /// <param name="debCur"></param>
        /// <param name="amt"></param>
        /// <param name="valueDate"></param>
        /// <param name="reference"></param>
        /// <param name="transGate"></param>
        /// <returns></returns>
        private string GetT24_DebLim_Content(string co_Code, string proc_FUNC, string recId, string debCustNo, string debAcctNo, string creAcctNo,
            string debCur, string amt, string valueDate, string reference, string transGate)
        {
            StringBuilder sb = new StringBuilder();
            #region ESCN.BP.DEBIT.LIMIT XML MSG
            sb.Append("<T24_DATA>");
            sb.Append("<T24_EAI>");
            sb.Append("<T24_EAI_HDR>");
            sb.Append("<MSG_SYS_ID/>");
            sb.Append("<HDR_MD5_DES/>");
            sb.Append("</T24_EAI_HDR>");
            sb.Append("<T24_EAI_MSG>");
            sb.Append("<REQ_MSG_GRP>");
            sb.Append("<REQ_MSG_OPT>");
            sb.AppendFormat("<SIGN_ON_ID>{0}</SIGN_ON_ID>", ProjectConfig.GetInstance().OfsUser);
            sb.AppendFormat("<SIGN_ON_PSWD>{0}</SIGN_ON_PSWD>", ProjectConfig.GetInstance().OfsPwd);
            sb.AppendFormat("<SIGN_ON_BRH>{0}</SIGN_ON_BRH>", co_Code);
            sb.AppendFormat("<PROC_FUNC>{0}</PROC_FUNC>", proc_FUNC);
            sb.Append("</REQ_MSG_OPT>");
            sb.Append("<REQ_MSG_DATA>");
            sb.AppendFormat("<REQ_TXN_CODE>{0}</REQ_TXN_CODE>", "ESCN.BP.DEBIT.LIMIT");
            sb.AppendFormat("<REQ_TXN_CODE_S>{0}</REQ_TXN_CODE_S>", "");
            sb.AppendFormat("<REQ_TXN_ID>{0}</REQ_TXN_ID>", recId);
            sb.AppendFormat("<CUSTOMER_NO sp='1' mp='1'>{0}</CUSTOMER_NO>", debCustNo);
            sb.AppendFormat("<DEBIT_ACCT sp='1' mp='1'>{0}</DEBIT_ACCT>", debAcctNo);
            sb.AppendFormat("<CREDIT_ACCT sp='1' mp='1'>{0}</CREDIT_ACCT>", creAcctNo);
            sb.AppendFormat("<CURRENCY sp='1' mp='1'>{0}</CURRENCY>", debCur);
            sb.AppendFormat("<AMOUNT sp='1' mp='1'>{0}</AMOUNT>", (Convert.ToDecimal(amt) * -1).ToString());
            sb.AppendFormat("<VALUE_DATE sp='1' mp='1'>{0}</VALUE_DATE>", valueDate);
            sb.AppendFormat("<REFERENCE sp='1' mp='1'>{0}</REFERENCE>", reference);
            sb.AppendFormat("<TRANS_GATE sp='1' mp='1'>{0}</TRANS_GATE>", transGate);
            sb.AppendFormat("<ITF_MSGKEY op='EQ'>{0}</ITF_MSGKEY>", "IB001.0011");
            sb.AppendFormat("<CHANNEL_ID op='EQ'>{0}</CHANNEL_ID>", ProjectConfig.GetInstance().ITFChannelID);
            sb.AppendFormat("<TERM_NO op='EQ'></TERM_NO>", System.Environment.MachineName);
            sb.AppendFormat("<EXT_BUSS_DATE op='EQ'>{0}</EXT_BUSS_DATE>", DateTime.Today.ToString("yyyyMMdd"));
            sb.AppendFormat("<EXT_REFERENCE op='EQ'>{0}</EXT_REFERENCE>", SeqNoMgr.GetInstance().GetNextSeqNo("ITF", 10));
            sb.AppendFormat("<EXT_TXN_TIME op='EQ'>{0}</EXT_TXN_TIME>", DateTime.Now.ToString("yyyyMMddHHmmss"));
            sb.Append("</REQ_MSG_DATA>");
            sb.Append("</REQ_MSG_GRP>");
            sb.Append("<REQ_PROC_INFO>");
            sb.Append("<UNQ_REF_ID/>");
            sb.Append("<REQ_PROC_SYS/>");
            sb.Append("</REQ_PROC_INFO>");
            sb.Append("</T24_EAI_MSG>");
            sb.Append("</T24_EAI>");
            sb.Append("</T24_DATA>");
            #endregion
            return sb.ToString();
        }

        /// <summary>
        /// 做為BroadcastMSMQTalk發起次日交易之判斷
        /// </summary>
        /// <param name="CANCEL">990:已撤銷(故MSMQ不會發起隔日交易)</param>
        /// <param name="TRANS">999:待執行隔日(入帳)交易</param>
        private enum NextDayMSMQStatus
        {
            CANCEL = 990,
            TRANS = 999
        }

        /// <summary>
        /// IBNextIntTrans資料表的Status(0可撤銷/1不可撤銷)
        /// </summary>
        /// <param name="ALLOW_REVERSE">0:可撤銷</param>
        /// <param name="NOT_REVERSE">1:不可撤銷</param>
        private enum IBNextIntTransStatus
        {
            ALLOW_REVERSE = 0,
            NOT_REVERSE = 1
        }

        /// <summary>
        /// 個人或企業
        /// </summary>
        /// <param name="INDIVIDUAL">個人</param>
        /// <param name="CORPORATION">企業</param>
        private enum CorpFlag
        {
            INDIVIDUAL = 0,
            CORPORATION = 1
        }
    }
}
