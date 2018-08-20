package com.maiya.kafka.consumer.bean;

/**
 * Created by DELL on 2016/3/28.
 */
public class LinkMan {
    private String sGuid;
    private String sUserId;
    private String sUserNo;
    private String iLinkLevel;
    private String iSeq;
    private String sName;
    private String iRelationship;
    private String sMobile;
    private String iCheckResult;
    private String sCheckPerson;
    private String sCheckRemark;
    private String Txlpp;
    private String iTalkNum;
    private String sLasttalkdate;

    public String getsLasttalkdate() {
        return sLasttalkdate;
    }

    public void setsLasttalkdate(String sLasttalkdate) {
        this.sLasttalkdate = sLasttalkdate;
    }

    public void setTxlpp(String txlpp) {
        Txlpp = txlpp;
    }

    public void setiTalkNum(String iTalkNum) {
        this.iTalkNum = iTalkNum;
    }

    public String getTxlpp() {

        return Txlpp;
    }

    public String getiTalkNum() {
        return iTalkNum;
    }

    private String sCheckDate;
    private String iDelFlag;
    private String sAddPerson;
    private String dAddDate;
    private String sIpAddress;
    private String sModifyPerson;
    private String dModifyDate;

    public String getsGuid() {
        return sGuid;
    }

    public String getsUserId() {
        return sUserId;
    }

    public String getsUserNo() {
        return sUserNo;
    }

    public String getiLinkLevel() {
        return iLinkLevel;
    }

    public String getiSeq() {
        return iSeq;
    }

    public String getsName() {
        return sName;
    }

    public String getiRelationship() {
        return iRelationship;
    }

    public String getsMobile() {
        return sMobile;
    }

    public String getiCheckResult() {
        return iCheckResult;
    }

    public String getsCheckPerson() {
        return sCheckPerson;
    }

    public String getsCheckRemark() {
        return sCheckRemark;
    }

    public String getsCheckDate() {
        return sCheckDate;
    }

    public String getiDelFlag() {
        return iDelFlag;
    }

    public String getsAddPerson() {
        return sAddPerson;
    }

    public String getdAddDate() {
        return dAddDate;
    }

    public String getsIpAddress() {
        return sIpAddress;
    }

    public String getsModifyPerson() {
        return sModifyPerson;
    }

    public String getdModifyDate() {
        return dModifyDate;
    }

    public void setsGuid(String sGuid) {
        this.sGuid = sGuid;
    }

    public void setsUserId(String sUserId) {
        this.sUserId = sUserId;
    }

    public void setsUserNo(String sUserNo) {
        this.sUserNo = sUserNo;
    }

    public void setiLinkLevel(String iLinkLevel) {
        this.iLinkLevel = iLinkLevel;
    }

    public void setiSeq(String iSeq) {
        this.iSeq = iSeq;
    }

    public void setsName(String sName) {
        this.sName = sName;
    }

    public void setiRelationship(String iRelationship) {
        this.iRelationship = iRelationship;
    }

    public void setsMobile(String sMobile) {
        this.sMobile = sMobile;
    }

    public void setiCheckResult(String iCheckResult) {
        this.iCheckResult = iCheckResult;
    }

    public void setsCheckPerson(String sCheckPerson) {
        this.sCheckPerson = sCheckPerson;
    }

    public void setsCheckRemark(String sCheckRemark) {
        this.sCheckRemark = sCheckRemark;
    }

    public void setsCheckDate(String sCheckDate) {
        this.sCheckDate = sCheckDate;
    }

    public void setiDelFlag(String iDelFlag) {
        this.iDelFlag = iDelFlag;
    }

    public void setsAddPerson(String sAddPerson) {
        this.sAddPerson = sAddPerson;
    }

    public void setdAddDate(String dAddDate) {
        this.dAddDate = dAddDate;
    }

    public void setsIpAddress(String sIpAddress) {
        this.sIpAddress = sIpAddress;
    }

    public void setsModifyPerson(String sModifyPerson) {
        this.sModifyPerson = sModifyPerson;
    }

    public void setdModifyDate(String dModifyDate) {
        this.dModifyDate = dModifyDate;
    }
}
