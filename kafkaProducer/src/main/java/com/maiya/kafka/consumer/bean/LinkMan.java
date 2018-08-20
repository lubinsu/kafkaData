package com.maiya.kafka.consumer.bean;

/**
 * Created by DELL on 2016/3/28.
 */
public class LinkMan {
    private String sGuid;
    private String sUserId;
    private String sUserNo;
    private int iLinkLevel;
    private int iSeq;
    private String sName;
    private int iRelationship;
    private String sMobile;
    private int iCheckResult;
    private String sCheckPerson;
    private String sCheckRemark;
    private String Txlpp;
    private String iTalkNum;
    private String sLasttalkdate;

    public int getiLinkLevel() {
        return iLinkLevel;
    }

    public LinkMan setiLinkLevel(int iLinkLevel) {
        this.iLinkLevel = iLinkLevel;
        return this;
    }

    public int getiSeq() {
        return iSeq;
    }

    public LinkMan setiSeq(int iSeq) {
        this.iSeq = iSeq;
        return this;
    }

    public int getiRelationship() {
        return iRelationship;
    }

    public LinkMan setiRelationship(int iRelationship) {
        this.iRelationship = iRelationship;
        return this;
    }

    public int getiCheckResult() {
        return iCheckResult;
    }

    public LinkMan setiCheckResult(int iCheckResult) {
        this.iCheckResult = iCheckResult;
        return this;
    }

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
    private int iDelFlag;
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


    public String getsName() {
        return sName;
    }


    public String getsMobile() {
        return sMobile;
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

    public void setsName(String sName) {
        this.sName = sName;
    }

    public void setsMobile(String sMobile) {
        this.sMobile = sMobile;
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

    public int getiDelFlag() {
        return iDelFlag;
    }

    public LinkMan setiDelFlag(int iDelFlag) {
        this.iDelFlag = iDelFlag;
        return this;
    }
}
