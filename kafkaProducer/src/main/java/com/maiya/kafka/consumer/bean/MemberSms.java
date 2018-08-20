package com.maiya.kafka.consumer.bean;

/**
 * Created by DELL on 2016/3/28.
 */
public class MemberSms {
    private String sGuid;
    private String sUserId;
    private String sUserNo;
    private String sName;
    private String dSMSDate;
    private String sDeviceNo;
    private String sSendMobile;
    private String sRecMobile;
    private String sSMSContent;
    private String iDelFlag;
    private String sAddPerson;
    private String dAddDate;
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

    public String getdSMSDate() {
        return dSMSDate;
    }

    public String getsDeviceNo() {
        return sDeviceNo;
    }

    public String getsSendMobile() {
        return sSendMobile;
    }

    public String getsRecMobile() {
        return sRecMobile;
    }

    public String getsSMSContent() {
        return sSMSContent;
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

    public void setdSMSDate(String dSMSDate) {
        this.dSMSDate = dSMSDate;
    }

    public void setsDeviceNo(String sDeviceNo) {
        this.sDeviceNo = sDeviceNo;
    }

    public void setsSendMobile(String sSendMobile) {
        this.sSendMobile = sSendMobile;
    }

    public void setsRecMobile(String sRecMobile) {
        this.sRecMobile = sRecMobile;
    }

    public void setsSMSContent(String sSMSContent) {
        this.sSMSContent = sSMSContent;
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

    public void setsModifyPerson(String sModifyPerson) {
        this.sModifyPerson = sModifyPerson;
    }

    public void setdModifyDate(String dModifyDate) {
        this.dModifyDate = dModifyDate;
    }
}
