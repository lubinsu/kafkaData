package com.maiya.kafka.consumer.bean;

/**
 * Created by DELL on 2016/3/28.
 */
public class InnerBlackList {
    private String sGuid;
    private String sUserId;
    private String sUserNo;
    private String sBlackCode;
    private String sBlackRemark;
    private String iDelFlag;
    private String sMobile;
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

    public String getsBlackCode() {
        return sBlackCode;
    }

    public String getsBlackRemark() {
        return sBlackRemark;
    }

    public String getiDelFlag() {
        return iDelFlag;
    }

    public String getsMobile() {
        return sMobile;
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

    public void setsBlackCode(String sBlackCode) {
        this.sBlackCode = sBlackCode;
    }

    public void setsBlackRemark(String sBlackRemark) {
        this.sBlackRemark = sBlackRemark;
    }

    public void setiDelFlag(String iDelFlag) {
        this.iDelFlag = iDelFlag;
    }

    public void setsMobile(String sMobile) {
        this.sMobile = sMobile;
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
