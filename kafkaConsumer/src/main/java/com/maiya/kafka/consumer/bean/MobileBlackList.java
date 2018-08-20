package com.maiya.kafka.consumer.bean;

/**
 * Created by DELL on 2016/4/8.
 */
public class MobileBlackList {
    private String sGuid;
    private String sMobile;
    private String sRemark;
    private String iDelFlag;
    private String sAddPerson;
    private String dAddDate;
    private String sModifyPerson;
    private String dModifyDate;

    public String getsGuid() {
        return sGuid;
    }

    public void setsGuid(String sGuid) {
        this.sGuid = sGuid;
    }

    public String getsMobile() {
        return sMobile;
    }

    public String getsRemark() {
        return sRemark;
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

    public void setsMobile(String sMobile) {
        this.sMobile = sMobile;
    }

    public void setsRemark(String sRemark) {
        this.sRemark = sRemark;
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
