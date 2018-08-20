package com.maiya.kafka.consumer.bean;

/**
 * Created by DELL on 2016/3/28.
 */
public class AddressListBW {
    private String sGuid;
    private String sKeyWord;
    private String iBlackOrWhite;
    private String iScore;
    private String iType;
    private String sRemark;
    private String iDelFlag;
    private String dAddDate;
    private String sModifyPerson;
    private String dModifyDate;

    public String getsGuid() {
        return sGuid;
    }

    public String getsKeyWord() {
        return sKeyWord;
    }

    public String getiBlackOrWhite() {
        return iBlackOrWhite;
    }

    public String getiScore() {
        return iScore;
    }

    public String getiType() {
        return iType;
    }

    public String getsRemark() {
        return sRemark;
    }

    public String getiDelFlag() {
        return iDelFlag;
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

    public void setsKeyWord(String sKeyWord) {
        this.sKeyWord = sKeyWord;
    }

    public void setiBlackOrWhite(String iBlackOrWhite) {
        this.iBlackOrWhite = iBlackOrWhite;
    }

    public void setiScore(String iScore) {
        this.iScore = iScore;
    }

    public void setiType(String iType) {
        this.iType = iType;
    }

    public void setsRemark(String sRemark) {
        this.sRemark = sRemark;
    }

    public void setiDelFlag(String iDelFlag) {
        this.iDelFlag = iDelFlag;
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
