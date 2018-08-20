/*
 * Copyright 2015-2020 frbao.com.com All right reserved.
 */
package com.maiya.kafka.consumer;

import com.lamfire.logger.Logger;
import com.maiya.kafka.consumer.bean.*;
import com.maiya.kafka.consumer.utils.ProducerUtil;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;


/**
 * @author zxc Sep 15, 2015 12:14:38 PM
 */
public class ProducerBoot {

    private static final Logger logger = Logger.getLogger(ProducerBoot.class);

    public static final String FULL_DATE_FORMAT_PATTERN = "yyyy-MM-dd HH:mm:ss";
    static ScheduledExecutorService executor = Executors.newScheduledThreadPool(3);

    public static void main(String[] args) throws InterruptedException {
        ProducerUtil producerUtil = new ProducerUtil();
        producerUtil.init("bigdata.properties");

        final String uuid = UUID.randomUUID().toString().replaceAll("-", "");
        String msg1 = "";
        String msg2 = "";
        String msg3 = "";
        String msg4 = "";
        String msg5 = "";
        String msg6 = "";
        String msg7 = "";
        String msg8  = "1";
        msg4 = hy_membercallhistory();
/*        msg1 = hy_MemberContacts();

        msg2 = hy_membercallhistory();
        msg3 = hy_innerblacklist();
        msg4 = hy_MobileBlackList();
        msg5 = hy_membersms();
        msg6 = hy_linkman();*/


        /*msg7 = hy_userinfo();*/

        //ProducerUtil producer = new ProducerUtil();
        //String uuid = UUID.randomUUID().toString().replaceAll("-", "");

        //producerUtil.send("hy_userinfo", uuid, msg8);

        for (int i = 0; i < 2; i++) {
            producerUtil.send("test", uuid, String.valueOf(i));
        }
        //        producer.send("hy_membercontacts", uuid, msg1);



//                    producerUtil.send("hy_mobileblacklist", "key" + uuid, msg4);
        /*for (int i = 0; i <= 11; i++) {
            producer.send("hy_membercallhistory", uuid, (i+1) + msg2);
        }

        Thread.sleep(1000);
        System.out.println("==============");
        producer.send("hy_membercallhistory", uuid, "13" + msg2);*/

        /*producerUtil.send("hy_membercontacts", uuid, msg1);
        producerUtil.send("hy_membercallhistory", uuid, msg2);
        producerUtil.send("hy_innerblacklist", uuid, msg3);
        producerUtil.send("hy_mobileblacklist", uuid, msg4);
        producerUtil.send("hy_membersms", uuid, msg5);
        producerUtil.send("hy_linkman", uuid, msg6);
        producerUtil.send("hy_userinfo", uuid, msg7);*/
//        logger.debug(msg7);
    }

    public static String hy_membercallhistory() {
        String msg = null;
        JSONObject jsonObject = new JSONObject();
        JSONArray array = new JSONArray();
        String table_status = "insert";

        List<MemberCallHistory> list = new ArrayList<MemberCallHistory>();

        MemberCallHistory memberCallHistory = new MemberCallHistory();
        MemberCallHistory memberCallHistory1 = new MemberCallHistory();


        memberCallHistory.setSguid("2222222222222222");
        memberCallHistory.setSuserid("00000000000001");
        memberCallHistory.setSuserno("W-888");
        memberCallHistory.setSdeviceno("android");
        memberCallHistory.setSname("wang");
        memberCallHistory.setScalledmobile("16666666666");
        memberCallHistory.setScallingmobile("18888888888");
        memberCallHistory.setItalktime("2016-08-15");
        memberCallHistory.setDtalkdate("2016-08-15");
        memberCallHistory.setItalkresult("aaaaaaa");
        memberCallHistory.setIdelflag("1");
        memberCallHistory.setSaddperson("liuming");
        memberCallHistory.setDadddate("2016-08-15");
        memberCallHistory.setSmodifyperson("join");
        memberCallHistory.setDmodifydate("2016-08-15");

        memberCallHistory1.setSguid("1111111111111111111");
        memberCallHistory1.setSuserid("00000000000001");
        memberCallHistory1.setSuserno("W-888");
        memberCallHistory1.setSdeviceno("android");
        memberCallHistory1.setSname("wang");
        memberCallHistory1.setScalledmobile("16666666666");
        memberCallHistory1.setScallingmobile("18888888888");
        memberCallHistory1.setItalktime("2016-08-15");
        memberCallHistory1.setDtalkdate("2016-08-15");
        memberCallHistory1.setItalkresult("aaaaaaa");
        memberCallHistory1.setIdelflag("1");
        memberCallHistory1.setSaddperson("liuming");
        memberCallHistory1.setDadddate("2016-08-15");
        memberCallHistory1.setSmodifyperson("join");
        memberCallHistory1.setDmodifydate("2016-08-15");

        JSONObject hy_MemberCallHistoryJson = new JSONObject();

        JSONObject hy_MemberCallHistoryJson1 = new JSONObject();

        hy_MemberCallHistoryJson.put("sGuid", memberCallHistory.getSguid());
        hy_MemberCallHistoryJson.put("sUserId", memberCallHistory.getSuserid());
        hy_MemberCallHistoryJson.put("sUserNo", memberCallHistory.getSuserno());
        hy_MemberCallHistoryJson.put("sDeviceNo", memberCallHistory.getSdeviceno());
        hy_MemberCallHistoryJson.put("sName", memberCallHistory.getSname());
        hy_MemberCallHistoryJson.put("sCallingMobile", memberCallHistory.getScallingmobile());
        hy_MemberCallHistoryJson.put("sCalledMobile", memberCallHistory.getScalledmobile());
        hy_MemberCallHistoryJson.put("iTalkTime", memberCallHistory.getItalktime());
        hy_MemberCallHistoryJson.put("dTalkDate", memberCallHistory.getDtalkdate());
        hy_MemberCallHistoryJson.put("iTalkResult", memberCallHistory.getItalkresult());
        hy_MemberCallHistoryJson.put("iDelFlag", memberCallHistory.getIdelflag());
        hy_MemberCallHistoryJson.put("sAddPerson", memberCallHistory.getSaddperson());
        hy_MemberCallHistoryJson.put("dAddDate", memberCallHistory.getDadddate());
        hy_MemberCallHistoryJson.put("sModifyPerson", memberCallHistory.getSmodifyperson());
        hy_MemberCallHistoryJson.put("dModifyDate", memberCallHistory.getDmodifydate());

        hy_MemberCallHistoryJson1.put("sGuid", memberCallHistory.getSguid());
        hy_MemberCallHistoryJson1.put("sUserId", memberCallHistory.getSuserid());
        hy_MemberCallHistoryJson1.put("sUserNo", memberCallHistory.getSuserno());
        hy_MemberCallHistoryJson1.put("sDeviceNo", memberCallHistory.getSdeviceno());
        hy_MemberCallHistoryJson1.put("sName", memberCallHistory.getSname());
        hy_MemberCallHistoryJson1.put("sCallingMobile", memberCallHistory.getScallingmobile());
        hy_MemberCallHistoryJson1.put("sCalledMobile", memberCallHistory.getScalledmobile());
        hy_MemberCallHistoryJson1.put("iTalkTime", memberCallHistory.getItalktime());
        hy_MemberCallHistoryJson1.put("dTalkDate", memberCallHistory.getDtalkdate());
        hy_MemberCallHistoryJson1.put("iTalkResult", memberCallHistory.getItalkresult());
        hy_MemberCallHistoryJson1.put("iDelFlag", memberCallHistory.getIdelflag());
        hy_MemberCallHistoryJson1.put("sAddPerson", memberCallHistory.getSaddperson());
        hy_MemberCallHistoryJson1.put("dAddDate", memberCallHistory.getDadddate());
        hy_MemberCallHistoryJson1.put("sModifyPerson", memberCallHistory.getSmodifyperson());
        hy_MemberCallHistoryJson1.put("dModifyDate", memberCallHistory.getDmodifydate());

        array.put(hy_MemberCallHistoryJson);
        array.put(hy_MemberCallHistoryJson1);

        jsonObject.put("data", array);
        jsonObject.put("table_status", table_status);
        jsonObject.put("table_name", "hy_membercallhistory");

        System.out.println(jsonObject.toString());

        msg = jsonObject.toString();

        return msg;
    }

    public static String hy_MemberContacts() {
        String msg = null;
        JSONObject jsonObject = new JSONObject();
        JSONArray array = new JSONArray();
        String table_status = "insert";

        List<MemberContacts> list = new ArrayList<>();

        MemberContacts hyMemberContacts = new MemberContacts();

        hyMemberContacts.setSguid("0000000000000000000000");
        hyMemberContacts.setSdeviceno("863837023689236");
        hyMemberContacts.setSuserid("1000");
        hyMemberContacts.setSuserno("M-001");
        hyMemberContacts.setSname("外婆");
        hyMemberContacts.setStelphone("14b46ebbfd5f4d85a7598b02e657c554");
        hyMemberContacts.setSterminalchannel("baidu");
        hyMemberContacts.setIdelflag(1);
        hyMemberContacts.setDadddate("2016-08-15");


        JSONObject hyMemberContactsJson = new JSONObject();
        JSONObject hyMemberContactsJson1 = new JSONObject();

        hyMemberContactsJson.put("sGuid", hyMemberContacts.getSguid());
        hyMemberContactsJson.put("sDeviceNo", hyMemberContacts.getSdeviceno());
        hyMemberContactsJson.put("sUserId", hyMemberContacts.getSuserid());
        hyMemberContactsJson.put("sUserNo", hyMemberContacts.getSuserno());
        hyMemberContactsJson.put("sName", hyMemberContacts.getSname());
        hyMemberContactsJson.put("sTelPhone", hyMemberContacts.getStelphone());
        hyMemberContactsJson.put("sTerminalChannel", hyMemberContacts.getSterminalchannel());
        hyMemberContactsJson.put("iDelFlag", hyMemberContacts.getIdelflag());
        hyMemberContactsJson.put("dAddDate", hyMemberContacts.getDadddate());
        array.put(hyMemberContactsJson);
        //jsonObject.put("hyMemberContacts",array);

        hyMemberContacts.setSguid("888888888888888888");
        hyMemberContacts.setSdeviceno("863837023689236");
        hyMemberContacts.setSuserid("1000");
        hyMemberContacts.setSuserno("M-001");
        hyMemberContacts.setSname("九斗鱼1");
        hyMemberContacts.setStelphone("1501");
        hyMemberContacts.setSterminalchannel("baidu");
        hyMemberContacts.setIdelflag(1);
        hyMemberContacts.setDadddate("2016-08-15");

        hyMemberContactsJson1.put("sGuid", hyMemberContacts.getSguid());
        hyMemberContactsJson1.put("sDeviceNo", hyMemberContacts.getSdeviceno());
        hyMemberContactsJson1.put("sUserId", hyMemberContacts.getSuserid());
        hyMemberContactsJson1.put("sUserNo", hyMemberContacts.getSuserno());
        hyMemberContactsJson1.put("sName", hyMemberContacts.getSname());
        hyMemberContactsJson1.put("sTelPhone", hyMemberContacts.getStelphone());
        hyMemberContactsJson1.put("sTerminalChannel", hyMemberContacts.getSterminalchannel());
        hyMemberContactsJson1.put("iDelFlag", hyMemberContacts.getIdelflag());
        hyMemberContactsJson1.put("dAddDate", hyMemberContacts.getDadddate());
        array.put(hyMemberContactsJson);
        array.put(hyMemberContactsJson1);
        //jsonObject.put("hyMemberContacts",array);

        array.put(hyMemberContactsJson1);

        jsonObject.put("data", array);
//        jsonObject.put("")

        jsonObject.put("table_status", table_status);
        jsonObject.put("table_name", "hy_MemberContacts");

        System.out.println(jsonObject.toString());

        msg = jsonObject.toString();

        return msg;
    }


    public static String hy_innerblacklist() {
        String msg = null;
        JSONObject jsonObject = new JSONObject();
        JSONArray array = new JSONArray();
        String table_status = "insert";

        List<InnerBlackList> list = new ArrayList<InnerBlackList>();

        InnerBlackList innerBlackList = new InnerBlackList();

        innerBlackList.setsGuid("30b04bfda33b442d8fe341a3460c3ded");
        innerBlackList.setsUserId("87e2bb9c61934ad1834e8e18eecb2581");
        innerBlackList.setsUserNo("YHXX160218000001");
        innerBlackList.setsMobile("13918917439");
        innerBlackList.setsBlackCode("overdue");
        innerBlackList.setsBlackRemark("Inner black list");
        innerBlackList.setsAddPerson("admin");
        innerBlackList.setdAddDate("May 5, 2016 9:10:49 AM");
        innerBlackList.setsModifyPerson("admin");
        innerBlackList.setdModifyDate("May 5, 2016 9:10:49 AM");
        innerBlackList.setiDelFlag("1");

        JSONObject hy_innerBlackListJson = new JSONObject();
        hy_innerBlackListJson.put("sGuid", innerBlackList.getsGuid());
        hy_innerBlackListJson.put("sUserId", innerBlackList.getsUserId());
        hy_innerBlackListJson.put("sUserNo", innerBlackList.getsUserNo());
        hy_innerBlackListJson.put("sMobile", innerBlackList.getsMobile());
        hy_innerBlackListJson.put("sBlackCode", innerBlackList.getsBlackCode());
        hy_innerBlackListJson.put("sBlackRemark", innerBlackList.getsBlackRemark());
        hy_innerBlackListJson.put("sAddPerson", innerBlackList.getsAddPerson());
        hy_innerBlackListJson.put("dAddDate", innerBlackList.getdAddDate());
        hy_innerBlackListJson.put("sModifyPerson", innerBlackList.getsModifyPerson());
        hy_innerBlackListJson.put("dModifyDate", innerBlackList.getdModifyDate());
        hy_innerBlackListJson.put("iDelFlag", innerBlackList.getiDelFlag());

        array.put(hy_innerBlackListJson);

        jsonObject.put("data", array);
        jsonObject.put("table_status", table_status);
        jsonObject.put("table_name", "hy_innerblacklist");

        System.out.println(jsonObject.toString());

        msg = jsonObject.toString();

        return msg;
    }

    public static String hy_MobileBlackList() {
        String msg = null;
        JSONObject jsonObject = new JSONObject();
        JSONArray array = new JSONArray();
        String table_status = "insert";

        List<MobileBlackList> list = new ArrayList<MobileBlackList>();

        MobileBlackList mobileBlackList = new MobileBlackList();

        mobileBlackList.setsGuid("07003b710b334e768cd6babc97ed3e88");
        mobileBlackList.setsMobile("13918917439");
        mobileBlackList.setsRemark("Mobile black list");
        mobileBlackList.setiDelFlag("1");

        mobileBlackList.setsAddPerson("admin");
        mobileBlackList.setdAddDate("IMay 5, 2016 3:31:42 PM");
        mobileBlackList.setsModifyPerson("admin");
        mobileBlackList.setdModifyDate("May 5, 2016 3:31:42 PM");


        JSONObject hy_mobileBlackListJson = new JSONObject();
        hy_mobileBlackListJson.put("sGuid", mobileBlackList.getsGuid());
        hy_mobileBlackListJson.put("sMobile", mobileBlackList.getsMobile());
        hy_mobileBlackListJson.put("sRemark", mobileBlackList.getsRemark());
        hy_mobileBlackListJson.put("iDelFlag", mobileBlackList.getiDelFlag());
        hy_mobileBlackListJson.put("sAddPerson", mobileBlackList.getsAddPerson());
        hy_mobileBlackListJson.put("dAddDate", mobileBlackList.getdAddDate());
        hy_mobileBlackListJson.put("sModifyPerson", mobileBlackList.getsModifyPerson());
        hy_mobileBlackListJson.put("dModifyDate", mobileBlackList.getdModifyDate());


        array.put(hy_mobileBlackListJson);

        jsonObject.put("data", array);
        jsonObject.put("table_status", table_status);
        jsonObject.put("table_name", "hy_mobileblacklist");

        System.out.println(jsonObject.toString());

        msg = jsonObject.toString();

        return msg;
    }

    public static String hy_membersms() {
        String msg = null;
        JSONObject jsonObject = new JSONObject();
        JSONArray array = new JSONArray();
        String table_status = "insert";

        List<MemberSms> list = new ArrayList<MemberSms>();

        MemberSms memberSms = new MemberSms();

        memberSms.setsGuid("96ffcd1e9a004a3796952b6915772580");
        memberSms.setsUserId("d7b866cf54704078a484125d7fa6f973");
        memberSms.setsUserNo("160126000001");
        memberSms.setsName("1");
        memberSms.setdSMSDate("20150902145916");
        memberSms.setsDeviceNo("");
        memberSms.setsSendMobile("");
        memberSms.setsRecMobile("15951945240");
        memberSms.setsSMSContent("而我认为");

        memberSms.setiDelFlag("");
        memberSms.setsAddPerson("");
        memberSms.setdAddDate("");

        memberSms.setsModifyPerson("");
        memberSms.setdModifyDate("");

        JSONObject hy_memberSmsJson = new JSONObject();
        hy_memberSmsJson.put("sGuid", memberSms.getsGuid());
        hy_memberSmsJson.put("sUserId", memberSms.getsUserId());
        hy_memberSmsJson.put("sUserNo", memberSms.getsUserNo());
        hy_memberSmsJson.put("sName", memberSms.getsName());
        hy_memberSmsJson.put("dSMSDate", memberSms.getdSMSDate());
        hy_memberSmsJson.put("sDeviceNo", memberSms.getsDeviceNo());
        hy_memberSmsJson.put("sSendMobile", memberSms.getsSendMobile());
        hy_memberSmsJson.put("sRecMobile", memberSms.getsRecMobile());
        hy_memberSmsJson.put("sSMSContent", memberSms.getsSMSContent());
        hy_memberSmsJson.put("iDelFlag", memberSms.getiDelFlag());
        hy_memberSmsJson.put("sAddPerson", memberSms.getsAddPerson());
        hy_memberSmsJson.put("dAddDate", memberSms.getdAddDate());
        hy_memberSmsJson.put("sModifyPerson", memberSms.getsModifyPerson());
        hy_memberSmsJson.put("dModifyDate", memberSms.getdModifyDate());

        array.put(hy_memberSmsJson);

        jsonObject.put("data", array);
        jsonObject.put("table_status", table_status);
        jsonObject.put("table_name", "hy_membersms");

        System.out.println(jsonObject.toString());

        msg = jsonObject.toString();

        return msg;
    }

    public static String hy_linkman() {
        String msg = null;
        JSONObject jsonObject = new JSONObject();
        JSONArray array = new JSONArray();
        String table_status = "insert";

        List<LinkMan> list = new ArrayList<LinkMan>();

        LinkMan linkMan = new LinkMan();

        linkMan.setsGuid("96ffcd1e9a004a3796952b6915772580");
        linkMan.setsUserId("1000");
        linkMan.setsUserNo("160126000001");
        linkMan.setsName("网利宝");
        linkMan.setiLinkLevel(8);
        linkMan.setiSeq(0);
        linkMan.setiRelationship(0);
        linkMan.setsMobile("13390601122");
        linkMan.setiCheckResult(0);
        linkMan.setsCheckPerson("");
        linkMan.setsCheckRemark("");

        linkMan.setsCheckDate("");
        linkMan.setiDelFlag(1);
        linkMan.setsAddPerson("xs");
        linkMan.setdAddDate("");
        linkMan.setsIpAddress("");
        linkMan.setsModifyPerson("");
        linkMan.setdModifyDate("");

        JSONObject hy_linkManJson = new JSONObject();
        hy_linkManJson.put("sGuid", linkMan.getsGuid());
        hy_linkManJson.put("sUserId", linkMan.getsUserId());
        hy_linkManJson.put("sUserNo", linkMan.getsUserNo());
        hy_linkManJson.put("sName", linkMan.getsName());
        hy_linkManJson.put("iLinkLevel", linkMan.getiLinkLevel());
        hy_linkManJson.put("iSeq", linkMan.getiSeq());

        hy_linkManJson.put("iRelationship", linkMan.getiRelationship());
        hy_linkManJson.put("sMobile", linkMan.getsMobile());
        hy_linkManJson.put("iCheckResult", linkMan.getiCheckResult());
        hy_linkManJson.put("sCheckPerson", linkMan.getsCheckPerson());
        hy_linkManJson.put("sCheckRemark", linkMan.getsCheckRemark());
        hy_linkManJson.put("sCheckDate", linkMan.getsCheckDate());
        hy_linkManJson.put("iDelFlag", linkMan.getiDelFlag());
        hy_linkManJson.put("sAddPerson", linkMan.getsAddPerson());
        hy_linkManJson.put("dAddDate", linkMan.getdAddDate());
        hy_linkManJson.put("sIpAddress", linkMan.getsIpAddress());
        hy_linkManJson.put("sModifyPerson", linkMan.getsModifyPerson());
        hy_linkManJson.put("dModifyDate", linkMan.getdModifyDate());
        array.put(hy_linkManJson);

        jsonObject.put("data", array);
        jsonObject.put("table_status", table_status);
        jsonObject.put("table_name", "hy_linkman");

        System.out.println(jsonObject.toString());

        msg = jsonObject.toString();

        return msg;
    }

    public static String hy_userinfo() {
        String msg = null;
        JSONObject jsonObject = new JSONObject();
        JSONArray array = new JSONArray();
        String table_status = "insert";

        List<UserInfo> list = new ArrayList<UserInfo>();

        UserInfo userInfo = new UserInfo();

        userInfo.setSguid("222222222222222222222222");
        userInfo.setSuserno("d7b866cf54704078a484125d7fa6f973");
        userInfo.setSmobile("15951945240");
        userInfo.setSemail("test@test.com");
        userInfo.setSname("test");
        userInfo.setIsex("m");
        userInfo.setDbirthdate("1990-09-08");
        userInfo.setIidtype("1");
        userInfo.setSidno("110108199211112738");
        userInfo.setSprovince("");
        userInfo.setScity("");
        userInfo.setArea("");
        userInfo.setSprovinceborn("");
        userInfo.setScityborn("");

        userInfo.setSaddress("xxxx");
        userInfo.setIaddresscheck("xxxx");
        userInfo.setBisauthenticate("xxx");
        userInfo.setDauthenticatedate("测试数据");
        userInfo.setIstatus("8");
        userInfo.setHeadphoto("");
        userInfo.setSremark("");
        userInfo.setDregisterdate("xx");
        userInfo.setDmodifydate("");
        userInfo.setSmodifyperson("");
        userInfo.setIversion("");
        userInfo.setSsource("");
        userInfo.setSinfosource("");
        userInfo.setSinfodetail("");

        JSONObject hy_userInfoJson = new JSONObject();
        hy_userInfoJson.put("sGuid", userInfo.getSguid());
        hy_userInfoJson.put("sUserNo", userInfo.getSuserno());
        hy_userInfoJson.put("sMobile", userInfo.getSmobile());
        hy_userInfoJson.put("sEmail", userInfo.getSemail());
        hy_userInfoJson.put("sName", userInfo.getSname());
        hy_userInfoJson.put("iSex", userInfo.getIsex());
        hy_userInfoJson.put("dBirthdate", userInfo.getDbirthdate());
        hy_userInfoJson.put("iIdtype", userInfo.getIidtype());
        hy_userInfoJson.put("sIdNo", userInfo.getSidno());
        hy_userInfoJson.put("sProvince", userInfo.getSprovince());
        hy_userInfoJson.put("sCity", userInfo.getScity());
        hy_userInfoJson.put("area", userInfo.getArea());
        hy_userInfoJson.put("sProvinceBorn", userInfo.getSprovinceborn());
        hy_userInfoJson.put("sCityBorn", userInfo.getScityborn());

        hy_userInfoJson.put("sAddress", userInfo.getSaddress());
        hy_userInfoJson.put("iAddressCheck", userInfo.getIaddresscheck());
        hy_userInfoJson.put("bIsAuthenticate", userInfo.getBisauthenticate());
        hy_userInfoJson.put("dAuthenticateDate", userInfo.getDauthenticatedate());
        hy_userInfoJson.put("iStatus", userInfo.getIstatus());
        hy_userInfoJson.put("headPhoto", userInfo.getHeadphoto());
        hy_userInfoJson.put("sRemark", userInfo.getSremark());
        hy_userInfoJson.put("dRegisterDate", userInfo.getDregisterdate());
        hy_userInfoJson.put("dModifyDate", userInfo.getDmodifydate());
        hy_userInfoJson.put("sModifyPerson", userInfo.getSmodifyperson());
        hy_userInfoJson.put("iVersion", userInfo.getIversion());
        hy_userInfoJson.put("sSource", userInfo.getSsource());
        hy_userInfoJson.put("sInfoSource", userInfo.getSinfosource());
        hy_userInfoJson.put("sInfoDetail", userInfo.getSinfodetail());
        array.put(hy_userInfoJson);

        jsonObject.put("data", array);
        jsonObject.put("table_status", table_status);
        jsonObject.put("table_name", "hy_userinfo");

        System.out.println(jsonObject.toString());

        msg = jsonObject.toString();

        return msg;
    }
}
