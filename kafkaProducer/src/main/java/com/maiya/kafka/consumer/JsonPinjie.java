package com.maiya.kafka.consumer;

import com.maiya.kafka.consumer.bean.MemberContacts;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by DELL on 2016/4/10.
 */
public class JsonPinjie {


    public static void main(String[] args) {

        String uuid = UUID.randomUUID().toString().replaceAll("-", "");
        String msg = "";
        String table_status = "insert";

        JSONObject jsonObject = new JSONObject();
        JSONArray array = new JSONArray();

        JSONObject listjsonObject = new JSONObject();


        JSONObject hyMemberContactsJson = new JSONObject();
        JSONObject hyMemberContactsJson1 = new JSONObject();

        List<MemberContacts> list = new ArrayList();

            MemberContacts hyMemberContacts = new MemberContacts();
            hyMemberContacts.setSguid("11111111111");
            hyMemberContacts.setSdeviceno("android");
            hyMemberContacts.setSuserid("111111");
            hyMemberContacts.setSuserno("M-001");
            hyMemberContacts.setSname("xiaoming");
            hyMemberContacts.setStelphone("18888888888");
            hyMemberContacts.setSterminalchannel("baidu");
            hyMemberContacts.setIdelflag(1);
            hyMemberContacts.setDadddate("2016-03-21");

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
            listjsonObject.put("list",array);
            //第一个list

            //jsonObject.put("hyMemberContacts",array);

            hyMemberContacts.setSguid("11111111111");
            hyMemberContacts.setSdeviceno("android");
            hyMemberContacts.setSuserid("111111");
            hyMemberContacts.setSuserno("M-001");
            hyMemberContacts.setSname("xiaoming");
            hyMemberContacts.setStelphone("7438");
            hyMemberContacts.setSterminalchannel("baidu");
            hyMemberContacts.setIdelflag(1);
            hyMemberContacts.setDadddate("2016-03-21");

            hyMemberContactsJson1.put("sGuid", hyMemberContacts.getSguid());
            hyMemberContactsJson1.put("sDeviceNo", hyMemberContacts.getSdeviceno());
            hyMemberContactsJson1.put("sUserId", hyMemberContacts.getSuserid());
            hyMemberContactsJson1.put("sUserNo", hyMemberContacts.getSuserno());
            hyMemberContactsJson1.put("sName", hyMemberContacts.getSname());
            hyMemberContactsJson1.put("sTelPhone", hyMemberContacts.getStelphone());
            hyMemberContactsJson1.put("sTerminalChannel", hyMemberContacts.getSterminalchannel());
            hyMemberContactsJson1.put("iDelFlag", hyMemberContacts.getIdelflag());
            hyMemberContactsJson1.put("dAddDate", hyMemberContacts.getDadddate());

        //第二个list
            array.put(hyMemberContactsJson1);
            listjsonObject.put("list",array);
            //jsonObject.put("hyMemberContacts",array);

            jsonObject.put("result", listjsonObject);
//        jsonObject.put("")

            jsonObject.put("table_status", table_status);

            System.out.println(jsonObject.toString());

            msg = jsonObject.toString();

        }


}
