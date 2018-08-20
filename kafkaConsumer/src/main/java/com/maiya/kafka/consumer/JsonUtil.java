package com.maiya.kafka.consumer;

import com.maiya.kafka.consumer.bean.MemberContacts;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
//import org.json.JSONArray;
//import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by DELL on 2016/3/31.
 */
public class JsonUtil {

    public static void main(String[] args){
//        String json = "{\n" +
//                "    \"table_status\": \"insert\"," +
////                "    \"data\": {\n" +
//                "        \"hy_membercontacts\": \"[{\\\"sguid\\\":\\\"1\\\",\\\"sdeviceno\\\":\\\"android\\\",\\\"suserid\\\":\\\"22806F12D8F936FCE0530100007F37A1\\\",\\\"suserno\\\":\\\"001\\\",\\\"sname\\\":\\\"xiaoming\\\",\\\"stelphone\\\":\\\"18888888888\\\",\\\"sterminalchannel\\\":\\\"huawei\\\",\\\"idelflag\\\":\\\"1\\\",\\\"dadddate\\\":\\\"2016-03-15 00:00:00.0\\\"},{\\\"sguid\\\":\\\"2\\\",\\\"sdeviceno\\\":\\\"android\\\",\\\"suserid\\\":\\\"22806F12D8F936FCE0530100007F37A2\\\",\\\"suserno\\\":\\\"002\\\",\\\"sname\\\":\\\"lilei\\\",\\\"stelphone\\\":\\\"18888888888\\\",\\\"sterminalchannel\\\":\\\"sanxing\\\",\\\"idelflag\\\":\\\"2\\\",\\\"dadddate\\\":\\\"2016-03-16 00:00:00.0\\\"},{\\\"sguid\\\":\\\"3\\\",\\\"sdeviceno\\\":\\\"ios\\\",\\\"suserid\\\":\\\"22806F12D8F936FCE0530100007F37A3\\\",\\\"suserno\\\":\\\"003\\\",\\\"sname\\\":\\\"zhangsan\\\",\\\"stelphone\\\":\\\"18888888888\\\",\\\"sterminalchannel\\\":\\\"ios\\\",\\\"idelflag\\\":\\\"3\\\",\\\"dadddate\\\":\\\"2016-03-17 00:00:00.0\\\"},{\\\"sguid\\\":\\\"4\\\",\\\"sdeviceno\\\":\\\"ios\\\",\\\"suserid\\\":\\\"22806F12D8F936FCE0530100007F37A4\\\",\\\"suserno\\\":\\\"004\\\",\\\"sname\\\":\\\"lisi\\\",\\\"stelphone\\\":\\\"18888888888\\\",\\\"sterminalchannel\\\":\\\"ios\\\",\\\"idelflag\\\":\\\"4\\\",\\\"dadddate\\\":\\\"2016-03-18 00:00:00.0\\\"}]\"\n" +
////                "    }\n" +
//                "}";

      String json = "{\n" +
              "    \"table_name\": \"hy_membercontacts\"," +
              "    \"table_status\": \"insert\"," +
//              "    \"data\":{\n" +
              "        \"data\":  \"[{\\\"sguid\\\":\\\"1\\\",\\\"sdeviceno\\\":\\\"android\\\",\\\"suserid\\\":\\\"22806F12D8F936FCE0530100007F37A1\\\",\\\"suserno\\\":\\\"001\\\",\\\"sname\\\":\\\"xiaoming\\\",\\\"stelphone\\\":\\\"18888888888\\\",\\\"sterminalchannel\\\":\\\"huawei\\\",\\\"idelflag\\\":\\\"1\\\",\\\"dadddate\\\":\\\"2016-03-15 00:00:00.0\\\"},{\\\"sguid\\\":\\\"2\\\",\\\"sdeviceno\\\":\\\"android\\\",\\\"suserid\\\":\\\"22806F12D8F936FCE0530100007F37A2\\\",\\\"suserno\\\":\\\"002\\\",\\\"sname\\\":\\\"lilei\\\",\\\"stelphone\\\":\\\"18888888888\\\",\\\"sterminalchannel\\\":\\\"sanxing\\\",\\\"idelflag\\\":\\\"2\\\",\\\"dadddate\\\":\\\"2016-03-16 00:00:00.0\\\"},{\\\"sguid\\\":\\\"3\\\",\\\"sdeviceno\\\":\\\"ios\\\",\\\"suserid\\\":\\\"22806F12D8F936FCE0530100007F37A3\\\",\\\"suserno\\\":\\\"003\\\",\\\"sname\\\":\\\"zhangsan\\\",\\\"stelphone\\\":\\\"18888888888\\\",\\\"sterminalchannel\\\":\\\"ios\\\",\\\"idelflag\\\":\\\"3\\\",\\\"dadddate\\\":\\\"2016-03-17 00:00:00.0\\\"},{\\\"sguid\\\":\\\"4\\\",\\\"sdeviceno\\\":\\\"ios\\\",\\\"suserid\\\":\\\"22806F12D8F936FCE0530100007F37A4\\\",\\\"suserno\\\":\\\"004\\\",\\\"sname\\\":\\\"lisi\\\",\\\"stelphone\\\":\\\"18888888888\\\",\\\"sterminalchannel\\\":\\\"ios\\\",\\\"idelflag\\\":\\\"4\\\",\\\"dadddate\\\":\\\"2016-03-18 00:00:00.0\\\"}]\"\n" +
//                "    }\n" +
                "}";
//        String json = "{\n" +
//                "    \"table_status\":\"insert\",\n" +
//                "    \"hy_membercontacts\":\"[{\"sguid\":\"1\",\"sdeviceno\":\"android\",\"suserid\":\"22806F12D8F936FCE0530100007F37A1\",\"suserno\":\"001\",\"sname\":\"xiaoming\",\"stelphone\":\"18888888888\",\"sterminalchannel\":\"huawei\",\"idelflag\":\"1\",\"dadddate\":\"2016-03-15 00:00:00.0\"},{\"sguid\":\"2\",\"sdeviceno\":\"android\",\"suserid\":\"22806F12D8F936FCE0530100007F37A2\",\"suserno\":\"002\",\"sname\":\"lilei\",\"stelphone\":\"18888888888\",\"sterminalchannel\":\"sanxing\",\"idelflag\":\"2\",\"dadddate\":\"2016-03-16 00:00:00.0\"},{\"sguid\":\"3\",\"sdeviceno\":\"ios\",\"suserid\":\"22806F12D8F936FCE0530100007F37A3\",\"suserno\":\"003\",\"sname\":\"zhangsan\",\"stelphone\":\"18888888888\",\"sterminalchannel\":\"ios\",\"idelflag\":\"3\",\"dadddate\":\"2016-03-17 00:00:00.0\"},{\"sguid\":\"4\",\"sdeviceno\":\"ios\",\"suserid\":\"22806F12D8F936FCE0530100007F37A4\",\"suserno\":\"004\",\"sname\":\"lisi\",\"stelphone\":\"18888888888\",\"sterminalchannel\":\"ios\",\"idelflag\":\"4\",\"dadddate\":\"2016-03-18 00:00:00.0\"}]\"\n" +
//                "}";



        String table_status = "insert";

        System.out.println(json);
//        JSONObject jo = new JSONObject(json);
//
//        System.out.println(jo.getString("table_status"));

        JSONObject ob=JSONObject.fromObject(json);

        System.out.println(ob.get("table_name"));
        System.out.println(ob.get("table_status"));


        JSONArray data = ob.getJSONArray("data");
        System.out.println(data.size());
        List<MemberContacts> memberList = new ArrayList<MemberContacts>();
        JSONObject rows=null;
        for (int i = 0; i < data.size(); i++){
            MemberContacts memberContacts = new MemberContacts();
            rows = data.getJSONObject(i);
            memberContacts.setSguid(rows.getString("sguid"));
            memberContacts.setSdeviceno(rows.getString("sdeviceno"));
            memberContacts.setSuserid(rows.getString("suserid"));
            memberContacts.setSuserno(rows.getString("suserno"));
            memberContacts.setSname(rows.getString("sname"));
            memberContacts.setStelphone(rows.getString("stelphone"));
            memberContacts.setSterminalchannel(rows.getString("sterminalchannel"));
            memberContacts.setIdelflag(rows.getString("idelflag"));
            memberContacts.setDadddate(rows.getString("dadddate"));
            memberList.add(memberContacts);
        }

        for (int i = 0; i < memberList.size(); i++){
            System.out.println(memberList.get(i).getSguid());
        }

    }


}
