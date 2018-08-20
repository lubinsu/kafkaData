package com.maiya.kafka.consumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lamfire.logger.Logger;
import com.maiya.kafka.consumer.utils.ProducerUtil;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.maiya.kafka.consumer.CheckParm.CheckTableParm.getParam1;

/**
 * Created by DELL on 2016/5/26.
 */
public class FrbaoProducer {
    private static final Logger logger = Logger.getLogger(ProducerBoot.class);

    public static final String FULL_DATE_FORMAT_PATTERN = "yyyy-MM-dd HH:mm:ss";
    static ScheduledExecutorService executor = Executors.newScheduledThreadPool(3);
    private static ProducerUtil proc = new ProducerUtil();

    public static void main(String[] args) {

        String msg1 = "{\n" +
                "        \"session_id\": \"2016030112265900475DADB1B-112F-402B-886E-46649280866E \",\n" +
                "            \"event\": \"SysData\",\n" +
                "            \"properties\": {\n" +
                "        \"apk_name\": \"付融宝\",\n" +
                "                \"apk_version\": \"3.5.1\",\n" +
                "                \"terminal_type\": \"iphone\",\n" +
                "                \"os_version\": \"ios9.2.1\",\n" +
                "                \"hardware_type\": \"iphone6s\",\n" +
                "                \"identification\": \"75DADB1B-112F-402B-886E-46649280866E\",\n" +
                "                \"resolution\": \"960x540\",\n" +
                "                \"ip\": \"168.192.1.1\",\n" +
                "                \"city\": \"南京市\",\n" +
                "                \"carrier\": \"中国移动\",\n" +
                "                \"network_type\": \"4G\",\n" +
                "                \"source\": \"0010\",\n" +
                "                \"logon_type\": \"R\",\n" +
                "                \"account\": \"188888888888\",\n" +
                "                \"location\": \"54.123212_60.232211\"\n" +
                "        }\n" +
                "    }";

        String msg2 = "{\n" +
                "    \"session_id\": \"2016030112265900475DADB1B-112F-402B-886E-46649280866E \",\n" +
                "     \"collect_time\": \"20160303120000000\",\n" +
                "    \"event\": \"UseInfo\",\n" +
                "    \"properties\": {\n" +
                "         \"begin_time\": \"20160303120000000\",\n" +
                "        \"end_time\": \"20160303120800000\"\n" +
                "    }\n" +
                "}\n";

        String msg3 = "{\n" +
                "    \"session_id\": \"2016030112265900475DADB1B-112F-402B-886E-46649280866E \",\n" +
                "    \"collect_time\": \"20160303120000000\",\n" +
                "    \"event\": \"ViewPage\",\n" +
                "    \"properties\": {\n" +
                "        \"page_name\": \"页面名称\",\n" +
                "        \"enter_time\":\"20160303120000000\",\n" +
                "       \"leave_time\":\"20160303120800000\",\n" +
                "        \"cur_page_no\":\"1\"\n" +
                "    }\n" +
                "}";

        String msg4 = "{\n" +
                "    \"session_id\": \"2016030112265900475DADB1B-112F-402B-886E-46649280866E \",\n" +
                "    \"collect_time\": \"20160303120800000\",\n" +
                "    \"event\": \"InWord\",\n" +
                "    \"properties\": {\n" +
                "        \" keywords \": \"输入词\", \n" +
                "        \"page_name\": \"页面名称\", \n" +
                "        \" position \": \"输入框名称\",\n" +
                "          \"enter_time\":\"20160303120000000\",\n" +
                "          \"leave_time\":\"20160303120800000\",\n" +
                "\"cur_page_no\":\"1\"\n" +
                "    }\n" +
                "}\n";

        String msg5 = "{\n" +
                "    \"session_id\": \"2016030112265900475DADB1B-112F-402B-886E-46649280866E \",\n" +
                "    \"collect_time\": \"20160303120800000\",\n" +
                "    \"event\": \"SignUp\",\n" +
                "    \"properties\": {\n" +
                "        \" reg_account \": \"1888888888\",\n" +
                "        \"cur_page_no\":\"1\"\n" +
                "    }\n" +
                "}\n";

        String msg6 = "{\n" +
                "   \"session_id\": \"2016030112265900475DADB1B-112F-402B-886E-46649280866E \",\n" +
                "    \"collect_time\": \"20160303120800000\",\n" +
                "     \"event\": \"InvestDetail\",\n" +
                "    \"properties\": {\n" +
                "        \" invest_no \": \"12345678\",\n" +
                "         \"cur_page_no\":\"1\"\n" +
                "    }\n" +
                "}\n";

        String msg7 = "{\n" +
                "   \"session_id\": \"2016030112265900475DADB1B-112F-402B-886E-46649280866E \",\n" +
                "    \"collect_time\": \"20160303120800000\",\n" +
                "    \"event\": \"OnClick\",\n" +
                "\"properties\": {\n" +
                "        \" click_name\": \"register_home_phone \",\n" +
                " \"cur_page_no\":\"1\"\n" +
                "    }\n" +
                "}\n";

        String msg8 = "{\n" +
                "    \"session_id\": \"2016030112265900475DADB1B-112F-402B-886E-46649280866E \",\n" +
                "    \"collect_time\": \"20160303120800000\",\n" +
                "    \"event\": \"CrashDetail\",\n" +
                "\"properties\": {\n" +
                "\"apk_name\": \"付融宝\",\n" +
                "        \"apk_version\": \"3.5.1\",\n" +
                "        \"terminal_type\": \"iphone\",\n" +
                "        \"os_version\": \"ios9.2.1\",\n" +
                "        \"hardware_type\": \"iphone6s\",\n" +
                "        \"identification\": \"75DADB1B-112F-402B-886E-46649280866E\",\n" +
                "        \"resolution\": \"960x540\",\n" +
                "        \"ip\": \"168.192.1.1\",\n" +
                "        \"city\": \"南京市\",\n" +
                "        \"carrier\": \"中国移动\",\n" +
                "        \"network_type\": \"4G\",\n" +
                "        \"source\": \"0010\",\n" +
                "        \"logon_type\": \"R\",\n" +
                "        \"account\": \"188888888888\",\n" +
                "        \"location\": \"54.123212_60.232211\",\n" +
                "        \" crash_info \": \"崩溃堆栈\"\n" +
                "    }\n" +
                "}\n";

        String ret1 = SendMessage(msg1);
        String ret2 = SendMessage(msg2);
        String ret3 = SendMessage(msg3);
        String ret4 = SendMessage(msg4);
        String ret5 = SendMessage(msg5);
        String ret6 = SendMessage(msg6);
        String ret7 = SendMessage(msg7);
        String ret8 = SendMessage(msg8);

//        System.out.println("ret = " + ret1);

    }

    public static String SendMessage(String msg){
        String uuid = UUID.randomUUID().toString().replaceAll("-", "");
        String flag = null;

        JSONObject jsonObject = JSON.parseObject(msg);

        String session_id = jsonObject.getString("session_id");
        String event = jsonObject.getString("event").toLowerCase();

        if (event.equals("sysdata")){
            System.out.println("enter sysdata.");
            Object jsonArray = jsonObject.get("properties");
//            System.out.println("data = " + jsonArray);

            String count = (checkCount(msg, ":") - 2) + "";
            System.out.println("count = " + count);
            String parm = getParam1(event,count);

            if (parm.equals("true")) {
                System.out.println("json format ok.");
                flag = "true";
                proc.send(event, "key" + uuid, msg);

            } else if (parm.equals("false")){
                System.out.println("json format error.");
                flag = "false";
            }
        }else if (event.equals("useinfo")){
            System.out.println("enter userinfo.");
            Object jsonArray = jsonObject.get("properties");
//            System.out.println("data = " + jsonArray);

            String count = (checkCount(msg, ":") - 3) + "";
            System.out.println("count = " + count);
            String parm = getParam1(event,count);

            if (parm.equals("true")) {
                System.out.println("json format ok.");
                flag = "true";
                proc.send(event, "key" + uuid, msg);

            } else if (parm.equals("false")){
                System.out.println("json format error.");
                flag = "false";
            }
        }else if (event.equals("viewpage")){
            System.out.println("enter viewpage.");
            Object jsonArray = jsonObject.get("properties");
//            System.out.println("data = " + jsonArray);

            String count = (checkCount(msg, ":") - 3) + "";
            System.out.println("count = " + count);
            String parm = getParam1(event,count);

            if (parm.equals("true")) {
                System.out.println("json format ok.");
                flag = "true";
                proc.send(event, "key" + uuid, msg);

            } else if (parm.equals("false")){
                System.out.println("json format error.");
                flag = "false";
            }
        }else if (event.equals("inword")){
            System.out.println("enter inword.");
            Object jsonArray = jsonObject.get("properties");
//            System.out.println("data = " + jsonArray);

            String count = (checkCount(msg, ":") - 3) + "";
            System.out.println("count = " + count);
            String parm = getParam1(event,count);

            if (parm.equals("true")) {
                System.out.println("json format ok.");
                flag = "true";
                proc.send(event, "key" + uuid, msg);

            } else if (parm.equals("false")){
                System.out.println("json format error.");
                flag = "false";
            }
        }else if (event.equals("signup")){
            System.out.println("enter signup.");
            Object jsonArray = jsonObject.get("properties");
//            System.out.println("data = " + jsonArray);

            String count = (checkCount(msg, ":") - 3) + "";
            System.out.println("count = " + count);
            String parm = getParam1(event,count);

            if (parm.equals("true")) {
                System.out.println("json format ok.");
                flag = "true";
                proc.send(event, "key" + uuid, msg);

            } else if (parm.equals("false")){
                System.out.println("json format error.");
                flag = "false";
            }
        }else if (event.equals("investdetail")){
            System.out.println("enter investdetail.");
            Object jsonArray = jsonObject.get("properties");
//            System.out.println("data = " + jsonArray);

            String count = (checkCount(msg, ":") - 3) + "";
            System.out.println("count = " + count);
            String parm = getParam1(event,count);

            if (parm.equals("true")) {
                System.out.println("json format ok.");
                flag = "true";
                proc.send(event, "key" + uuid, msg);

            } else if (parm.equals("false")){
                System.out.println("json format error.");
                flag = "false";
            }
        }else if (event.equals("onclick")){
            System.out.println("enter onclick.");
            Object jsonArray = jsonObject.get("properties");
//            System.out.println("data = " + jsonArray);

            String count = (checkCount(msg, ":") - 3) + "";
            System.out.println("count = " + count);
            String parm = getParam1(event,count);

            if (parm.equals("true")) {
                System.out.println("json format ok.");
                flag = "true";
                proc.send(event, "key" + uuid, msg);

            } else if (parm.equals("false")){
                System.out.println("json format error.");
                flag = "false";
            }
        }else if (event.equals("crashdetail")){
            System.out.println("enter crashdetail.");
            Object jsonArray = jsonObject.get("properties");
//            System.out.println("data = " + jsonArray);

            String count = (checkCount(msg, ":") - 3) + "";
            System.out.println("count = " + count);
            String parm = getParam1(event,count);

            if (parm.equals("true")) {
                System.out.println("json format ok.");
                flag = "true";
                proc.send(event, "key" + uuid, msg);

            } else if (parm.equals("false")){
                System.out.println("json format error.");
                flag = "false";
            }
        }

        return flag;
    }


    public static int checkCount(String msg, String sub){
        int count = 0, start = 0;

        while((start=msg.indexOf(sub,start))>=0){
            start += sub.length();
            count ++;
        }
        return count;
    }

}