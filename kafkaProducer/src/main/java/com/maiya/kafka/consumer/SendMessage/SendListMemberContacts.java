package com.maiya.kafka.consumer.SendMessage;

import com.google.gson.Gson;
import com.lamfire.logger.Logger;
import com.maiya.kafka.consumer.bean.MemberContacts;
import com.maiya.kafka.consumer.utils.ProducerUtil;
import org.json.JSONObject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by DELL on 2016/3/28.
 */

/*MemberContacts生产者数据写入kafka队列*/
public class SendListMemberContacts {

    static String sql = null;
    static DBHelper db = null;
    static ResultSet ret = null;
    private static final Logger logger = Logger.getLogger(SendListMemberContacts.class);
    public static void main(String[] args){
        //1. 从数据库获取MemberContacts这个表的信息,定义list<MemberContacts>结构
        List<MemberContacts> list = new ArrayList();

        sql = "select * from hy_membercontacts";
        db = new DBHelper(sql);

        try {
            ret = db.pst.executeQuery();
            while (ret.next()){
                MemberContacts contacts = new MemberContacts();
                contacts.setSguid(ret.getString(1));
                contacts.setSdeviceno(ret.getString(2));
                contacts.setSuserid(ret.getString(3));
                contacts.setSuserno(ret.getString(4));
                contacts.setSname(ret.getString(5));
                contacts.setStelphone(ret.getString(6));
                contacts.setSterminalchannel(ret.getString(7));
                contacts.setIdelflag(ret.getInt(8));
                contacts.setDadddate(ret.getString(9));

                list.add(contacts);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //2. 把输入转成json格式，输出出来
        Gson json = new Gson();
        String listjson = json.toJson(list);

        System.out.println(listjson);

        String table_status = "insert";

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("table_name","hy_membercontacts");
        jsonObject.put("table_status",table_status);

        jsonObject.put("data",listjson);

        //System.out.println(jsonObject.toString().replaceAll("\\\\",""));

        //String msg = jsonObject.toString().replaceAll("\\\\","");
        String msg = jsonObject.toString();
        System.out.println(msg);
        String uuid = UUID.randomUUID().toString().replaceAll("-", "");

        System.out.println(msg);
        //3. 把json格式数据写入Kafka队列中
        ProducerUtil pro = new ProducerUtil();
        pro.send("hy_membercontacts", "key" + uuid, msg);
        logger.debug(msg);

    }
}
