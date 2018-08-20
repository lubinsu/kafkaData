package com.maiya.kafka.consumer.CheckParm;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by DELL on 2016/5/23.
 */
public class CheckTableParm {
    private static String param1;
    private static String param2;


    public static String getParam1(String event,String count) {
        Properties prop = new Properties();
        InputStream in = Object.class.getResourceAsStream("/test.properties");
        try {
            prop.load(in);
            param1 = prop.getProperty(event).trim();

            System.out.println(event + " :" + param1);

            if (event.equals("sysdata")){
                if (param1.equals(count)){
                    param1 = "true";
                }else {
                    param1 = "false";
                }
            }else if (event.equals("useinfo")){
                if (param1.equals(count)){
                    param1 = "true";
                }else {
                    param1 = "false";
                }
            }else if (event.equals("viewpage")){
                if (param1.equals(count)){
                    param1 = "true";
                }else {
                    param1 = "false";
                }
            }else if (event.equals("inword")){
                if (param1.equals(count)){
                    param1 = "true";
                }else {
                    param1 = "false";
                }
            }else if (event.equals("signup")){
                if (param1.equals(count)){
                    param1 = "true";
                }else {
                    param1 = "false";
                }
            }else if (event.equals("investdetail")){
                if (param1.equals(count)){
                    param1 = "true";
                }else {
                    param1 = "false";
                }
            }else if (event.equals("onclick")){
                if (param1.equals(count)){
                    param1 = "true";
                }else {
                    param1 = "false";
                }
            }else if (event.equals("crashdetail")){
                if (param1.equals(count)){
                    param1 = "true";
                }else {
                    param1 = "false";
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return param1;
    }

}
