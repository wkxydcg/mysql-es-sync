package com.wosai.mysql.es.sync.util;

import org.apache.commons.lang3.StringUtils;

import java.io.*;

public class BinLogInfoUtil {

    private static final String FILE_NAME_KEY = "binLogName";

    private static final String POS_KEY = "binLogPos";

    public static void updateBinLogFileName(String fileName, String name){
        File file = createFileIfAbsent(fileName);
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String content = reader.readLine();
            String updateContent;
            if(StringUtils.isNotEmpty(content)){
                String []contents = content.split(":");
                if(contents.length > 1){
                    updateContent = name + ":" +contents[1];
                }else{
                    updateContent = name + ":";
                }
            }else{
                updateContent = name + ":";
            }
            BufferedWriter writer =new BufferedWriter(new FileWriter(file));
            writer.write(updateContent,0,updateContent.length());
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void updateBinLogPos(String fileName,Long pos){
        File file = createFileIfAbsent(fileName);
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String content = reader.readLine();
            String updateContent;
            if(StringUtils.isNotEmpty(content)){
                String []contents = content.split(":");
                if(contents.length > 1){
                    updateContent = contents[0] + ":" + String.valueOf(pos);
                }else{
                    updateContent = ":" + String.valueOf(pos);
                }
            }else{
                updateContent = ":" + String.valueOf(pos);
            }
            BufferedWriter writer =new BufferedWriter(new FileWriter(file));
            writer.write(updateContent,0,updateContent.length());
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getBinLogFileName(String fileName){
        File file = new File(fileName);
        if(!file.exists()) return null;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String content = reader.readLine();
            if(StringUtils.isEmpty(content)) return null;
            String [] contents = content.split(":");
            if(content.length() >= 1){
                return contents[0];
            }
            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Long getBinLogPos(String fileName){
        File file = new File(fileName);
        if(!file.exists()) return null;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String content = reader.readLine();
            if(StringUtils.isEmpty(content)) return null;
            String [] contents = content.split(":");
            if(content.length() >= 2){
                return Long.parseLong(contents[1]);
            }
            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private static File createFileIfAbsent(String fileName){
        File file =new File(fileName);
        if(!file.exists()){
            try {
                if(!file.createNewFile()){
                    throw new RuntimeException("文件创建失败");
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return file;
    }
}
