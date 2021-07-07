package com.aliyun.adb.contest;

import java.io.File;

public class debug {
    public static void main(String[] args) {
        String dir = "/Users/tempo/Downloads/2021-tianchi-contest-1-master-5b605158c8bbb8ae5dffc77c4a3959a2c5f854422021-tianchi-contest-1.git 2/./target/L_ORDERKEY/19";
        File[] files = new File(dir).listFiles();
        System.out.println(files);
    }
}
