package com.aliyun.adb.contest;

import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class TestSimpleAnalyticDB {

    @Test
    public void testCorrectness() throws Exception {
        File testDataDir = new File("./test_data1");
        File testWorkspaceDir = new File("./target");
        File testResultsFile = new File("./test_result/results");
        SimpleAnalyticDB analyticDB = new SimpleAnalyticDB();

        // Step #1: load data
        long s = System.currentTimeMillis();
        analyticDB.load(testDataDir.getAbsolutePath(), testWorkspaceDir.getAbsolutePath());
        long e = System.currentTimeMillis();
        System.out.println(e-s);
        // Step #2: test quantile function
        try (BufferedReader resReader = new BufferedReader(new FileReader(testResultsFile))) {
            String line;

            while ((line = resReader.readLine()) != null) {
                String resultStr[] = line.split(" ");
                String table = resultStr[0];
                String column = resultStr[1];
                double percentile = Double.valueOf(resultStr[2]);
                String answer = resultStr[3];

                Assert.assertEquals(answer, analyticDB.quantile(table, column, percentile));
            }
        }
    }

}
