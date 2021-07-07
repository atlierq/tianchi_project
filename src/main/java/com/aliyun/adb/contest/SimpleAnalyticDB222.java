package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;


import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class SimpleAnalyticDB222 implements AnalyticDB {
    int maxsize = 40*1024*1024;
    int DataNum;
    String workspaceDir;
    String[] columns;
    //文件起始位置
    long channelStartIndex = 20;

    int segment = 12;
    Map<String,long[]> map = new HashMap<>();
    private final int fileN = 1 << 7;

    int[][] bucketNums;
    //前缀和数组
    int[][] preBucketBums;
    /**
     *
     * The implementation must contain a public no-argument constructor.
     *
     */
    public SimpleAnalyticDB222() {
    }

    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {

        this.workspaceDir = workspaceDir;
        File dir = new File(tpchDataFileDir);

        for (File dataFile : dir.listFiles()) {
            System.out.println("Start loading table " + dataFile.getName());

            // You can write data to workspaceDir
            File yourDataFile = new File(workspaceDir, dataFile.getName());
            if (!yourDataFile.exists()) {
                yourDataFile.mkdirs();
            }
            loadInMemory(dataFile,yourDataFile);
        }

    }

    @Override
    public String quantile(String table, String column, double percentile) throws Exception {

//        List<Long> values = data.get(tableColumnKey(table, column));
//
//        if (values == null) {
//            throw new IllegalArgumentException();
//        }
        String bucketFileDir = "";
        //在桶里的位置
        int numInBucketIndex = 0;
        //找下在那个桶
        int bucketIndex=0;
        //在哪一列
        int colIndex=0;
        int find = (int) Math.round(DataNum* percentile);

        findOk:
        for(int i=0;i<columns.length;i++){
            if(columns[i].equals(column)){
                for(int j=0;j<preBucketBums[i].length;j++){
                    if(preBucketBums[i][j]>=find){
                        bucketFileDir=workspaceDir+File.separator+table+File.separator+File.separator+columns[i]+File.separator+j;
                        numInBucketIndex = bucketNums[i][j]-preBucketBums[i][j]+find;
                        bucketIndex = j;
                        colIndex = i;
                        break findOk;
                    }
                }
            }
        }
        //map机制
        Long res = 0L;
        if(map.containsKey(bucketFileDir)){
            res = map.get(bucketFileDir)[numInBucketIndex-1];
            System.out.println("used map");
        }else{
            long[] nums = new long[bucketNums[colIndex][bucketIndex]];

            for(File file:new File(bucketFileDir).listFiles()){
                FileChannel fc = new RandomAccessFile(file, "rw").getChannel();
                int fsize = (int)fc.size();
                ByteBuffer bf = ByteBuffer.allocate(fsize);
                fc.read(bf);
                bf.flip();
                for(int i=0;i<fsize/8;i++){
//                    System.out.println(bf.getLong());
                    nums[i] = bf.getLong();
//                    System.out.println(nums[i]);
                }
            }
            Arrays.sort(nums);
            map.put(bucketFileDir,nums);
            res = nums[numInBucketIndex-1];
        }

        System.out.println("Query:" + table + ", " + column + ", " + percentile + " Answer:" + find + ", " + res);

        return res.toString();
    }
    public static long bufferToLong(char[] charTmp, int charCount){
        long res = 0L;
        for (int i = 0; i < charCount; i++) {
            // res *= 10;
            res = (res << 3) + (res << 1);
            res += charTmp[i] - '0';
        }
        return res;
    }
    private List<long[]> getSegmentList(File dataFile) throws IOException {
        List<long[]> segmentList = new ArrayList<>();
        FileChannel channel = new RandomAccessFile(dataFile, "r").getChannel();
        int tmpBfSize = 40;
        ByteBuffer tmpBf = ByteBuffer.allocate(tmpBfSize);
        long segmentLength = (channel.size()-21+segment-1)/segment;
        for(int i=0;i<segment;i++){
            //对于最后一个seg的处理
            if(i==segment-1){
                segmentList.add(new long[]{channelStartIndex,channel.size()-1});
                break;
            }
            long channelEndIndex = segmentLength+channelStartIndex;
            channel.read(tmpBf,channelEndIndex);
            tmpBf.flip();

            //找换行符
            for(int bfindex=0;bfindex<tmpBfSize;bfindex++){
                if(tmpBf.get(bfindex)==(int)'\n'){
                    segmentList.add(new long[]{channelStartIndex,channelEndIndex+bfindex});
                    channelStartIndex = channelEndIndex+bfindex;
                    break;
                }
            }
            tmpBf.clear();
        }
        return segmentList;


    }
    private void loadInMemory(File dataFile,File workdir) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(dataFile));

        columns = reader.readLine().split(",");

        for (String column : columns) {
//            data.put(tableColumnKey(table, column), new ArrayList<Long>());
            for(int i=0;i<fileN;i++){
                File file = new File(workdir, column + File.separator + i);
                if (!file.exists()) {
                    file.mkdirs();
                }
            }
        }
        //bucketNums初始化。里面存的是每个桶的的数量
        bucketNums = new int[columns.length][fileN];
        //前缀和数组初始化
        preBucketBums = new int[columns.length][fileN];
//        File threadDataFile = new File(dataFile.toString());
        FileChannel channels = new RandomAccessFile(dataFile, "r").getChannel();
//        int mapSize = 397450;//
//        int mapDoneIndex = 20;
        FileChannel[][] fileChannels = new FileChannel[columns.length][fileN];
        ByteBuffer[][] byteBuffers = new ByteBuffer[columns.length][fileN];
        List<long[]> segmentList = getSegmentList(dataFile);

//        List<long[]> segmentList = new ArrayList<>();
//        segmentList.add(new long[]{20,397470});
//        397450
//        int mapDoneIndex = 20;
        long[] colTmp = new long[columns.length];
        char[] charTmp = new char[19];
        int charCount = 0;
        for(int indexS=0;indexS<segmentList.size();indexS++){
            System.out.println(Arrays.toString(segmentList.get(indexS)));
            long l = segmentList.get(indexS)[0];
            long r = segmentList.get(indexS)[1];
            while(l+1<r){
                int  mapBufferSize= 0;
                mapBufferSize = getSingleMapSize(channels,l, maxsize, segmentList.get(indexS)[1]);
                MappedByteBuffer mappedByteBuffer = channels.map(FileChannel.MapMode.READ_ONLY, l + 1, mapBufferSize);
                l +=mapBufferSize;
                while(mappedByteBuffer.hasRemaining()){
                    char c = (char) mappedByteBuffer.get();
                    if (c == '\u0000') break; // 文件结束
                    if (c == ',') {
                        colTmp[0] = bufferToLong(charTmp, charCount);
                        charCount = 0;
                    }else if(c == '\n'){
                        //计数
                        DataNum++;
                        colTmp[1] = bufferToLong(charTmp,charCount);
                        charCount = 0;
                        //
                        for(int i=0;i<colTmp.length;i++){
                            long num = colTmp[i];
                            int idx = (int)(num>>>56);
                            if(byteBuffers[i][idx] == null){
                                String bucketDataName= workdir+File.separator+columns[i]+File.separator+idx+File.separator+"longData";
                                fileChannels[i][idx]= new RandomAccessFile(new File(bucketDataName), "rw").getChannel();
                                byteBuffers[i][idx] = ByteBuffer.allocate(8 * 12880);
                            }
//                        System.out.println(num);
                            byteBuffers[i][idx].putLong(num);
                            //每个桶计数
                            bucketNums[i][idx]++;
                            if (byteBuffers[i][idx].remaining() == 0) {
                                byteBuffers[i][idx].flip();
                                try {
                                    fileChannels[i][idx].write(byteBuffers[i][idx]);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                                byteBuffers[i][idx].clear();
                            }
                        }

                    }else{
                        charTmp[charCount++] = c;
                    }

            }

//            long mapSize = segmentList.get(indexS)[1]-segmentList.get(indexS)[0];
//            System.out.println(mapSize);

            }


        }
        for (int i = 0; i < columns.length; i++) {
            for (int index = 0; index < byteBuffers[0].length; index++) {
                if (byteBuffers[i][index] != null) {
                    byteBuffers[i][index].flip();
                    try {
                        fileChannels[i][index].write(byteBuffers[i][index]);
                        fileChannels[i][index].close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        //前缀和数组计数//不用动
        for(int i=0;i<preBucketBums.length;i++){
            preBucketBums[i][0] = bucketNums[i][0];
            for(int j = 1;j<preBucketBums[0].length;j++){
                preBucketBums[i][j] = preBucketBums[i][j-1]+bucketNums[i][j];
            }
        }
        for(int i = 0;i<preBucketBums.length;i++){
            System.out.println(columns[i]+":Nums====>"+preBucketBums[i][fileN-1]);
        }

        System.out.println("load完毕");
    }


    public int getSingleMapSize(FileChannel fileChannel, long mapDoneIndex, int maxSize, long endReadIndex) throws IOException {
        if(mapDoneIndex + maxSize >= endReadIndex){
            return (int) (endReadIndex - mapDoneIndex);
        }
        int size = maxSize;
//        ByteBuffer byteBuffer = ByteBuffer.allocate(1);
//        fileChannel.read(byteBuffer, mapDoneIndex + size);
        ByteBuffer byteBuffer = ByteBuffer.allocate(40);
        fileChannel.read(byteBuffer, mapDoneIndex + size-40);
        byteBuffer.flip();
        for(int i=0;i<40;i++){
            if((char)byteBuffer.get(i)=='\n'){
                return size-40+i;

            }
        }

//        while ((char)byteBuffer.get() != '\n'){
//            size--;
//            byteBuffer.clear();
//            fileChannel.read(byteBuffer, mapDoneIndex + size);
//            byteBuffer.flip();
//        }
        return size;
    }

}
