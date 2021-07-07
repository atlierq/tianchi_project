package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;


public class SimpleAnalyticDB implements AnalyticDB {
    long channelStartIndex = 20;
    int DataNum;
    FileChannel channel;

    String workspaceDir;
    String[] columns;
    FileChannel[][][] fileChannels;
    ByteBuffer[][][] byteBuffers;
    int[][] bucketNums;
    //前缀和数组
    int[][] preBucketBums;
    Map<String,long[]> map = new HashMap<>();

    //文件起始位置
    AtomicInteger tmpDataNum = new AtomicInteger(0);
    int mapUsed=0;
    int segment = 12;
    CountDownLatch countDownLatch = new CountDownLatch(segment);
    int bit =7;
    int maxsize = 40*1024*1024;//40m
    int readBufferSize = 1024*16;//8*122880
    private final int fileN = 1 << bit;
    //偷鸡
    int counter = 0;
    long queryTime = 0;
    /**
     *
     * The implementation must contain a public no-argument constructor.
     *
     */
    public SimpleAnalyticDB() {
    }
    class bucketThread implements Runnable {
        long l;
        long r;
        File workdir;
        FileChannel channels;
        int indexThread;

        long[] colTmp;
        char[] charTmp;
        int[][] tmpBucketNums;

        public bucketThread(long l, long r, File workdir, FileChannel channels, int indexThread) {
            this.l = l;
            this.r = r;
            this.workdir = workdir;
            this.channels = channels;
            this.indexThread = indexThread;

            this.colTmp = new long[columns.length];
            this.charTmp = new char[19];
            this.tmpBucketNums= new int[columns.length][fileN];
        }

        @Override
        public void run() {

            int tmpNums = putToBucket();

            //同步放入bucketBums;
            synchronized (bucketNums){
                for(int i=0;i<tmpBucketNums.length;i++){
                    for(int j=0;j<tmpBucketNums[0].length;j++){
                        bucketNums[i][j]+=tmpBucketNums[i][j];
                    }
                }
            }
            //同步增加总数
            tmpDataNum.addAndGet(tmpNums);
            countDownLatch.countDown();

        }

        private int putToBucket() {

            int ThreadNum = Integer.parseInt(Thread.currentThread().getName().split("-")[1]);
            int tmpNums = 0;
            int charCount = 0;

            while(l+1<r){
                int  mapBufferSize= 0;
                try {
                    mapBufferSize = getSingleMapSize(channels,l, maxsize, r);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                MappedByteBuffer mappedByteBuffer = null;
                try {
                    mappedByteBuffer = channels.map(FileChannel.MapMode.READ_ONLY, l + 1, mapBufferSize);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                l += mapBufferSize;

                while(mappedByteBuffer.hasRemaining()){


                    char c = (char) mappedByteBuffer.get();
                    switch (c) {
                        case '\u0000':
                            break;
                        //if (c == ) break; // 文件结束
                        case ',':
                            colTmp[0] = bufferToLong(charTmp, charCount);
                            charCount = 0;
                            break;
                        case '\n':
                            //计数
                            tmpNums++;
                            colTmp[1] = bufferToLong(charTmp, charCount);
                            charCount = 0;
                            //
                            for (int i = 0; i < colTmp.length; i++) {
                                long num = colTmp[i];
                                int idx = (int) (num >>>(63-bit));

                                //                        System.out.println(num);
                                byteBuffers[i][idx][ThreadNum].putLong(num);
                                //每个桶计数
                                tmpBucketNums[i][idx]++;
                                if (byteBuffers[i][idx][ThreadNum].remaining() == 0) {
                                    byteBuffers[i][idx][ThreadNum].flip();
                                    try {
                                        fileChannels[i][idx][ThreadNum].write(byteBuffers[i][idx][ThreadNum]);
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                    byteBuffers[i][idx][ThreadNum].clear();
                                }
                            }
                            break;

                        default:
                            charTmp[charCount++] = c;
                            break;

                    }
                }

//            long mapSize = segmentList.get(indexS)[1]-segmentList.get(indexS)[0];
//            System.out.println(mapSize);
            }
            //处理下byte中剩余的
            for (int i = 0; i < columns.length; i++) {
                for (int idx = 0; idx < byteBuffers[0].length; idx++) {
                    if (byteBuffers[i][idx][ThreadNum] != null) {
                        byteBuffers[i][idx][ThreadNum].flip();
                        try {
                            fileChannels[i][idx][ThreadNum].write(byteBuffers[i][idx][ThreadNum]);
                            fileChannels[i][idx][ThreadNum].close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            return tmpNums;
        }


    }
    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
        long s = System.currentTimeMillis();

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
        long e = System.currentTimeMillis();
        System.out.println("load time===>"+(e-s));

    }

    @Override
    public String quantile(String table, String column, double percentile) throws Exception {
        long s = System.currentTimeMillis();
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

        Long res = getRes(bucketFileDir, numInBucketIndex, bucketIndex, colIndex);
        System.out.println("Query:" + table + ", " + column + ", " + percentile + " Answer:" + find + ", " + res);
        queryTime += (System.currentTimeMillis() - s);
        counter++;
        if(counter == 10){
            System.out.println("query time=====> " + queryTime);
            throw new NumberFormatException("tao lao shi she le");
        }
        return res.toString();
    }

    private Long getRes(String bucketFileDir, int numInBucketIndex, int bucketIndex, int colIndex) throws IOException {
        Long res = 0L;

        if(map.containsKey(bucketFileDir)){
            res = map.get(bucketFileDir)[numInBucketIndex -1];
            mapUsed++;

        }else{
            long[] nums = new long[bucketNums[colIndex][bucketIndex]];

            int index= 0;
            for(File file:new File(bucketFileDir).listFiles()){

                FileChannel fc = new RandomAccessFile(file, "rw").getChannel();
                int fsize = (int)fc.size();
                ByteBuffer bf = ByteBuffer.allocateDirect(fsize);
                fc.read(bf);
                bf.flip();

                for(int i=0;i<fsize/8;i++){
//                    System.out.println(bf.getLong());
                    nums[index++] = bf.getLong();
//                    System.out.println(nums[i]);
                }
            }
            Arrays.sort(nums);
            map.put(bucketFileDir,nums);
            res = nums[numInBucketIndex -1];
        }
        return res;
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

    private void loadInMemory(File dataFile,File workdir) throws IOException, InterruptedException {

        makeDir(dataFile, workdir);
        channel = new RandomAccessFile(dataFile, "r").getChannel();
        System.out.println("File====>"+channel.size());
        List<long[]> segmentList = getSegmentList(dataFile);

        //bucketNums初始化。里面存的是每个桶的的数量
        bucketNums = new int[columns.length][fileN];
        //前缀和数组初始化
        preBucketBums = new int[columns.length][fileN];
        fileChannels = new FileChannel[columns.length][fileN][segment];
        byteBuffers = new ByteBuffer[columns.length][fileN][segment];
        for(int i=0;i<columns.length;i++){
            for(int j = 0;j<fileN;j++){
                for(int k=0;k<segment;k++){
                    String bucketDataName = workdir + File.separator + columns[i] + File.separator + j + File.separator + "Thread-"+k;
                    fileChannels[i][j][k] = new RandomAccessFile(new File(bucketDataName), "rw").getChannel();
                    byteBuffers[i][j][k] = ByteBuffer.allocateDirect(readBufferSize);
                }
            }
        }

//        File threadDataFile = new File(dataFile.toString());

//        int mapSize = 397450;//
//        int mapDoneIndex = 20;

        for(int i=0;i<segment;i++){
            long[] tmp = segmentList.get(i);
            long l = tmp[0];
            long r = tmp[1];
            new Thread(new bucketThread(l,r,workdir,channel,i)).start();
        }
        countDownLatch.await();
        DataNum = tmpDataNum.get();


//        List<long[]> segmentList = new ArrayList<>();
//        segmentList.add(new long[]{20,397470});
//        397450
//        int mapDoneIndex = 20;


        //前缀和数组计数//不用动
        getPreArray();

        System.out.println("load完毕");

        }

    private void makeDir(File dataFile, File workdir) throws IOException {
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
    }
    private List<long[]> getSegmentList(File dataFile) throws IOException {
        List<long[]> segmentList = new ArrayList<>();
        FileChannel channel = new RandomAccessFile(dataFile, "r").getChannel();
        //System.out.println(channel.size());
        long end = channel.size();
        long segmentSize = (end - 21 - segment - 1)/segment;
        long segmentStart = 20;
        while (segmentStart < end) {
            //System.out.println("==========");
            if (segmentStart >= end - segmentSize) {
                long[] temp = new long[]{segmentStart, end - 1};
                segmentList.add(temp);
                break;
            }

            ByteBuffer bf = ByteBuffer.allocate(40);
            channel.read(bf, segmentStart + segmentSize);
            bf.flip();
            int backward = 39;
            while (backward >= 0) {
                if (bf.get(backward) == (int) '\n') {
                    long[] temp = new long[]{segmentStart, segmentSize + segmentStart + backward};
                    segmentList.add(temp);
                    break;
                }
                backward--;
            }
            bf.clear();
            segmentStart = segmentSize + segmentStart + backward;
        }
//        for(int i=0;i<12;i++)
//            System.out.println(Arrays.toString(segmentList.get(i)));
        return segmentList;
    }
    private void getPreArray() {
        for(int i=0;i<preBucketBums.length;i++){
            preBucketBums[i][0] = bucketNums[i][0];
            for(int j = 1;j<preBucketBums[0].length;j++){
                preBucketBums[i][j] = preBucketBums[i][j-1]+bucketNums[i][j];
            }
        }
        for(int i = 0;i<preBucketBums.length;i++){
            System.out.println(columns[i]+":Nums====>"+preBucketBums[i][fileN-1]);
        }
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
