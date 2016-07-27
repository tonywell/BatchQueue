package com.tds.queue;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by willtony on 16/7/21.
 */
public class BatchQueue<T> {

    private Logger logger = LoggerFactory.getLogger(BatchQueue.class);

    // 默认间隔处理队列时间
    private static int DEFAULT_TIME = 5000;

    // 默认队列处理长度
    private static int DEFAULT_COUNT = 2000;

    // 设置队列处理时间
    private long intervalTime;

    // 设置队列处理长度
    private int handleLength;

    // 阻塞队列
    ArrayBlockingQueue<T> queue = new ArrayBlockingQueue<T>(20000);

    // 回调接口
    private QueueProcess<T> process;

    // 用来存放从队列拿出的数据
    private List<T> dataList;

    // 往队列添加数据
    public void add(T t){
        queue.offer(t);
    }

    // 清理生成的list
    public void clearList(){
        dataList = null;
        dataList = new ArrayList<T>();
    }

    /**
     * 设置默认的队列处理时间和数量
     * @param process
     */
    public BatchQueue(QueueProcess<T> process){
        this(DEFAULT_TIME, DEFAULT_COUNT, process);
    }

    /**
     * 可以设置队列的处理的间隔时间和处理长度
     * @param intervalTime
     * @param handleQueueLength
     * @param process
     */
    public BatchQueue(int intervalTime, int handleQueueLength, QueueProcess<T> process){
        this.process = process;
        this.intervalTime = intervalTime;
        this.handleLength = handleQueueLength;
        start();
    }

    private void  start(){
        dataList = new ArrayList<T>(handleLength);
        DataListener listener = new  DataListener();
        new Thread(listener).start();
    }

    // 队列监听，当队列达到一定数量和时间后处理队列
    class DataListener implements Runnable{

        @Override
        public void run() {

            long startTime = System.currentTimeMillis();
            T t = null;
            while(true){
                try {
                    // 从队列拿出队列头部的元素
                    t = queue.poll();
                    if(null != t){
                        dataList.add(t);
                    }

                    if(dataList.size() >= handleLength){
                        logger.debug("list size: " + dataList.size());
                        startTime = callBack(dataList);
                        continue;
                    }

                    long currentTime = System.currentTimeMillis();
                    if(currentTime - startTime > intervalTime && dataList.size() > 0){
                        logger.debug("currentTime - startTime " + (currentTime - startTime) + " intervalTime==>" + intervalTime);
                        startTime = callBack(dataList);
                        continue;
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    e.printStackTrace();
                }

            }
        }

        private long callBack(List<T> dataList) {

            // 处理队列
            try{
                process.processData(dataList);
            }catch(Exception e){
                e.printStackTrace();
            }finally{
                // 清理掉dataList中的元素
                clearList();
            }


            return System.currentTimeMillis();
        }

    }
}
