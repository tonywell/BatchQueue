package com.zhehekeji.queue;

import java.util.List;

/**
 * Created by willtony on 16/7/26.
 */
public interface QueueProcess <T> {

    void processData(List<T> list);
}
