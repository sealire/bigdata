package org.leesia.dataio.service;

import org.leesia.entity.IntData;

import java.util.List;

public interface IntDataService {

    /**
     * 整数入库
     * @param data
     * @return
     */
    int insert(IntData data);

    /**
     * 整数批量入库
     * @param datas
     * @return
     */
    int batchInsert(List<IntData> datas);

    /**
     * 整数批量读取
     * @param from
     * @param to
     * @param min
     * @param max
     * @return
     */
    List<IntData> bacthRead(int from, int to, Integer min, Integer max);
}
