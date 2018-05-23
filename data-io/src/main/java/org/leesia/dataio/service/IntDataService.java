package org.leesia.dataio.service;

import org.leesia.dataio.domain.IntData;

import java.util.List;

public interface IntDataService {

    List<IntData> datas();

    int insert(IntData data);

    int batchInsert(List<IntData> datas);
}
