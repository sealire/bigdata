package org.leesia.dataio.service.impl;

import org.leesia.dataio.dao.ExtIntDataMapper;
import org.leesia.dataio.dao.IntDataMapper;
import org.leesia.dataio.service.IntDataService;
import org.leesia.entity.IntData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class IntDataServiceImpl implements IntDataService {

    private static final Logger logger = LoggerFactory.getLogger(IntDataServiceImpl.class);

    @Autowired
    private IntDataMapper mapper;

    @Autowired
    private ExtIntDataMapper extMapper;

    @Override
    public int insert(IntData data) {
        logger.info("整数入库，number：{}", data.getNumber());
        return mapper.insertSelective(data);
    }

    @Override
    public int batchInsert(List<IntData> datas) {
        logger.info("整数批量入库，size：{}", datas.size());
        return extMapper.bacthInsert(datas);
    }

    @Override
    public List<IntData> bacthRead(int from, int to, Integer min, Integer max) {
        logger.info("整数批量读取，from：{}，to：{}，min：{}，max：{}", from, to, min, max);
        return extMapper.bacthRead(from, to, min, max);
    }
}
