package org.leesia.dataio.service.impl;

import org.leesia.dataio.dao.IntDataMapper;
import org.leesia.dataio.domain.IntData;
import org.leesia.dataio.domain.IntDataCriteria;
import org.leesia.dataio.service.IntDataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class IntDataServiceImpl implements IntDataService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private IntDataMapper mapper;

    @Override
    public List<IntData> datas() {
        logger.info("查询整数");
        IntDataCriteria criteria = new IntDataCriteria();
        return mapper.selectByExample(criteria);
    }

    @Override
    public int insert(IntData data) {
        logger.info("整数入库，number：{}", data.getNumber());
        return mapper.insertSelective(data);
    }

    @Override
    public int batchInsert(List<IntData> datas) {
        logger.info("整数批量入库，size：{}", datas.size());
        return mapper.bacthInsert(datas);
    }
}
