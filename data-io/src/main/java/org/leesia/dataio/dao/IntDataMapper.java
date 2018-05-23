package org.leesia.dataio.dao;

import java.util.List;

import org.apache.ibatis.annotations.Param;
import org.leesia.dataio.domain.IntData;
import org.leesia.dataio.domain.IntDataCriteria;

public interface IntDataMapper {
    long countByExample(IntDataCriteria example);

    int deleteByExample(IntDataCriteria example);

    int deleteByPrimaryKey(Integer id);

    int insert(IntData record);

    int insertSelective(IntData record);

    List<IntData> selectByExample(IntDataCriteria example);

    IntData selectByPrimaryKey(Integer id);

    int updateByExampleSelective(@Param("record") IntData record, @Param("example") IntDataCriteria example);

    int updateByExample(@Param("record") IntData record, @Param("example") IntDataCriteria example);

    int updateByPrimaryKeySelective(IntData record);

    int updateByPrimaryKey(IntData record);

    int bacthInsert(List<IntData> datas);
}