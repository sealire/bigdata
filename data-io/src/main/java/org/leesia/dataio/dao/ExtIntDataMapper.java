package org.leesia.dataio.dao;

import org.apache.ibatis.annotations.Param;
import org.leesia.entity.IntData;

import java.util.List;

/**
 * @Auther: leesia
 * @Date: 2018/5/24 08:35
 * @Description:
 */
public interface ExtIntDataMapper {

    int bacthInsert(List<IntData> datas);

    List<IntData> bacthRead(@Param("from") int from, @Param("to") int to, @Param("min") Integer min, @Param("max") Integer max);
}
