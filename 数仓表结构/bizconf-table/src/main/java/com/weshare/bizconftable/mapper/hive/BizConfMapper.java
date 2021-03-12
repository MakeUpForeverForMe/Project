package com.weshare.bizconftable.mapper.hive;


import com.weshare.bizconftable.bean.SitBizConf;
import org.apache.ibatis.annotations.Param;
import tk.mybatis.mapper.common.Mapper;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by mouzwang on 2021-01-19 17:32
 */
@org.apache.ibatis.annotations.Mapper

public interface BizConfMapper extends Mapper<SitBizConf> {
    List<SitBizConf> getAllConf();

    void insertConfRows(@Param("sitBizConfList") ArrayList<SitBizConf> sitBizConfList);

    void deleteOneConfRow(@Param("col1Name") String col1Name,@Param("col1Val") String col1Val);
}
