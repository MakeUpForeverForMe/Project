package com.travel.dao;

import com.travel.entity.Active;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

/**
 * Created by angel
 */
@Mapper
public interface Active_mapper {
    List<Active> _active_searchContext();
}
