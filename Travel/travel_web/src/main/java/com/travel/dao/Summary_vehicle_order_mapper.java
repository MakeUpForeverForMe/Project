package com.travel.dao;

import com.travel.entity.Summary_vehicle_order_df;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

/**
 * Created by angel
 */
@Mapper
public interface Summary_vehicle_order_mapper {
    List<Summary_vehicle_order_df> vehicle_order_searchContext();
}
