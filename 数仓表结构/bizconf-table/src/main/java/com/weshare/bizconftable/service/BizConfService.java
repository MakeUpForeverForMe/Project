package com.weshare.bizconftable.service;

import com.weshare.bizconftable.bean.PageBizConf;
import com.weshare.bizconftable.bean.SitBizConf;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by mouzwang on 2021-01-19 17:33
 */
@Service
public interface BizConfService {
    List<SitBizConf> getAllConf();

    List<PageBizConf> convertConf();

    void insertConfRows(List<PageBizConf> pageBizConfList);

    void updateConfRow(PageBizConf pageBizConf);

    void deleteConfRow(PageBizConf pageBizConf);
}
