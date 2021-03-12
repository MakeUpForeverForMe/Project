package com.weshare.bizconftable.service;

import com.weshare.bizconftable.bean.WatchRule;

/**
 * created by chao.guo on 2021/2/1
 **/

public interface WatchRuleService {
    /**
     * 插入校验规则
     * @param watchRule
     * @return
     */
    int save(WatchRule watchRule);
}
