package com.weshare.bizconftable.controller;

import com.weshare.bizconftable.bean.PageBizConf;
import com.weshare.bizconftable.service.BizConfService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Created by mouzwang on 2021-01-19 17:31
 */
@Api
@Controller
public class BizConfController {

    @Autowired
    BizConfService bizConfService;

    /**
     * 将Hive高表展平成宽表
     * @return
     */
    @ApiOperation(value="获取所有配置")
    @GetMapping("getAllConf")
    @ResponseBody
    public List<PageBizConf> getAllConf() {
        List<PageBizConf> pageBizConfList = bizConfService.convertConf();
        return pageBizConfList;
    }

    /**
     * 插入多行新的配置数据
     * @param pageBizConfList
     * @return
     */
    @ApiOperation(value="插入新配置")
    @PostMapping ("insertConf")
    @ResponseBody
    public String insertConf(@ApiParam(name="pageBizConfList",value="新配置数据数组",required=true)
                                 @RequestBody List<PageBizConf> pageBizConfList) {
        if (pageBizConfList == null || (pageBizConfList != null && pageBizConfList.size() == 0)) {
            return "no data needs to insert,please check your operation";
        }
        bizConfService.insertConfRows(pageBizConfList);
        return "redirect:getAllConf";
    }

    /**
     * 更新单行数据.将传入的宽表转成高表
     * @param pageBizConf
     * @return
     */
    @ApiOperation("更新单行配置")
    @PostMapping("updateConf")
    public String updateConf(@ApiParam(name="pageBizConf",value="更新的配置数据",required=true)
                                 @RequestBody PageBizConf pageBizConf) {
        if (pageBizConf != null) {
            bizConfService.updateConfRow(pageBizConf);
        }
        return "redirect:getAllConf";
    }

    /**
     * 删除单行数据
     * @param pageBizConf
     * @return
     */
    @ApiOperation("删除单行配置")
    @PostMapping("deleteConf")
    public String deleteConf(@ApiParam(name="pageBizConf",value="要删除的配置数据",required=true)
                             @RequestBody PageBizConf pageBizConf) {
        if (pageBizConf != null) {
            bizConfService.deleteConfRow(pageBizConf);
        }
        return "redirect:getAllConf";
    }
}
