package com.weshare.bizconftable.controller;

import com.weshare.bizconftable.bean.WatchRule;
import com.weshare.bizconftable.service.WatchRuleService;
import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

/**
 * created by chao.guo on 2021/2/1
 **/
@Api
@Controller
public class WatchRuleController {
   @Autowired
   WatchRuleService watchRuleService;
   @PostMapping("save_rule")
   public String insertIntoWatchRule(@RequestBody WatchRule WatchRule){

       watchRuleService.save(WatchRule);
       return "";
   }




}
