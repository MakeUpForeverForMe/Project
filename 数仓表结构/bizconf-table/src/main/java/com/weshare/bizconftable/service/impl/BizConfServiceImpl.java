package com.weshare.bizconftable.service.impl;

import com.weshare.bizconftable.bean.PageBizConf;
import com.weshare.bizconftable.bean.SitBizConf;
import com.weshare.bizconftable.mapper.hive.BizConfMapper;
import com.weshare.bizconftable.service.BizConfService;
import com.weshare.bizconftable.utils.CustomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by mouzwang on 2021-01-19 17:34
 */
@Service
public class BizConfServiceImpl implements BizConfService {
    private static final Logger LOGGER = LoggerFactory.getLogger(BizConfServiceImpl.class);
    //存放sit_biz_conf表的col2_name与col2_comment映射关系
    private final static HashMap<String, String> col2NameValMap = new HashMap<>();

    @Autowired
    BizConfMapper bizConfMapper;

    @Override
    public List<SitBizConf> getAllConf() {
//        List<SitBizConf> bizConfs = bizConfMapper.selectAll();
        List<SitBizConf> bizConfs = bizConfMapper.getAllConf();
        return bizConfs;
    }

    @Override
    public List<PageBizConf> convertConf() {
        //现在只处理了看管的项目数据
        List<SitBizConf> bizConfs = bizConfMapper.getAllConf();
        ArrayList<PageBizConf> bizConfList = new ArrayList<PageBizConf>();
        PageBizConf pageBizConf = null;
        String lastValue = "";
        for (SitBizConf bizConf : bizConfs) {
            //拿到新的product时,初始化一个PageBizConf,更新lastValue
            if (!lastValue.equals(bizConf.getCol1Val())) {
                pageBizConf = new PageBizConf();
                bizConfList.add(pageBizConf);
                lastValue = bizConf.getCol1Val();
                if ("product_id".equals(bizConf.getCol1Name())) {
                    pageBizConf.setProductId(lastValue);
                    pageBizConf.setId(bizConf.getCol1Name().concat("-").concat(lastValue));
                }
            }
            pageBizConf = setAttribute(pageBizConf, bizConf);
        }
        return bizConfList;
    }

    @Override
    public void insertConfRows(List<PageBizConf> pageBizConfList) {
        ArrayList<SitBizConf> sitBizConfList = new ArrayList<>();
        for (PageBizConf pageBizConf : pageBizConfList) {
            packBizConf(pageBizConf, sitBizConfList);
        }
        bizConfMapper.insertConfRows(sitBizConfList);
    }

    @Override
    public void updateConfRow(PageBizConf pageBizConf) {
        ArrayList<SitBizConf> sitBizConfList = new ArrayList<>();
        packBizConf(pageBizConf, sitBizConfList);
        //先overwrite删除后再插入
        String col1Name = pageBizConf.getId().split("-")[0];
        String col1Val = pageBizConf.getId().split("-")[1];
        if(col1Name == null || col1Val == null) return;
        bizConfMapper.deleteOneConfRow(col1Name,col1Val);
        bizConfMapper.insertConfRows(sitBizConfList);
    }

    @Override
    public void deleteConfRow(PageBizConf pageBizConf) {
        //使用overwrite删除
        String col1Name = pageBizConf.getId().split("-")[0];
        String col1Val = pageBizConf.getId().split("-")[1];
        if(col1Name == null || col1Val == null) return;
        bizConfMapper.deleteOneConfRow(col1Name,col1Val);
    }


    /**
     * 将高表转为宽表
     * @param pageBizConf
     * @param bizConf
     * @return
     */
    private PageBizConf setAttribute(PageBizConf pageBizConf, SitBizConf bizConf) {
        String col2Name = bizConf.getCol2Name();
        String col2Val = bizConf.getCol2Val();
        switch (col2Name) {
            case "biz_name":
                pageBizConf.setBizName(col2Val);
                break;
            case "biz_name_en":
                pageBizConf.setBizNameEn(col2Val);
                break;
            case "capital_id":
                pageBizConf.setCapitalId(col2Val);
                break;
            case "capital_name":
                pageBizConf.setCapitalName(col2Val);
                break;
            case "capital_name_en":
                pageBizConf.setCapitalNameEn(col2Val);
                break;
            case "channel_id":
                pageBizConf.setChannelId(col2Val);
                break;
            case "channel_name":
                pageBizConf.setChannelName(col2Val);
                break;
            case "channel_name_en":
                pageBizConf.setChannelNameEn(col2Val);
                break;
            case "trust_id":
                pageBizConf.setTrustId(col2Val);
                break;
            case "trust_name":
                pageBizConf.setTrustName(col2Val);
                break;
            case "trust_name_en":
                pageBizConf.setProjectId(col2Val);
                break;
            case "project_id":
                pageBizConf.setProjectId(col2Val);
                break;
            case "project_name":
                pageBizConf.setProjectName(col2Val);
                break;
            case "project_name_en":
                pageBizConf.setProjectNameEn(col2Val);
                break;
            case "project_amount":
                pageBizConf.setProjectAmount(col2Val);
                break;
            case "product_id":
                pageBizConf.setProductId(col2Val);
                break;
            case "product_name":
                pageBizConf.setProductName(col2Val);
                break;
            case "product_name_en":
                pageBizConf.setProductNameEn(col2Val);
                break;
            case "product_id_vt":
                pageBizConf.setProductIdVt(col2Val);
                break;
            case "product_name_vt":
                pageBizConf.setProductNameVt(col2Val);
                break;
            case "product_name_en_vt":
                pageBizConf.setProductNameEnVt(col2Val);
                break;
        }
        return pageBizConf;
    }

    /**
     * 将宽表转为高表
     * @param pageBizConf
     * @param sitBizConfList
     */
    private void packBizConf(PageBizConf pageBizConf, ArrayList<SitBizConf> sitBizConfList) {
        //获取对象属性,但是要剔除id和product_id
        Field[] fields = pageBizConf.getClass().getDeclaredFields();
        for (int i = 1; i < fields.length; i++) {
            Field field = fields[i];
            //对于看管数据,跳过设置product_id
            if("productId".equals(field.getName())) continue;
            SitBizConf sitBizConf = new SitBizConf();
            String col1Name = pageBizConf.getId().split("-")[0];
            if (!"product_id".equals(col1Name) &&!"project_id".equals(col1Name) && !"bag_id".equals(col1Name)) {
                LOGGER.error("illegal col1Name : {}", col1Name);
                continue;
            }
            String col1Comment = "product_id".equals(col1Name) ? "产品编号" :
                    ("project_id".equals(col1Name) ? "项目编号" : "包编号");
            sitBizConf.setCol1Name(col1Name);
            sitBizConf.setCol1Comment(col1Comment);
            String col1Val = pageBizConf.getId().split("-")[1];
            sitBizConf.setCol1Val(col1Val);
            String col2Name = CustomStringUtils.humpToUnderline(field.getName());
            sitBizConf.setCol2Name(col2Name);
            sitBizConf.setCol2Comment(col2NameValMap.get(col2Name));
            try {
                field.setAccessible(true);
                String col2Val = (String) field.get(pageBizConf);
                if(col2Val == null) continue;
                sitBizConf.setCol2Val(col2Val);
                sitBizConf.setColId(col1Name.concat("@").concat(col1Val)
                        .concat("@").concat(col2Name)
                        .concat("@").concat(col2Val));
            } catch (IllegalAccessException e) {
                LOGGER.error("get field attribute by reflect error : ",e);
            }
            sitBizConfList.add(sitBizConf);
        }
    }

    static {
        col2NameValMap.put("biz_name", "业务名称(中文)");
        col2NameValMap.put("biz_name_en", "业务名称(英文)");
        col2NameValMap.put("capital_id", "资金方编号");
        col2NameValMap.put("capital_name", "资金方名称(中文)");
        col2NameValMap.put("capital_name_en", "资金方名称(英文)");
        col2NameValMap.put("channel_id", "渠道方编号");
        col2NameValMap.put("channel_name", "渠道方名称(中文)");
        col2NameValMap.put("channel_name_en", "渠道方名称（英文)");
        col2NameValMap.put("trust_id", "信托计划编号");
        col2NameValMap.put("trust_name", "信托计划名称(中文)");
        col2NameValMap.put("trust_name_en", "信托计划名称(英文)");
        col2NameValMap.put("project_id", "项目编号");
        col2NameValMap.put("project_name", "项目名称(中文)");
        col2NameValMap.put("project_name_en", "项目名称(英文)");
        col2NameValMap.put("product_id", "产品编号");
        col2NameValMap.put("project_amount", "项目初始金额");
        col2NameValMap.put("product_name", "产品名称(中文)");
        col2NameValMap.put("product_name_en", "产品名称(英文)");
        col2NameValMap.put("product_id_vt", "产品编号(虚拟)");
        col2NameValMap.put("product_name_vt", "产品名称(中文、虚拟)");
        col2NameValMap.put("product_name_en_vt", "产品名称(英文、虚拟)");
    }
}
