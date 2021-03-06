[TOC]
# 回调接口
|              回调ip（生产）             |  回调ip（测试） |           备注          |
|-----------------------------------------|-----------------|-------------------------|
| https://uabs-server.weshareholdings.com | 10.83.0.69:8210 | 生产对应ip： 10.80.1.25 |

| 场景 |                       api                       |    参数    |    值    |
|------|-------------------------------------------------|------------|----------|
| 封包 | /uabs-core/callback/packageSuccessConfirm       | assetBagId | 包编号   |
| 解包 | /uabs-core/callback/unPackageSuccessConfirm     | assetBagId | 包编号   |
| 债转 | /uabs-core/callback/assetTransferSuccessConfirm | importId   | 导入编号 |

# 调脚本的场景和参数
|   场景   | row_type | 参数个数 |                         参数                        |
|----------|----------|----------|-----------------------------------------------------|
| 新增项目 | insert   |        1 | project_info@项目编号.json                          |
| 更新项目 | update   |        1 | project_info@项目编号.json                          |
| 债转新增 | insert   |        1 | project_due_bill_no@项目编号.json                   |
| 债转删除 | delete   |        1 | project_due_bill_no@项目编号.json                   |
| 解包     | delete   |        1 | bag_info@包编号.json                                |
| 包更新   | update   |        1 | bag_info@包编号.json                                |
| 封包     | insert   |        2 | bag_info@包编号.json<br>bag_due_bill_no@包编号.json |


# 资产包文件格式文档

## ProjectInfo 项目 新增/更新

|       字段名       |                    说明                   |
|--------------------|-------------------------------------------|
| row_type           | 操作类型(insert,update)                   |
| project_id         | 项目编号                                  |
| project_name       | 项目名称                                  |
| project_stage      | 项目阶段                                  |
| asset_side         | 资产方                                    |
| fund_side          | 资金方                                    |
| year               | 年份                                      |
| term               | 期数                                      |
| remarks            | 备注                                      |
| project_full_name  | 项目全名称                                |
| asset_type         | 资产类别（1：汽车贷，2：房贷，3：消费贷） |
| project_type       | 业务模式                                  |
| mode               | 模型归属                                  |
| project_time       | 立项时间                                  |
| project_begin_date | 项目开始时间                              |
| project_end_date   | 项目结束时间                              |
| asset_pool_type    | 资产池类型                                |
| public_offer       | 公募名称                                  |
| data_source        | 数据来源                                  |
| create_user        | 创建人                                    |
| create_time        | 创建时间                                  |
| update_time        | 更新时间                                  |

样例:

```json
{
  "fund_side": "云信",
  "project_type": "1",
  "row_type": "insert",
  "create_time": "2021-03-19 09:15:17",
  "year": "2021",
  "project_name": "富融云信-鉴权6",
  "data_source": "1",
  "project_end_date": "",
  "mode": "null",
  "asset_pool_type": "1",
  "update_time": "2021-03-19 09:15:17",
  "project_time": "2021-03-19",
  "project_id": "PL202103190076",
  "project_stage": "2",
  "asset_side": "富融",
  "project_begin_date": "",
  "project_full_name": "富融-云信-消费金融-2021年第6期-鉴权",
  "asset_type": "3",
  "term": "6",
  "create_user": "i_xujianyang",
  "remarks": "鉴权"
}
```

## ProjectDueBillNo 债转 新增

|   字段名    |                                            说明                                           |
|-------------|-------------------------------------------------------------------------------------------|
| import_id   | 导入id                                                                                    |
| row_type    | 操作类型(insert)                                                                          |
| project_id  | 项目编号                                                                                  |
| due_bill_no | JSON数组<br>serialNumber(借据号)<br>relatedDate(债转日)<br>relatedProjectId(债转前项目id) |

样例:

```json
{
  "import_id": "xxxx",
  "row_type": "insert",
  "project_id": "CL202012250044",
  "due_bill_no": [
    {
      "relatedDate": "2021-03-19",
      "relatedProjectId": "PL202103170072",
      "serialNumber": "444ff0f1afe934594b90b1b056ccf072f"
    }
  ]
}
```

## ProjectDueBillNo 债转 删除

|   字段名   |       说明       |
|------------|------------------|
| import_id  | 导入id           |
| row_type   | 操作类型(delete) |
| project_id | 项目编号         |

样例:

```json
{
  "import_id": "xxxx",
  "row_type": "delete",
  "project_id": "CL202012250044"
}
```

## BagInfo 封包/解包/更新

|        字段名        |                                说明                                |
|----------------------|--------------------------------------------------------------------|
| row_type             | 操作类型(insert/delete/update)                                     |
| project_id           | 项目编号                                                           |
| bag_id               | 包编号                                                             |
| bag_name             | 包名称                                                             |
| bag_status           | 包状态<br>insert：封包中<br>回调：已封包<br>update：改名字、转发行 |
| bag_remain_principal | 封包总本金余额                                                     |
| bag_date             | 封包日期                                                           |
| insert_date          | 封包操作日期                                                       |

样例:

```json
{
  "bag_id": "PL202103180073_1",
  "bag_name": "星云演示资产包_1",
  "row_type": "insert",
  "project_id": "PL202103180073",
  "insert_date": "2021-03-18",
  "bag_date": "2019-03-01",
  "bag_status": "已发行",
  "bag_remain_principal": 10000
}
```

## BagDueBillNo 封包

|   字段名    |                                                      说明                                                      |
|-------------|----------------------------------------------------------------------------------------------------------------|
| row_type    | 操作类型(insert)                                                                                               |
| project_id  | 项目编号                                                                                                       |
| bag_id      | 包编号                                                                                                         |
| due_bill_no | JSON数组<br>serialNumber(借据号)<br>packageRemainPeriods(封包剩余期限)<br>packageRemainPrincipal(封包剩余本金) |

样例:

```json
{
  "project_id": "CL202104250093",
  "bag_id": "PL202103190076_1",
  "row_type": "insert",
  "due_bill_no": [
    {
      "packageRemainPeriods": 3,
      "packageRemainPrincipal": 10000,
      "serialNumber": "444ff0f1afe934594b90b1b056ccf076f"
    }
  ]
}
```
