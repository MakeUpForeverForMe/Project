t_asset_package 为快照表 记录对应封包资产的快照
<!-- 未尝本金余额分布  -->
    <select id="getPrincipalBalanceBagDistribute" resultType="java.util.Map">
        SELECT tbl.package_remain_principal_region AS loanDim,
        ROUND(SUM(package_remain_principal),2) AS PrincipalAmount,
        ROUND(SUM(package_remain_principal)/tbl2.package_remain_principals*100,2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(COUNT(1)/tbl2.fcontract_count*100,2) AS loanNumRatio,
        ROUND(SUM(package_remain_principal)/COUNT(1),2) AS avgAmount
        FROM(
        SELECT
        package_remain_principal,
        CASE
        <![CDATA[
				WHEN   (package_remain_principal>10000000) THEN '1000万元（不含）以上'
				WHEN   (package_remain_principal>9500000 AND package_remain_principal<=10000000) THEN '950万元（不含）-1000万元（含）'
				WHEN   (package_remain_principal>9000000 AND package_remain_principal<=9500000) THEN '900万元（不含）-950万元（含）'
				WHEN   (package_remain_principal>8500000 AND package_remain_principal<=9000000) THEN '850万元（不含）-900万元（含）'
				WHEN   (package_remain_principal>8000000 AND package_remain_principal<=8500000) THEN '800万元（不含）-850万元（含）'
				WHEN   (package_remain_principal>7500000 AND package_remain_principal<=8000000) THEN '750万元（不含）-800万元（含）'
				WHEN   (package_remain_principal>7000000 AND package_remain_principal<=7500000) THEN '700万元（不含）-750万元（含）'
				WHEN   (package_remain_principal>6500000 AND package_remain_principal<=7000000) THEN '650万元（不含）-700万元（含）'
				WHEN   (package_remain_principal>6000000 AND package_remain_principal<=6500000) THEN '600万元（不含）-650万元（含）'
				WHEN   (package_remain_principal>5500000 AND package_remain_principal<=6000000) THEN '550万元（不含）-600万元（含）'
				WHEN   (package_remain_principal>5000000 AND package_remain_principal<=5500000) THEN '500万元（不含）-550万元（含）'
				WHEN   (package_remain_principal>4500000 AND package_remain_principal<=5000000) THEN '450万元（不含）-500万元（含）'
				WHEN   (package_remain_principal>4000000 AND package_remain_principal<=4500000) THEN '400万元（不含）-450万元（含）'
				WHEN   (package_remain_principal>3500000 AND package_remain_principal<=4000000) THEN '350万元（不含）-400万元（含）'
				WHEN   (package_remain_principal>3000000 AND package_remain_principal<=3500000) THEN '300万元（不含）-350万元（含）'
				WHEN   (package_remain_principal>2500000 AND package_remain_principal<=3000000) THEN '250万元（不含）-300万元（含）'
				WHEN   (package_remain_principal>2000000 AND package_remain_principal<=2500000) THEN '200万元（不含）-250万元（含）'
				WHEN   (package_remain_principal>1500000 AND package_remain_principal<=2000000) THEN '150万元（不含）-200万元（含）'
				WHEN   (package_remain_principal>1000000 AND package_remain_principal<=1500000) THEN '100万元（不含）-150万元（含）'
				WHEN   (package_remain_principal>500000 AND package_remain_principal<=1000000) THEN '50万元（不含）-100万元（含）'
				WHEN   (package_remain_principal>400000 AND package_remain_principal<=500000) THEN '40万元（不含）-50万元（含）'
				WHEN   (package_remain_principal>300000 AND package_remain_principal<=400000) THEN '30万元（不含）-40万元（含）'
				WHEN   (package_remain_principal>200000 AND package_remain_principal<=300000) THEN '20万元（不含）-30万元（含）'
				WHEN   (package_remain_principal>100000 AND package_remain_principal<=200000) THEN '10万元（不含）-20万元（含）'
				WHEN   (package_remain_principal>50000 AND package_remain_principal<=100000) THEN '5万元（不含）-10万元（含）'
				WHEN   (package_remain_principal<=50000) THEN '5万元（含）以下'
				ELSE    '其他区间'
		        END  AS package_remain_principal_region ]]>,
        CASE
        <![CDATA[
				WHEN   (package_remain_principal>10000000) THEN 26
				WHEN   (package_remain_principal>9500000 AND package_remain_principal<=10000000) THEN 25
				WHEN   (package_remain_principal>9000000 AND package_remain_principal<=9500000) THEN 24
				WHEN   (package_remain_principal>8500000 AND package_remain_principal<=9000000) THEN 23
				WHEN   (package_remain_principal>8000000 AND package_remain_principal<=8500000) THEN 22
				WHEN   (package_remain_principal>7500000 AND package_remain_principal<=8000000) THEN 21
				WHEN   (package_remain_principal>7000000 AND package_remain_principal<=7500000) THEN 20
				WHEN   (package_remain_principal>6500000 AND package_remain_principal<=7000000) THEN 19
				WHEN   (package_remain_principal>6000000 AND package_remain_principal<=6500000) THEN 18
				WHEN   (package_remain_principal>5500000 AND package_remain_principal<=6000000) THEN 17
				WHEN   (package_remain_principal>5000000 AND package_remain_principal<=5500000) THEN 16
				WHEN   (package_remain_principal>4500000 AND package_remain_principal<=5000000) THEN 15
				WHEN   (package_remain_principal>4000000 AND package_remain_principal<=4500000) THEN 14
				WHEN   (package_remain_principal>3500000 AND package_remain_principal<=4000000) THEN 13
				WHEN   (package_remain_principal>3000000 AND package_remain_principal<=3500000) THEN 12
				WHEN   (package_remain_principal>2500000 AND package_remain_principal<=3000000) THEN 11
				WHEN   (package_remain_principal>2000000 AND package_remain_principal<=2500000) THEN 10
				WHEN   (package_remain_principal>1500000 AND package_remain_principal<=2000000) THEN 9
				WHEN   (package_remain_principal>1000000 AND package_remain_principal<=1500000) THEN 8
				WHEN   (package_remain_principal>500000 AND package_remain_principal<=1000000) THEN 7
				WHEN   (package_remain_principal>400000 AND package_remain_principal<=500000) THEN 6
				WHEN   (package_remain_principal>300000 AND package_remain_principal<=400000) THEN 5
				WHEN   (package_remain_principal>200000 AND package_remain_principal<=300000) THEN 4
				WHEN   (package_remain_principal>100000 AND package_remain_principal<=200000) THEN 3
				WHEN   (package_remain_principal>50000 AND package_remain_principal<=100000) THEN 2
				WHEN   (package_remain_principal<=50000) THEN 1
				ELSE    27
		        END  AS orderSeq ]]>, project_id, asset_bag_id
        FROM
        t_asset_package
        WHERE project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        ) tbl
        INNER JOIN (SELECT max(project_id) project_id, max(asset_bag_id) asset_bag_id, COUNT(*) AS
        fcontract_count,SUM(package_remain_principal) AS
        package_remain_principals FROM t_asset_package
        WHERE
        project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        )tbl2
        on tbl.project_id = tbl2.project_id and tbl.asset_bag_id = tbl2.asset_bag_id
        GROUP BY tbl.package_remain_principal_region, tbl.orderSeq
        order by tbl.orderSeq
    </select>

    <!-- 资产利率分布 -->
    <select id="getLoanRateBagDistribute" resultType="java.util.Map">
        SELECT tbl.package_remain_principal_region AS loanDim,
        ROUND(SUM(package_remain_principal),2) AS PrincipalAmount,
        ROUND(SUM(package_remain_principal)/tbl2.package_remain_principals*100,2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(COUNT(1)/tbl2.fcontract_count*100,2) AS loanNumRatio,
        ROUND(SUM(package_remain_principal)/COUNT(1),2) AS avgAmount
        FROM(
        SELECT
        package_remain_principal,
        CASE
        <![CDATA[
				WHEN   (contract_interest_rate>20) THEN '20%(不含)以上'
				WHEN   (contract_interest_rate>15 AND contract_interest_rate<=20) THEN '15%（不含）-20%（含）'
				WHEN   (contract_interest_rate>10 AND contract_interest_rate<=15) THEN '10%（不含）-15%（含）'
				WHEN   (contract_interest_rate>5 AND contract_interest_rate<=10) THEN '5%（不含）-10%（含）'
				WHEN   (contract_interest_rate<=5) THEN '5%（含）以下'
				ELSE    '未知利率'
		        END  AS package_remain_principal_region ]]>,
        CASE
        <![CDATA[
				WHEN   (contract_interest_rate>20) THEN 5
				WHEN   (contract_interest_rate>15 AND contract_interest_rate<=20) THEN 4
				WHEN   (contract_interest_rate>10 AND contract_interest_rate<=15) THEN 3
				WHEN   (contract_interest_rate>5 AND contract_interest_rate<=10) THEN 2
				WHEN   (contract_interest_rate<=5) THEN 1
				ELSE    6
		        END  AS orderSeq ]]>, project_id, asset_bag_id
        FROM
        t_asset_package
        WHERE project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        ) tbl
        INNER JOIN (SELECT max(project_id) project_id, max(asset_bag_id) asset_bag_id,COUNT(*) AS
        fcontract_count,SUM(package_remain_principal) AS
        package_remain_principals FROM t_asset_package
        WHERE
        project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        )tbl2
        on tbl.project_id = tbl2.project_id and tbl.asset_bag_id = tbl2.asset_bag_id
        GROUP BY tbl.package_remain_principal_region, tbl.orderSeq
        order by tbl.orderSeq
    </select>

    <!-- 资产合同期数分布 -->
    <select id="getContractMaturityDistribute" resultType="java.util.Map">
        SELECT tbl.periods AS loanDim,
        ROUND(SUM(package_remain_principal),2) AS PrincipalAmount,
        ROUND(SUM(package_remain_principal)/tbl2.package_remain_principals*100,2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(COUNT(1)/tbl2.fcontract_count*100,2) AS loanNumRatio,
        ROUND(SUM(package_remain_principal)/COUNT(1),2) AS avgAmount
        FROM(
        SELECT
        package_remain_principal,periods, project_id, asset_bag_id
        FROM
        t_asset_package
        WHERE project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        ) tbl
        INNER JOIN (SELECT max(project_id) project_id, max(asset_bag_id) asset_bag_id, COUNT(*) AS
        fcontract_count,SUM(package_remain_principal) AS
        package_remain_principals FROM t_asset_package
        WHERE
        project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        )tbl2
        on tbl.project_id = tbl2.project_id and tbl.asset_bag_id = tbl2.asset_bag_id
        GROUP BY tbl.periods
        order by tbl.periods
    </select>

    <!-- 剩余期数分布 -->
    <select id="getResidualMaturityDistribute" resultType="java.util.Map">
        SELECT tbl.package_remain_periods AS loanDim,
        ROUND(SUM(package_remain_principal),2) AS PrincipalAmount,
        ROUND(SUM(package_remain_principal)/tbl2.package_remain_principals*100,2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(COUNT(1)/tbl2.fcontract_count*100,2) AS loanNumRatio,
        ROUND(SUM(package_remain_principal)/COUNT(1),2) AS avgAmount
        FROM(
        SELECT
        package_remain_principal,
        CASE
        <![CDATA[
				WHEN   (package_remain_periods>=1 AND package_remain_periods<=6) THEN '1期（含）-6期（含）'
				WHEN   (package_remain_periods>6 AND package_remain_periods<=12) THEN '6期（不含）-12期（含）'
				WHEN   (package_remain_periods>12 AND package_remain_periods<=18) THEN '12期（不含）-18期（含）'
				WHEN   (package_remain_periods>18 AND package_remain_periods<=24) THEN '18期（不含）-24期（含）'
				WHEN   (package_remain_periods>24 AND package_remain_periods<=30) THEN '24期（不含）-30期（含）'
				WHEN   (package_remain_periods>30 AND package_remain_periods<=36) THEN '30期（不含）-36期（含）'
				WHEN   (package_remain_periods>36 ) THEN '大于36期'
				ELSE    '0期'
		        END  AS package_remain_periods ]]>,
        CASE
        <![CDATA[
				WHEN   (package_remain_periods>=1 AND package_remain_periods<=6) THEN 2
				WHEN   (package_remain_periods>6 AND package_remain_periods<=12) THEN 3
				WHEN   (package_remain_periods>12 AND package_remain_periods<=18) THEN 4
				WHEN   (package_remain_periods>18 AND package_remain_periods<=24) THEN 5
				WHEN   (package_remain_periods>24 AND package_remain_periods<=30) THEN 6
				WHEN   (package_remain_periods>30 AND package_remain_periods<=36) THEN 7
				WHEN   (package_remain_periods>36 ) THEN 8
				ELSE    1
		        END  AS orderSeq ]]>, project_id, asset_bag_id
        FROM
        t_asset_package
        WHERE project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        ) tbl
        INNER JOIN (SELECT  max(project_id) project_id, max(asset_bag_id) asset_bag_id, COUNT(*) AS
        fcontract_count,SUM(package_remain_principal) AS
        package_remain_principals FROM t_asset_package
        WHERE
        project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        )tbl2
        on tbl.project_id = tbl2.project_id and tbl.asset_bag_id = tbl2.asset_bag_id
        GROUP BY tbl.package_remain_periods
        order by tbl.package_remain_periods
    </select>

    <!-- 已还期数分布 -->
    <select id="getYetMaturityDistribute" resultType="java.util.Map">
        SELECT tbl.yet_periods AS loanDim,
        ROUND(SUM(package_remain_principal),2) AS PrincipalAmount,
        ROUND(SUM(package_remain_principal)/tbl2.package_remain_principals*100,2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(COUNT(1)/tbl2.fcontract_count*100,2) AS loanNumRatio,
        ROUND(SUM(package_remain_principal)/COUNT(1),2) AS avgAmount
        FROM(
        SELECT
        package_remain_principal,
        CASE
        <![CDATA[
				WHEN   (periods-package_remain_periods>=1 AND periods-package_remain_periods<=6) THEN '1期（含）-6期（含）'
				WHEN   (periods-package_remain_periods>6 AND periods-package_remain_periods<=12) THEN '6期（不含）-12期（含）'
				WHEN   (periods-package_remain_periods>12 AND periods-package_remain_periods<=18) THEN '12期（不含）-18期（含）'
				WHEN   (periods-package_remain_periods>18 AND periods-package_remain_periods<=24) THEN '18期（不含）-24期（含）'
				WHEN   (periods-package_remain_periods>24 AND periods-package_remain_periods<=30) THEN '24期（不含）-30期（含）'
				WHEN   (periods-package_remain_periods>30 AND periods-package_remain_periods<=36) THEN '30期（不含）-36期（含）'
				WHEN   (periods-package_remain_periods>36 ) THEN '大于36期'
				ELSE    '0期'
		        END  AS yet_periods ]]>,
        CASE
        <![CDATA[
				WHEN   (periods-package_remain_periods>=1 AND periods-package_remain_periods<=6) THEN 2
				WHEN   (periods-package_remain_periods>6 AND periods-package_remain_periods<=12) THEN 3
				WHEN   (periods-package_remain_periods>12 AND periods-package_remain_periods<=18) THEN 4
				WHEN   (periods-package_remain_periods>18 AND periods-package_remain_periods<=24) THEN 5
				WHEN   (periods-package_remain_periods>24 AND periods-package_remain_periods<=30) THEN 6
				WHEN   (periods-package_remain_periods>30 AND periods-package_remain_periods<=36) THEN 7
				WHEN   (periods-package_remain_periods>36 ) THEN 8
				ELSE    1
		        END  AS orderSeq ]]>, project_id, asset_bag_id
        FROM
        t_asset_package
        WHERE project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        ) tbl
        INNER JOIN (SELECT max(project_id) project_id, max(asset_bag_id) asset_bag_id, COUNT(*) AS
        fcontract_count,SUM(package_remain_principal) AS
        package_remain_principals FROM t_asset_package
        WHERE
        project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        )tbl2
        on tbl.project_id = tbl2.project_id and tbl.asset_bag_id = tbl2.asset_bag_id
        GROUP BY tbl.yet_periods
        order by tbl.yet_periods
    </select>

    <!-- 借款人年龄分布 -->
    <select id="getAgeDistribute" resultType="java.util.Map">
        SELECT tbl.age AS loanDim,
        ROUND(SUM(package_remain_principal),2) AS PrincipalAmount,
        ROUND(SUM(package_remain_principal)/tbl2.package_remain_principals*100,2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(COUNT(1)/tbl2.fcontract_count*100,2) AS loanNumRatio,
        ROUND(SUM(package_remain_principal)/COUNT(1),2) AS avgAmount
        FROM(
        SELECT
        package_remain_principal,
        CASE
        <![CDATA[
				WHEN   (age>50) THEN '50岁（不含）以上'
				WHEN   (age>45 AND age<=50) THEN '45岁（不含）-50岁（含）'
				WHEN   (age>40 AND age<=45) THEN '40岁（不含）-45岁（含）'
				WHEN   (age>35 AND age<=40) THEN '35岁（不含）-40岁（含）'
				WHEN   (age>30 AND age<=35) THEN '30岁（不含）-35岁（含）'
				WHEN   (age>25 AND age<=30) THEN '25岁（不含）-30岁（含）'
				WHEN   (age>20 AND age<=25) THEN '20岁（不含）-25岁（含）'
				WHEN   (age>=18 AND age<=20) THEN '18岁（含）-20岁（含）'
				ELSE    '未知年龄'
		        END  AS age ]]>,
        CASE
        <![CDATA[
				WHEN   (age>50) THEN 8
				WHEN   (age>45 AND age<=50) THEN 7
				WHEN   (age>40 AND age<=45) THEN 6
				WHEN   (age>35 AND age<=40) THEN 5
				WHEN   (age>30 AND age<=35) THEN 4
				WHEN   (age>25 AND age<=30) THEN 3
				WHEN   (age>20 AND age<=25) THEN 2
				WHEN   (age>18 AND age<=20) THEN 1
				ELSE    9
		        END  AS orderSeq ]]>, project_id, asset_bag_id
        FROM
        t_asset_package
        WHERE project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        ) tbl
        INNER JOIN (SELECT max(project_id) project_id, max(asset_bag_id) asset_bag_id,COUNT(*) AS
        fcontract_count,SUM(package_remain_principal) AS
        package_remain_principals FROM t_asset_package
        WHERE
        project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        )tbl2
        on tbl.project_id = tbl2.project_id and tbl.asset_bag_id = tbl2.asset_bag_id
        GROUP BY tbl.age, tbl.orderSeq
        order by tbl.orderSeq
    </select>

    <!-- 借款人年收入分布 -->
    <select id="getAnnualIncomeDistribute" resultType="java.util.Map" >
        SELECT tbl.annual_income AS loanDim,
        ROUND(SUM(package_remain_principal),2) AS PrincipalAmount,
        ROUND(SUM(package_remain_principal)/tbl2.package_remain_principals*100,2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(COUNT(1)/tbl2.fcontract_count*100,2) AS loanNumRatio,
        ROUND(SUM(package_remain_principal)/COUNT(1),2) AS avgAmount
        FROM(
        SELECT
        package_remain_principal,
        CASE
        <![CDATA[
				WHEN   (annual_income>1000000) THEN '100万元(不含)以上'
				WHEN   (annual_income>500000 AND annual_income<=1000000) THEN '50万元（不含）-100万元（含）'
				WHEN   (annual_income>400000 AND annual_income<=500000) THEN '40万元（不含）-50万元（含）'
				WHEN   (annual_income>300000 AND annual_income<=400000) THEN '30万元（不含）-40万元（含）'
				WHEN   (annual_income>200000 AND annual_income<=300000) THEN '20万元（不含）-30万元（含）'
				WHEN   (annual_income>100000 AND annual_income<=200000) THEN '10万元（不含）-20万元（含）'
				WHEN   (annual_income>50000 AND annual_income<=100000) THEN '5万元（不含）-10万元（含）'
				WHEN   (annual_income<=50000) THEN '5万元（含）以下'
				ELSE    '未知收入'
		        END  AS annual_income ]]>,
        CASE
        <![CDATA[
				WHEN   (annual_income>1000000) THEN 8
				WHEN   (annual_income>500000 AND annual_income<=1000000) THEN 7
				WHEN   (annual_income>400000 AND annual_income<=500000) THEN 6
				WHEN   (annual_income>300000 AND annual_income<=400000) THEN 5
				WHEN   (annual_income>200000 AND annual_income<=300000) THEN 4
				WHEN   (annual_income>100000 AND annual_income<=200000) THEN 3
				WHEN   (annual_income>50000 AND annual_income<=100000) THEN 2
				WHEN   (annual_income<=50000) THEN 1
				ELSE    9
		        END  AS orderSeq ]]>, project_id, asset_bag_id
        FROM
        t_asset_package
        WHERE project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        ) tbl
        INNER JOIN (SELECT max(project_id) project_id, max(asset_bag_id) asset_bag_id,COUNT(*) AS
        fcontract_count,SUM(package_remain_principal) AS
        package_remain_principals FROM t_asset_package
        WHERE
        project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        )tbl2
        on tbl.project_id = tbl2.project_id and tbl.asset_bag_id = tbl2.asset_bag_id
        GROUP BY tbl.annual_income, tbl.orderSeq
        order by tbl.orderSeq
    </select>

    <!-- 抵押率分布 -->
    <select id="getPanwnRateDistribute" resultType="java.util.Map">
        SELECT max(tbl.pawn_value) AS loanDim,
        ROUND(SUM(package_remain_principal),2) AS PrincipalAmount,
        ROUND(SUM(package_remain_principal)/max(tbl2.package_remain_principals)*100,2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(COUNT(1)/max(tbl2.fcontract_count)*100,2) AS loanNumRatio,
        ROUND(SUM(package_remain_principal)/COUNT(1),2) AS avgAmount
        FROM(
        SELECT
        SUM(t1.package_remain_principal) package_remain_principal,
        CASE
        <![CDATA[
				WHEN   (max(t1.contract_amount) / sum(t2.pawn_value) >0.9) THEN '90%（不含）以上'
				WHEN   (max(t1.contract_amount) / sum(t2.pawn_value)>0.8 AND max(t1.contract_amount) / sum(t2.pawn_value)<=0.9) THEN '80%（不含）-90%（含）'
				WHEN   (max(t1.contract_amount) / sum(t2.pawn_value)>0.7 AND max(t1.contract_amount) / sum(t2.pawn_value)<=0.8) THEN '70%（不含）-80%（含）'
				WHEN   (max(t1.contract_amount) / sum(t2.pawn_value)>0.6 AND max(t1.contract_amount) / sum(t2.pawn_value)<=0.7) THEN '60%（不含）-70%（含）'
				WHEN   (max(t1.contract_amount) / sum(t2.pawn_value)>0.5 AND max(t1.contract_amount) / sum(t2.pawn_value)<=0.6) THEN '50%（不含）-60%（含）'
				WHEN   (max(t1.contract_amount) / sum(t2.pawn_value)>0.4 AND max(t1.contract_amount) / sum(t2.pawn_value)<=0.5) THEN '40%（不含）-50%（含）'
				WHEN   (max(t1.contract_amount) / sum(t2.pawn_value)>0.3 AND max(t1.contract_amount) / sum(t2.pawn_value)<=0.4) THEN '30%（不含）-40%（含）'
				WHEN   (max(t1.contract_amount) / sum(t2.pawn_value)>0.2 AND max(t1.contract_amount) / sum(t2.pawn_value)<=0.3) THEN '20%（不含）-30%（含）'
				WHEN   (max(t1.contract_amount) / sum(t2.pawn_value)>0.1 AND max(t1.contract_amount) / sum(t2.pawn_value)<=0.2) THEN '10%（不含）-20%（含）'
				ELSE    '10%(含)以下'
		        END  AS pawn_value ]]>,
        CASE
        <![CDATA[
				WHEN   (max(t1.contract_amount) / sum(t2.pawn_value) >0.9) THEN 10
				WHEN   (max(t1.contract_amount) / sum(t2.pawn_value)>0.8 AND max(t1.contract_amount) / sum(t2.pawn_value)<=0.9) THEN 9
				WHEN   (max(t1.contract_amount) / sum(t2.pawn_value)>0.7 AND max(t1.contract_amount) / sum(t2.pawn_value)<=0.8) THEN 8
				WHEN   (max(t1.contract_amount) / sum(t2.pawn_value)>0.6 AND max(t1.contract_amount) / sum(t2.pawn_value)<=0.7) THEN 7
				WHEN   (max(t1.contract_amount) / sum(t2.pawn_value)>0.5 AND max(t1.contract_amount) / sum(t2.pawn_value)<=0.6) THEN 6
				WHEN   (max(t1.contract_amount) / sum(t2.pawn_value)>0.4 AND max(t1.contract_amount) / sum(t2.pawn_value)<=0.5) THEN 5
				WHEN   (max(t1.contract_amount) / sum(t2.pawn_value)>0.3 AND max(t1.contract_amount) / sum(t2.pawn_value)<=0.4) THEN 4
				WHEN   (max(t1.contract_amount) / sum(t2.pawn_value)>0.2 AND max(t1.contract_amount) / sum(t2.pawn_value)<=0.3) THEN 3
				WHEN   (max(t1.contract_amount) / sum(t2.pawn_value)>0.1 AND max(t1.contract_amount) / sum(t2.pawn_value)<=0.2) THEN 2
				ELSE    1
		        END  AS orderSeq ]]>,
        max(t1.project_id) project_id,
        max(t1.asset_bag_id) asset_bag_id,
        max(t1.serial_number) serial_number,
        max(t1.contract_code) contract_code
        FROM
        t_asset_package t1
        inner join (select max(project_id) project_id,
        max(asset_bag_id) asset_bag_id,
        max(serial_number) serial_number ,max(contract_code) contract_code,sum(pawn_value) pawn_value from t_asset_package_guaranty group by serial_number )t2
        ON t1.project_id = t2.project_id
        AND t1.asset_bag_id = t2.asset_bag_id
        AND t1.serial_number = t2.serial_number
        AND t1.contract_code = t2.contract_code
        WHERE t1.project_id = #{distribute.projectId}
        AND t1.guarantee_type='抵押担保'
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and t1.asset_bag_id = #{distribute.assetBagId}
        </if>
        group by t2.contract_code
        ) tbl
        INNER JOIN (SELECT max( serial_number ) serial_number,
        max( project_id ) project_id,
        max( asset_bag_id ) asset_bag_id,COUNT(*) AS
        fcontract_count,SUM(package_remain_principal) AS
        package_remain_principals FROM t_asset_package
        WHERE
        project_id = #{distribute.projectId}
        AND guarantee_type='抵押担保'
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        )tbl2
        ON tbl.project_id = tbl2.project_id
        AND tbl.asset_bag_id = tbl2.asset_bag_id
        GROUP BY tbl.pawn_value,tbl.orderSeq
        ORDER BY tbl.orderSeq
    </select>

    <!-- 资产质量分布（待增加） -->
    <select id="getAssetQualityDistribute" resultType="java.util.Map">

    </select>

    <!--账龄分布-->
    <select id="getAgingDistribute" resultType="java.util.Map">
      SELECT tbl.agings AS loanDim,
        ROUND(SUM(package_remain_principal),2) AS PrincipalAmount,
        ROUND(SUM(package_remain_principal)/tbl2.package_remain_principals*100,2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(COUNT(1)/tbl2.fcontract_count*100,2) AS loanNumRatio,
        ROUND(SUM(package_remain_principal)/COUNT(1),2) AS avgAmount
        FROM(
        SELECT
        package_remain_principal,
        CASE
        WHEN (aging = 0) THEN '0(含) '
        ELSE CONCAT(FLOOR((aging-1)/6)*6+1,'(含)~',CEILING(aging/6)*6,'(含)')
        END AS agings,
        CASE
        WHEN (aging = 0) THEN 0
        ELSE CEILING(aging/6)
        END AS orderSeq, project_id, asset_bag_id
        FROM (
        SELECT
        package_remain_principal,
        IF(loan_expiry_date &lt; bag_date,
        IF(DAYOFMONTH(loan_expiry_date) - DAYOFMONTH(loan_issue_date) > 27,
        FLOOR(TIMESTAMPDIFF(MONTH,loan_issue_date, loan_expiry_date))+1,
        FLOOR(TIMESTAMPDIFF(MONTH,loan_issue_date, loan_expiry_date))),
        IF(loan_issue_date &lt; bag_date,
        FLOOR(TIMESTAMPDIFF(MONTH,loan_issue_date, bag_date)),
        0)) AS aging,
        project_id, asset_bag_id
        FROM
        t_asset_package
        WHERE project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        ) t ) tbl
        INNER JOIN (SELECT max(project_id) project_id, max(asset_bag_id) asset_bag_id,COUNT(*) AS
        fcontract_count,SUM(package_remain_principal) AS
        package_remain_principals FROM t_asset_package
        WHERE project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
          and asset_bag_id = #{distribute.assetBagId}
        </if>
        )tbl2
        on tbl.project_id = tbl2.project_id and tbl.asset_bag_id = tbl2.asset_bag_id
        GROUP BY tbl.agings
        order by tbl.agings
    </select>

    <!--合同剩余期限分布-->
    <select id="getSurplusTermDistribute" resultType="java.util.Map">
        SELECT tbl.surplusTerms AS loanDim,
        ROUND(SUM(package_remain_principal),2) AS PrincipalAmount,
        ROUND(SUM(package_remain_principal)/tbl2.package_remain_principals*100,2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(COUNT(1)/tbl2.fcontract_count*100,2) AS loanNumRatio,
        ROUND(SUM(package_remain_principal)/COUNT(1),2) AS avgAmount
        FROM(
        SELECT
        package_remain_principal,
        CASE
        WHEN ((term-aging) = 0) THEN '0(含) '
        ELSE CONCAT(FLOOR(((term-aging)-1)/6)*6+1,'(含)~',CEILING((term-aging)/6)*6,'(含)')
        END AS surplusTerms,
        CASE
        WHEN ((term-aging) = 0) THEN 0
        ELSE CEILING((term-aging)/6)
        END AS orderSeq, project_id, asset_bag_id
        FROM (
        SELECT
        package_remain_principal,
        IF(loan_expiry_date &lt; bag_date,
        IF(DAYOFMONTH(loan_expiry_date) - DAYOFMONTH(loan_issue_date) > 27,
        FLOOR(TIMESTAMPDIFF(MONTH,loan_issue_date, loan_expiry_date))+1,
        FLOOR(TIMESTAMPDIFF(MONTH,loan_issue_date, loan_expiry_date))),
        IF(loan_issue_date &lt; bag_date,
        FLOOR(TIMESTAMPDIFF(MONTH,loan_issue_date, bag_date)),
        0)) AS aging,
        IF(DAYOFMONTH(loan_expiry_date) - DAYOFMONTH(loan_issue_date) > 27,
        FLOOR(TIMESTAMPDIFF(MONTH,loan_issue_date, loan_expiry_date))+1,
        FLOOR(TIMESTAMPDIFF(MONTH,loan_issue_date, loan_expiry_date))) AS term,
        project_id, asset_bag_id
        FROM
        t_asset_package
        WHERE project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">and asset_bag_id =
            #{distribute.assetBagId}
        </if>
        ) t ) tbl
        INNER JOIN (SELECT max(project_id) project_id, max(asset_bag_id) asset_bag_id,COUNT(*) AS
        fcontract_count,SUM(package_remain_principal) AS
        package_remain_principals FROM t_asset_package
        WHERE project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
          and asset_bag_id = #{distribute.assetBagId}
        </if>
        )tbl2
        on tbl.project_id = tbl2.project_id and tbl.asset_bag_id = tbl2.asset_bag_id
        GROUP BY tbl.surplusTerms
        order by tbl.surplusTerms
    </select>

    <!--  还款方式分布-->
    <select id="getRepaymentTypeDistribute" resultType="java.util.Map">
        SELECT tbl.repayment_type AS loanDim,
        ROUND(SUM(package_remain_principal),2) AS PrincipalAmount,
        ROUND(SUM(package_remain_principal)/tbl2.package_remain_principals*100,2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(COUNT(1)/tbl2.fcontract_count*100,2) AS loanNumRatio,
        ROUND(SUM(package_remain_principal)/COUNT(1),2) AS avgAmount
        FROM(
        SELECT
        package_remain_principal,repayment_type, project_id, asset_bag_id
        FROM
        t_asset_package
        WHERE project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        ) tbl
        INNER JOIN (SELECT max(project_id) project_id, max(asset_bag_id) asset_bag_id,COUNT(*) AS
        fcontract_count,SUM(package_remain_principal) AS
        package_remain_principals FROM t_asset_package
        WHERE
        project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        )tbl2
        on tbl.project_id = tbl2.project_id and tbl.asset_bag_id = tbl2.asset_bag_id
        GROUP BY tbl.repayment_type
        order by tbl.repayment_type
    </select>

    <!-- 借款人行业分布-->
    <select id="getIndustryDistribute" resultType="java.util.Map">
        SELECT tbl.borrower_industry AS loanDim,
        ROUND(SUM(package_remain_principal),2) AS PrincipalAmount,
        ROUND(SUM(package_remain_principal)/tbl2.package_remain_principals*100,2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(COUNT(1)/tbl2.fcontract_count*100,2) AS loanNumRatio,
        ROUND(SUM(package_remain_principal)/COUNT(1),2) AS avgAmount
        FROM(
        SELECT
        package_remain_principal,borrower_industry, project_id, asset_bag_id
        FROM
        t_asset_package
        WHERE project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        ) tbl
        INNER JOIN (SELECT max(project_id) project_id, max(asset_bag_id) asset_bag_id,COUNT(*) AS
        fcontract_count,SUM(package_remain_principal) AS
        package_remain_principals FROM t_asset_package
        WHERE
        project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        )tbl2
        on tbl.project_id = tbl2.project_id and tbl.asset_bag_id = tbl2.asset_bag_id
        GROUP BY tbl.borrower_industry
        order by tbl.borrower_industry
    </select>

    <!-- 借款人信用评分分布 -->
    <select id="getCreditScoreDistribute" resultType="java.util.Map">
        SELECT tbl.score_level AS loanDim,
        ROUND(SUM(package_remain_principal),2) AS PrincipalAmount,
        ROUND(SUM(package_remain_principal)/tbl2.package_remain_principals*100,2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(COUNT(1)/tbl2.fcontract_count*100,2) AS loanNumRatio,
        ROUND(SUM(package_remain_principal)/COUNT(1),2) AS avgAmount
        FROM(
        SELECT
        package_remain_principal,score_level, project_id, asset_bag_id
        FROM
        t_asset_package
        WHERE project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        ) tbl
        INNER JOIN (SELECT max(project_id) project_id, max(asset_bag_id) asset_bag_id, COUNT(*) AS
        fcontract_count,SUM(package_remain_principal) AS
        package_remain_principals FROM t_asset_package
        WHERE
        project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        )tbl2
        on tbl.project_id = tbl2.project_id and tbl.asset_bag_id = tbl2.asset_bag_id
        GROUP BY tbl.score_level
        order by tbl.score_level
    </select>

    <!-- 反欺诈等级评分-->
    <select id="getAntiRfuadLevelDistribute" resultType="java.util.Map">
        SELECT tbl.cheat_level AS loanDim,
        ROUND(SUM(package_remain_principal),2) AS PrincipalAmount,
        ROUND(SUM(package_remain_principal)/tbl2.package_remain_principals*100,2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(COUNT(1)/tbl2.fcontract_count*100,2) AS loanNumRatio,
        ROUND(SUM(package_remain_principal)/COUNT(1),2) AS avgAmount
        FROM(
        SELECT
        package_remain_principal,cheat_level, project_id, asset_bag_id
        FROM
        t_asset_package
        WHERE project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        ) tbl
        INNER JOIN (SELECT max(project_id) project_id, max(asset_bag_id) asset_bag_id,COUNT(*) AS
        fcontract_count,SUM(package_remain_principal) AS
        package_remain_principals FROM t_asset_package
        WHERE
        project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        )tbl2
        on tbl.project_id = tbl2.project_id and tbl.asset_bag_id = tbl2.asset_bag_id
        GROUP BY tbl.cheat_level
        order by tbl.cheat_level
    </select>

    <!-- 资产等级分布 -->
    <select id="getAssetLevelDistribute" resultType="java.util.Map">
        SELECT
        cast(tbl.score_range as SIGNED) AS loanDim,
        ROUND(SUM(package_remain_principal),2) AS PrincipalAmount,
        ROUND(SUM(package_remain_principal)/tbl2.package_remain_principals*100,2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(COUNT(1)/tbl2.fcontract_count*100,2) AS loanNumRatio,
        ROUND(SUM(package_remain_principal)/COUNT(1),2) AS avgAmount
        FROM(
        SELECT
        package_remain_principal,score_range, project_id, asset_bag_id
        FROM
        t_asset_package
        WHERE project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        ) tbl
        INNER JOIN (SELECT max(project_id) project_id, max(asset_bag_id) asset_bag_id,COUNT(*) AS
        fcontract_count,SUM(package_remain_principal) AS
        package_remain_principals FROM t_asset_package
        WHERE
        project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        )tbl2
        on tbl.project_id = tbl2.project_id and tbl.asset_bag_id = tbl2.asset_bag_id
        GROUP BY loanDim
        order by loanDim
    </select>

    <!-- 借款人地区分布 -->
    <select id="getProvinceDistribute" resultType="java.util.Map" >
        SELECT tbl.province AS loanDim,
        ROUND(SUM(package_remain_principal),2) AS PrincipalAmount,
        ROUND(SUM(package_remain_principal)/tbl2.package_remain_principals*100,2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(COUNT(1)/tbl2.fcontract_count*100,2) AS loanNumRatio,
        ROUND(SUM(package_remain_principal)/COUNT(1),2) AS avgAmount
        FROM(
        SELECT
        package_remain_principal,substring(province, 1, 2) AS province, project_id, asset_bag_id
        FROM
        t_asset_package
        WHERE project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        ) tbl
        INNER JOIN (SELECT max(project_id) project_id, max(asset_bag_id) asset_bag_id,COUNT(*) AS
        fcontract_count,SUM(package_remain_principal) AS
        package_remain_principals FROM t_asset_package
        WHERE
        project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        )tbl2
        on tbl.project_id = tbl2.project_id and tbl.asset_bag_id = tbl2.asset_bag_id
        GROUP BY tbl.province
        order by AmountRatio DESC,tbl.province
    </select>

    <!-- 车辆品牌分布 -->
    <select id="getCarBrandDistribute" resultType="java.util.Map">
        SELECT max(tb3.car_brand) AS loanDim,
        ROUND(SUM(package_remain_principal),2) AS PrincipalAmount,
        ROUND(SUM(package_remain_principal)/(SELECT SUM( package_remain_principal ) AS package_remain_principals FROM t_asset_package
      WHERE
      project_id = #{distribute.projectId}
      <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
        and asset_bag_id = #{distribute.assetBagId}
      </if>)*100,2) AS AmountRatio,
      COUNT(1) AS loanNum,
      ROUND(COUNT(1)/(SELECT COUNT( 1 ) AS fcontract_count FROM t_asset_package
      WHERE
      project_id = #{distribute.projectId}
      <if test="distribute.assetBagId != null and distribute.assetBagId !=''">and asset_bag_id =
        #{distribute.assetBagId}
      </if>)*
      100,2) AS loanNumRatio,
      ROUND(SUM(package_remain_principal)/COUNT(1),2) AS avgAmount
      FROM(
      SELECT
      t1.package_remain_principal,t1.project_id, t1.asset_bag_id,t1.contract_code
      FROM
      t_asset_package t1
      WHERE t1.project_id = #{distribute.projectId}
      <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and t1.asset_bag_id = #{distribute.assetBagId}
        </if>
        ) tbl
        INNER JOIN(select max(contract_code) contract_code,max(project_id) project_id, max(asset_bag_id) asset_bag_id,
        GROUP_CONCAT(car_brand) AS car_brand
        from t_asset_package_guaranty
        WHERE
        project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        group by contract_code
        )tb3
        on tbl.project_id = tb3.project_id and tbl.asset_bag_id = tb3.asset_bag_id and tbl.contract_code=tb3.contract_code
        GROUP BY tb3.car_brand
        order by AmountRatio DESC,loanDim
    </select>

    <!-- 新旧车辆分布 -->
    <select id="getCarTypeDistribute" resultType="java.util.Map">
        SELECT max(tb3.car_type) AS loanDim,
        ROUND(SUM(package_remain_principal),2) AS PrincipalAmount,
        ROUND(SUM(package_remain_principal)/(SELECT SUM( package_remain_principal ) AS package_remain_principals FROM t_asset_package
        WHERE
        project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>)*100,2) AS AmountRatio,
      COUNT(1) AS loanNum,
      ROUND(COUNT(1)/(SELECT COUNT( 1 ) AS fcontract_count FROM t_asset_package
      WHERE
      project_id = #{distribute.projectId}
      <if test="distribute.assetBagId != null and distribute.assetBagId !=''">and asset_bag_id =
        #{distribute.assetBagId}
      </if>)*
      100,2) AS loanNumRatio,
      ROUND(SUM(package_remain_principal)/COUNT(1),2) AS avgAmount
      FROM(
      SELECT
      t1.package_remain_principal,t1.project_id, t1.asset_bag_id,t1.contract_code
      FROM
      t_asset_package t1
      WHERE t1.project_id = #{distribute.projectId}
      <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and t1.asset_bag_id = #{distribute.assetBagId}
        </if>
        ) tbl
        INNER JOIN(select max(contract_code) contract_code,max(project_id) project_id, max(asset_bag_id) asset_bag_id,
        CASE
        <![CDATA[
        WHEN   (find_in_set('新车',GROUP_CONCAT(car_type))>0 and find_in_set('二手车',GROUP_CONCAT(car_type))<1) THEN '新车'
        WHEN   (find_in_set('新车',GROUP_CONCAT(car_type))<1 and find_in_set('二手车',GROUP_CONCAT(car_type))>0 ) THEN '二手车'
        ELSE    '二手车,新车'
        END  AS car_type ]]>,
        CASE
        <![CDATA[
        WHEN   (find_in_set('新车',GROUP_CONCAT(car_type))>0 and find_in_set('二手车',GROUP_CONCAT(car_type))<1) THEN 1
        WHEN   (find_in_set('新车',GROUP_CONCAT(car_type))<1 and find_in_set('二手车',GROUP_CONCAT(car_type))>0 ) THEN 2
        ELSE    3
        END  AS orderSeq ]]>
        from t_asset_package_guaranty
        WHERE
        project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        group by contract_code
        )tb3
        on tbl.project_id = tb3.project_id and tbl.asset_bag_id = tb3.asset_bag_id and tbl.contract_code=tb3.contract_code
        GROUP BY
        tb3.car_type,tb3.orderSeq
        ORDER BY tb3.orderSeq
    </select>

    <!-- 风控结果分布 -->
    <select id="getAssetWindControlResultDistribute" resultType="java.util.Map">
        SELECT tbl.wind_control_status AS loanDim,
        ROUND(SUM(package_remain_principal),2) AS PrincipalAmount,
        ROUND(SUM(package_remain_principal)/tbl2.package_remain_principals*100,2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(COUNT(1)/tbl2.fcontract_count*100,2) AS loanNumRatio,
        ROUND(SUM(package_remain_principal)/COUNT(1),2) AS avgAmount
        FROM(
        SELECT
        package_remain_principal,wind_control_status, project_id, asset_bag_id
        FROM
        t_asset_package
        WHERE project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        ) tbl
        INNER JOIN (SELECT max(project_id) project_id, max(asset_bag_id) asset_bag_id,COUNT(*) AS
        fcontract_count,SUM(package_remain_principal) AS
        package_remain_principals FROM t_asset_package
        WHERE
        project_id = #{distribute.projectId}
        <if test="distribute.assetBagId != null and distribute.assetBagId !=''">
            and asset_bag_id = #{distribute.assetBagId}
        </if>
        )tbl2
        on tbl.project_id = tbl2.project_id and tbl.asset_bag_id = tbl2.asset_bag_id
        GROUP BY tbl.wind_control_status
        order by tbl.wind_control_status
    </select>