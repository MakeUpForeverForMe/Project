SELECT
        IFNULL(count(*), 0) AS 'assetCount', -- 资产数量
        IFNULL(count(DISTINCT document_num),0) AS 'borrowersNumber', -- 借款人数量
        IFNULL(MAX(contract_amount), 0) AS 'maximumContractAmount', -- 单笔最大合同金额
        IFNULL(MIN(contract_amount), 0) AS 'minimumContractAmount', -- 单笔最小合同金额
        ROUND(IFNULL(SUM(contract_amount) / count(*),0),2)
        AS 'averageContractAmount', -- 平均合同金额
        IFNULL(MAX(annual_income), 0) AS 'maximumAnnualIncome', -- 借款人最大年收入
        IFNULL(MIN(annual_income), 0) AS 'minimumAnnualIncome', -- 借款人最小年收入
        ROUND(IFNULL(SUM(annual_income) / count(*),0),2)
        AS 'averageAnnualIncome', -- 借款人平均年收入
        ROUND(IFNULL(SUM(annual_income * balance) / SUM(balance),0),2)
        AS 'weightedAnnualIncome', -- 借款人加权平均年收入
        IFNULL(MAX(COALESCE (annual_income / balance,0)),0)
        AS 'maximumIncomeRatio', -- 借款人最大债务比
        IFNULL(MIN(COALESCE (annual_income / balance,0)),0)
        AS 'minimumIncomeRatio', -- 借款人最小债务比
        ROUND(IFNULL(SUM(COALESCE (annual_income / balance,0)) / count(*),0),2)
        AS 'averageIncomeRatio', -- 借款人平均债务比
        ROUND(IFNULL(SUM(annual_income) / SUM(balance),0),2)
        AS 'weightedIncomeRatio', -- 借款人加权平均债务比
        COALESCE (MAX(age), 0) AS 'maximumAge', -- 单笔最大年龄
        COALESCE (MIN(age), 0) AS 'minimumAge', -- 单笔最小年龄
        COALESCE ((SUM(age)), 0) AS 'sumAge', -- 总年龄
        ROUND(IFNULL(sum(balance * age) / SUM(balance),0),2) AS 'weightedAge', -- 加权平均年龄
        IFNULL(SUM(balance),0) AS 'principal', -- 本金余额
        IFNULL(SUM(remain_interest + balance),0) AS 'interest', -- 本息余额
        ROUND(IFNULL(SUM(balance*IFNULL(CAST(year_loan_rate AS DECIMAL(20,6)),0))/SUM(balance),0),2)
        AS 'interestRate', -- 加权平均年利率
        ROUND(IFNULL(SUM(balance*remain_term)/SUM(balance),0),2)
        AS 'remainingPeriods', -- 加权平均剩余期数
        IFNULL(MAX(balance),0) AS 'maximumPrincipal', -- 单笔最大本金余额
        IFNULL(MIN(balance),0) AS 'minimumPrincipal', -- 单笔最小本金余额
        IFNULL(MAX(total_term),0) AS 'maximumNumberPeriods', -- 单笔最大合同期数
        IFNULL(MIN(total_term),0) AS 'minimumberPeriods', -- 单笔最小合同期数
        IFNULL(SUM(total_term),0) AS 'sumNumberPeriods', -- 总合同期数
        ROUND(IFNULL(SUM(balance*total_term)/SUM(balance),0),2)
        AS 'weightedNumberPeriods', -- 加权平均合同期数
        IFNULL(MAX(remain_term),0) AS 'maximumNumberIssues', -- 单笔最大剩余合同期数
        IFNULL(MIN(remain_term),0) AS 'minimumNumberIssues', -- 单笔最小剩余合同期数
        IFNULL(SUM(remain_term),0) AS 'sumNumberIssues', -- 资产剩余合同总期数
        ROUND(IFNULL(SUM(balance*remain_term)/SUM(balance),0),2)
        AS 'weightedNumberIssues', -- 加权平均剩余合同期数
        IFNULL(MAX(already_repay_term),0) AS 'maximumNumberOrders', -- 单笔最大已还合同期数
        IFNULL(MIN(already_repay_term),0) AS 'minimumNumberOrders', -- 单笔最小已还合同期数
        IFNULL(SUM(already_repay_term),0) AS 'sumNumberOrders', -- 已还总合同期数
        ROUND(IFNULL(SUM(balance*already_repay_term)/SUM(balance),0),2)
        AS 'weightedNumberOrders', -- 加权平均已还合同期数
        COALESCE(MAX(CAST(year_loan_rate AS DECIMAL(20,6))),0) AS 'maximumInterestRate', -- 单笔最大年利率
        COALESCE(MIN(CAST(year_loan_rate AS DECIMAL(20,6))),0) AS 'minimumInterestRate', -- 单笔最小年利率
        COALESCE(SUM(CAST(year_loan_rate AS DECIMAL(20,6))),0) AS 'sumInterestRate', -- 总年利率
        IFNULL(MIN(aging),0) AS 'minAging', -- 最小账龄
        IFNULL(MAX(aging),0) AS 'maxAging',	-- 最大账龄
        IFNULL(SUM(aging),0) AS 'sumAging', -- 总账龄
        ROUND(IFNULL(SUM(aging * balance) / SUM(balance),0),2) AS 'weightedAging', -- 加权平均账龄
        IFNULL(MIN(term-aging),0) AS 'minSurplusTerm', -- 最小合同剩余期限
        IFNULL(MAX(term-aging),0) AS 'maxSurplusTerm',	-- 最大合同剩余期限
        IFNULL(SUM(term-aging),0) AS 'sumSurplusTerm', -- 总合同剩余期限
        ROUND(IFNULL(SUM((term-aging) * balance) / SUM(balance),0),2) AS 'weightedSurplusTerm' -- 加权平均剩余期限
        FROM (
        select project_id, serial_number, document_num,
        contract_amount, annual_income,
        (floor((DATEDIFF(loan_issue_date, birthday)) / 365)) AS age,
        -- TIMESTAMPDIFF(MONTH,loan_issue_date, frist_repayment_date)
        IF(loan_expiry_date &lt; #{queryMap.endDate,jdbcType=DATE},
        IF(DAYOFMONTH(loan_expiry_date) - DAYOFMONTH(loan_issue_date) > 27,
        FLOOR(TIMESTAMPDIFF(MONTH,loan_issue_date, loan_expiry_date))+1,
        FLOOR(TIMESTAMPDIFF(MONTH,loan_issue_date, loan_expiry_date))),
        IF(loan_issue_date &lt; #{queryMap.endDate,jdbcType=DATE},
        FLOOR(TIMESTAMPDIFF(MONTH,loan_issue_date, #{queryMap.endDate,jdbcType=DATE})),
        0)) AS aging,
        IF(DAYOFMONTH(loan_expiry_date) - DAYOFMONTH(loan_issue_date) > 27,
        FLOOR(TIMESTAMPDIFF(MONTH,loan_issue_date, loan_expiry_date))+1,
        FLOOR(TIMESTAMPDIFF(MONTH,loan_issue_date, loan_expiry_date))) AS term
        from t_basic_asset
        where `STATUS` != - 1 AND project_id = #{queryMap.projectId,jdbcType=VARCHAR}
        <if test="queryMap.bagIdList != null and queryMap.bagIdList.size()>0">
            and (asset_bag_id in
            <foreach close=")" collection="queryMap.bagIdList" item="listItem" open="(" separator=",">
                #{listItem}
            </foreach>
            <if test="queryMap.isNotPackage == 1">
                or asset_bag_id is null
            </if>
            )
        </if>
        <if test="(queryMap.bagIdList == null or queryMap.bagIdList.size() == 0) and queryMap.isNotPackage == 1">
            and asset_bag_id is null
        </if> ) t1
        INNER JOIN (
        select project_id, serial_number, COALESCE(remain_principal,0) AS balance,
        COALESCE(remain_interest,0) AS remain_interest, COALESCE(remain_term,0) AS remain_term,
        COALESCE(total_term,0) AS total_term, COALESCE(already_repay_term,0) AS already_repay_term,
        COALESCE(year_loan_rate,0) AS year_loan_rate
        from t_assetaccountcheck
        where project_id = #{queryMap.projectId,jdbcType=VARCHAR}
        and data_extract_time = #{queryMap.endDate,jdbcType=DATE}
        <if test="queryMap.isAssetStatus != 1">
            AND asset_status != '已结清'
        </if> ) t2 ON t2.serial_number = t1.serial_number


-- 查询抵押物信息
SELECT
        IFNULL(SUM(t2.remain_principal), 0) AS 'assetsBalance',	-- 抵押的资产余额
        IFNULL(count(*), 0) AS 'frequencyAssets',	-- 抵押的资产笔数
        IFNULL(SUM(t3.pawn), 0) AS 'initialAssessedMortgage',	-- 抵押的初始评估价值
        ROUND(IFNULL(SUM(t1.contract_amount / t3.pawn * t2.remain_principal) / SUM(t2.remain_principal) * 100,0),2)
        AS 'weightedAverageMortgageRate'	-- 加权平均抵押率
        FROM
        (
            select project_id, serial_number, contract_amount from t_basic_asset
            where STATUS != - 1 AND project_id = #{queryMap.projectId,jdbcType=VARCHAR}
            and guarantee_type = '抵押担保' and asset_type = '汽车抵押贷款'
            <if test="queryMap.bagIdList != null and queryMap.bagIdList.size()>0">
                and (asset_bag_id in
                <foreach close=")" collection="queryMap.bagIdList" item="listItem" open="(" separator=",">
                    #{listItem}
                </foreach>
                <if test="queryMap.isNotPackage == 1">
                    or asset_bag_id is null
                </if>
                )
            </if>
            <if test="(queryMap.bagIdList == null or queryMap.bagIdList.size() == 0) and queryMap.isNotPackage == 1">
                and asset_bag_id is null
            </if>
        ) t1
        INNER JOIN
        (
            select project_id, serial_number, remain_principal from t_assetaccountcheck
            where project_id = #{queryMap.projectId,jdbcType=VARCHAR} and data_extract_time = #{queryMap.endDate,jdbcType=DATE}
            <if test="queryMap.isAssetStatus != 1">
                AND asset_status != '已结清'
            </if>
        ) t2
        on t1.serial_number = t2.serial_number
        INNER JOIN
        (
            select serial_number, IFNULL(SUM(pawn_value), 0) AS pawn from t_guarantycarinfo
            where project_id = #{queryMap.projectId,jdbcType=VARCHAR} group by serial_number
        ) t3
        on t1.serial_number = t3.serial_number

