<!--
        根据项目编号,资产包ID 查询字段:
        单笔最大合同金额,单笔最小合同金额,平均合同金额,
        借款人最大年收入,借款人最小年收入,借款人平均年收入,加权平均年收入,
        单笔最大年利率,单笔最小年利率,平均年利率,加权平均贷款年利率
    -->
    <select id="getAssetPackagePacketSituation" resultType="java.util.Map">
        SELECT
            COUNT(1) assetCount,
            -- 资产数量,
            MAX(contract_amount) singleMaxOntractAmount,
            -- 单笔最大合同金额
            MIN(contract_amount) singleMinOntractAmount,
            -- 单笔最小合同金额
            ROUND(
                (
                    SUM(contract_amount) / COUNT(*)
                ),
                2
            ) avgContractAmount,
            -- 平均合同金额
            MAX(annual_income) maxBorrowerAnnualIncome,
            -- 借款人最大年收入
            MIN(annual_income) minBorrowerAnnualIncome,
            -- 借款人最小年收入
            ROUND(
                SUM(annual_income) / COUNT(*),
                2
            ) avgBorrowerAnnualIncome,
            -- 借款人平均年收入
            ROUND(
                sum(
                    package_remain_principal * annual_income
                ) / SUM(package_remain_principal),
                2
            ) weightAvgBorrowerAnnualIncome,
            -- 加权平均年收入
            MAX(contract_interest_rate) singleMaxInterestRate,
            -- 单笔最大年利率
            MIN(contract_interest_rate) singleMinInterestRate,
            -- 单笔最小年利率
            ROUND(
                SUM(contract_interest_rate) / COUNT(*),
                2
            ) avgInterestRate,
            -- 平均年利率
            ROUND(
                sum(
                    package_remain_principal * contract_interest_rate
                ) / SUM(package_remain_principal),
                2
            ) weightAvgInterestRate -- 加权平均贷款年利率
        FROM
            t_basic_asset
        WHERE
            project_id = #{projectId,jdbcType=VARCHAR}
        AND asset_bag_id = #{assetBagId,jdbcType=VARCHAR}
    </select>
    <!--
        根据项目编号,资产包ID,封包日期 查询字段:
        本金余额汇总,本息余额汇总,
        单笔最大本金余额,单笔最小本金余额,平均本金余额
    -->
    <select id="getPrincipalBalanceInfo" resultType="java.util.Map">
    SELECT
        sum(t1.should_repay_principal) principalBalance, -- 本金余额汇总
        sum(t1.should_repay_principal+should_repay_interest) principalInterestBalance, -- 本息余额汇总
        max(t2.package_remain_principal) singleMaxPrincipalBalance, -- 单笔最大本金余额
        MIN(t2.package_remain_principal) singleMinPrincipalBalance, -- 单笔最小本金余额
        ROUND(sum(t1.should_repay_principal)/(SELECT
                            COUNT(*) assetCount
                        FROM
                        t_basic_asset
                        WHERE
                            project_id = #{projectId,jdbcType=VARCHAR}
                        AND asset_bag_id = #{assetBagId,jdbcType=VARCHAR}
                        ),2) avgPrincipalBalance -- 平均本金余额
    from
        t_repaymentplan t1 join t_basic_asset t2 on t1.serial_number = t2.serial_number
    where
            t2.project_id = #{projectId,jdbcType=VARCHAR}
    AND	t2.asset_bag_id = #{assetBagId,jdbcType=VARCHAR}
    AND t1.should_repay_date >= #{packetDate,jdbcType=VARCHAR}
    AND (t1.repay_status != 1
    OR t1.repay_status IS NULL)
    </select>

    <!--
        根据项目编号,资产包ID,封包日期 查询字段:
        单笔最大债务比,单笔最小债务比,平均债务比,加权平均债务比
    -->
    <select id="getAnnualIncomeInfo" resultType="java.util.Map">
        SELECT
            Max(q1.annualIncomeRate) maxIncomeDebtRatio,
            MIN(q1.annualIncomeRate) minIncomeDebtRatio,
            ROUND(sum(q1.annualIncomeRate) / COUNT(q2.serial_number),2) avgIncomeDebtRatio,
            ROUND(
                sum(
                    (
                        q2.package_remain_principal * q2.annual_income
                    ) / q2.package_remain_principal
                ) / SUM(
                    q2.package_remain_principal
                ),
                2
            ) weightAvgIncomeDebtRatio
        FROM
            (
                SELECT
                    t1.serial_number,
                    ROUND(
                        t2.annual_income / sum(t1.should_repay_principal),
                        2
                    ) annualIncomeRate
                FROM
                    t_repaymentplan t1
                JOIN t_basic_asset t2 ON t1.serial_number = t2.serial_number
                WHERE
                    t2.project_id = #{projectId,jdbcType=VARCHAR}
                AND t2.asset_bag_id = #{assetBagId,jdbcType=VARCHAR}
                AND t1.should_repay_date >= #{packetDate,jdbcType=VARCHAR}
                AND (t1.repay_status != 1
                OR t1.repay_status IS NULL)
                GROUP BY
                    t2.annual_income,
                    serial_number
            ) q1
        JOIN t_basic_asset q2 ON q1.serial_number = q2.serial_number
        WHERE
            q2.project_id = #{projectId,jdbcType=VARCHAR}
        AND q2.asset_bag_id = #{assetBagId,jdbcType=VARCHAR}
    </select>
    <!--
        根据项目编号,资产包ID,封包日期 查询字段:
        借款人数量,借款人平均本金余额
    -->
    <select id="getBorrowersCountAndRestPrincipalAverage" resultType="java.util.Map">
    SELECT
        t2.borrowersCount borrowerCount,
        -- 借款人数量
        (
            t1.borrowersRestPrincipalAverage / t2.borrowersCount
        ) borrowerAvgPrincipalBalance
        -- 借款人平均本金余额
    FROM
        (
            SELECT
                COALESCE (
                    sum(package_remain_principal),
                    0
                ) borrowersRestPrincipalAverage
            FROM
                t_basic_asset
            WHERE
                project_id = #{projectId,jdbcType=VARCHAR}
                AND asset_bag_id = #{assetBagId,jdbcType=VARCHAR}
        ) t1,
        (
            SELECT
                count(1) borrowersCount
            -- 借款人数量
            FROM
                (
                    SELECT
                        document_num
                    FROM
                        t_basic_asset
                    WHERE
                        project_id = #{projectId,jdbcType=VARCHAR}
                    AND asset_bag_id = #{assetBagId,jdbcType=VARCHAR}
                    GROUP BY
                        document_num
                ) t
        ) t2
    </select>

    <!--
        根据项目编号,资产包ID,封包日期 查询字段:
        单笔最大合同期数,单笔最小合同期数,平均合同期数,加权平均合同期数
    -->
    <select id="getContractPeriodInfo" resultType="java.util.Map">
        SELECT
            MAX(periods) singleMaxContractPeriods, -- 单笔最大合同期数
            MIN(periods) singleMinContractPeriods, -- 单笔最小合同期数
            ROUND(sum(periods)/COUNT(*),2) avgContractPeriods, -- 平均合同期数
            ROUND(sum(package_remain_principal * periods) / SUM(package_remain_principal),2) weightAvgContractPeriods -- 加权平均合同期数
        FROM
            t_basic_asset
        WHERE
            project_id = #{projectId,jdbcType=VARCHAR}
        AND asset_bag_id = #{assetBagId,jdbcType=VARCHAR}
    </select>
    <!--
        根据项目编号,资产包ID,封包日期 查询字段:
        单笔最大剩余期数,单笔最小剩余期数,平均剩余期数,加权平均剩余期数
    -->
    <select id="getAssetResiduePeriodInfo" resultType="java.util.Map">
        SELECT
            MAX(package_remain_periods) singleMaxRemainPeriods, -- 单笔最大剩余期数
            MIN(package_remain_periods) singleMinRemainPeriods, -- 单笔最小剩余期数
            ROUND(sum(package_remain_periods)/COUNT(*),2) avgRemainPeriods, -- 平均剩余期数
            ROUND(sum(package_remain_principal * package_remain_periods) / SUM(package_remain_principal),2) weightAvgRemainPeriods -- 加权平均剩余期数
        FROM
            t_basic_asset
        WHERE
            project_id = #{projectId,jdbcType=VARCHAR}
        AND asset_bag_id = #{assetBagId,jdbcType=VARCHAR}
    </select>
    <!--
        根据项目编号,资产包ID,封包日期 查询字段:
        单笔最大已还期数,单笔最小已还期数,平均已还期数,加权平均已还期数
    -->
    <select id="getAssetRepayPeriodInfo" resultType="java.util.Map">
        SELECT
            MAX(periods-package_remain_periods) singleMaxReplayPeriods, -- 单笔最大已还期数
            MIN(periods-package_remain_periods) singleMinReplayPeriods, -- 单笔最小已还期数
            ROUND(sum(periods-package_remain_periods)/COUNT(*),2) avgReplayPeriods, -- 平均已还期数
            ROUND(sum(package_remain_principal * (periods-package_remain_periods)) / SUM(package_remain_principal),2) weightAvgReplayPeriods -- 加权平均已还期数
        FROM
            t_basic_asset
        WHERE
            project_id = #{projectId,jdbcType=VARCHAR}
        AND asset_bag_id = #{assetBagId,jdbcType=VARCHAR}
    </select>
    <!--
    根据项目编号,资产包ID,封包日期 查询字段:
    单笔最大年龄,单笔最小年龄,平均年龄,加权平均年龄
    -->
    <select id="getAgeInfo" resultType="java.util.Map">
    SELECT
        COALESCE (MAX(age), 0) maxBorrowerAge,
        -- 单笔最大年龄
        COALESCE (MIN(age), 0) minBorrowerAge,
        -- 单笔最小年龄
        COALESCE (
            (
                SUM(age) / (
                    SELECT
                        COUNT(*) assetCount
                    FROM
                        t_basic_asset
                    WHERE
                        project_id = #{projectId,jdbcType=VARCHAR}
                    AND asset_bag_id = #{assetBagId,jdbcType=VARCHAR}
                )
            ),
            0
        ) avgBorrowerAge,
        -- 平均年龄
        COALESCE (
            sum(
                packagePrincipalBalance * age
            ) / SUM(packagePrincipalBalance),
            0
        ) weightAvgBorrowerAge -- 加权平均年龄
    FROM
        (
            SELECT
                (
                    floor(
                        (
                            DATEDIFF(
                                t2.loan_issue_date,
                                t2.birthday
                            )
                        ) / 365
                    )
                ) age,
                t2.package_remain_principal packagePrincipalBalance
            FROM
                (
                    SELECT
                        serial_number,
                        loan_issue_date,
                        birthday,
                        package_remain_principal
                    FROM
                        t_basic_asset
                    WHERE
                        project_id = #{projectId,jdbcType=VARCHAR}
                    AND asset_bag_id = #{assetBagId,jdbcType=VARCHAR}
                ) t2
        ) t
    </select>

    <!--
    根据项目编号,资产包ID,封包日期 查询字段:
    最小账龄,最大账龄,平均账龄,加权平均账龄,最小合同剩余期限,最大合同剩余期限,平均合同剩余期限,加权平均剩余期限
    -->
    <select id="getAgingTermInfo"  resultType="java.util.Map">
        SELECT
            IFNULL(MAX(aging),0) AS maxAging,
            IFNULL(MIN(aging),0) AS minAging,
            ROUND(IFNULL(SUM(aging)/count(1),0),2) AS avgAging,
            ROUND(IFNULL(sum(package_remain_principal*aging)/SUM(package_remain_principal),0),2) AS weightedAging,
            IFNULL(MAX(term-aging),0) AS maxSurplusTerm,
            IFNULL(MIN(term-aging),0) AS minSurplusTerm,
            ROUND(IFNULL(SUM(term-aging)/count(1),0),2) AS avgSurplusTerm,
            ROUND(IFNULL(sum(package_remain_principal*(term-aging))/SUM(package_remain_principal),0),2) AS weightedSurplusTerm
        FROM (
            select  package_remain_principal,
                IF(loan_expiry_date &lt; #{packetDate,jdbcType=DATE},
                IF(DAYOFMONTH(loan_expiry_date) - DAYOFMONTH(loan_issue_date) > 27,
                FLOOR(TIMESTAMPDIFF(MONTH,loan_issue_date, loan_expiry_date))+1,
                FLOOR(TIMESTAMPDIFF(MONTH,loan_issue_date, loan_expiry_date))),
                IF(loan_issue_date &lt; #{packetDate,jdbcType=DATE},
                FLOOR(TIMESTAMPDIFF(MONTH,loan_issue_date, #{packetDate,jdbcType=DATE})),
                0)) AS aging,
                IF(DAYOFMONTH(loan_expiry_date) - DAYOFMONTH(loan_issue_date) > 27,
                FLOOR(TIMESTAMPDIFF(MONTH,loan_issue_date, loan_expiry_date))+1,
                FLOOR(TIMESTAMPDIFF(MONTH,loan_issue_date, loan_expiry_date))) AS term
            from t_basic_asset
            where project_id = #{projectId,jdbcType=VARCHAR}
            AND asset_bag_id = #{assetBagId,jdbcType=VARCHAR} ) t
    </select>

    <!--
    根据项目编号,资产包ID,封包日期 查询字段:
    抵押的资产笔数,抵押的资产余额,汽车的评估价格,抵押资产余额占比,抵押资产笔数占比,加权平均抵押率
    -->
    <select id="getAssetGuarantyInfo" resultType="java.util.Map">
        SELECT
            t.a mortgageAssetBalance,
            t.b mortgageAssetCount,
            ROUND((t.a / y.c) *100, 2) mortgageAssetBalanceRatio,
            ROUND((t.b / y.d) *100, 2) mortgageAssetCountRatio,
            q.*
        FROM
            (
                SELECT
                    sum(package_remain_principal) a,
                    COUNT(*) b
                FROM
                    t_basic_asset
                WHERE
                    project_id =  #{projectId,jdbcType=VARCHAR}
                AND asset_bag_id = #{assetBagId,jdbcType=VARCHAR}
                AND asset_type = '汽车抵押贷款'
                AND guarantee_type = '抵押担保'
            ) t,
            (
                SELECT
                    sum(package_remain_principal) c,
                    COUNT(*) d
                FROM
                    t_basic_asset
                WHERE
                    project_id =  #{projectId,jdbcType=VARCHAR}
                AND asset_bag_id = #{assetBagId,jdbcType=VARCHAR}
                AND asset_type = '汽车抵押贷款'
            ) y,
            (
                SELECT
                    ROUND(sum(t.c), 2) mortgageInitValuation,
                    -- 汽车的评估价格,
                    ROUND(
                        (sum(t.a *(t.b / t.c)) / sum(t.a))*100,
                        2
                    ) weightAvgMortgageRate -- 加权平均抵押率
                FROM
                    (
                        SELECT
                            t1.package_remain_principal a,
                            t1.contract_amount b,
                            t2.pa c
                        FROM
                            t_basic_asset t1
                        JOIN (
                            SELECT
                                serial_number,
                                sum(pawn_value) pa
                            FROM
                                t_guarantycarinfo
                            WHERE
                                project_id =  #{projectId,jdbcType=VARCHAR}
                            GROUP BY
                                serial_number
                        ) t2 ON t1.serial_number = t2.serial_number
                        WHERE
                            t1.project_id =  #{projectId,jdbcType=VARCHAR}
                        AND t1.asset_bag_id = #{assetBagId,jdbcType=VARCHAR}
                        AND t1.asset_type = '汽车抵押贷款'
                        AND t1.guarantee_type = '抵押担保'
                    ) t
            ) q
    </select>
