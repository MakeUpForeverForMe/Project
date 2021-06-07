<!--
        ������Ŀ���,�ʲ���ID ��ѯ�ֶ�:
        ��������ͬ���,������С��ͬ���,ƽ����ͬ���,
        ��������������,�������С������,�����ƽ��������,��Ȩƽ��������,
        �������������,������С������,ƽ��������,��Ȩƽ������������
    -->
    <select id="getAssetPackagePacketSituation" resultType="java.util.Map">
        SELECT
            COUNT(1) assetCount,
            -- �ʲ�����,
            MAX(contract_amount) singleMaxOntractAmount,
            -- ��������ͬ���
            MIN(contract_amount) singleMinOntractAmount,
            -- ������С��ͬ���
            ROUND(
                (
                    SUM(contract_amount) / COUNT(*)
                ),
                2
            ) avgContractAmount,
            -- ƽ����ͬ���
            MAX(annual_income) maxBorrowerAnnualIncome,
            -- ��������������
            MIN(annual_income) minBorrowerAnnualIncome,
            -- �������С������
            ROUND(
                SUM(annual_income) / COUNT(*),
                2
            ) avgBorrowerAnnualIncome,
            -- �����ƽ��������
            ROUND(
                sum(
                    package_remain_principal * annual_income
                ) / SUM(package_remain_principal),
                2
            ) weightAvgBorrowerAnnualIncome,
            -- ��Ȩƽ��������
            MAX(contract_interest_rate) singleMaxInterestRate,
            -- �������������
            MIN(contract_interest_rate) singleMinInterestRate,
            -- ������С������
            ROUND(
                SUM(contract_interest_rate) / COUNT(*),
                2
            ) avgInterestRate,
            -- ƽ��������
            ROUND(
                sum(
                    package_remain_principal * contract_interest_rate
                ) / SUM(package_remain_principal),
                2
            ) weightAvgInterestRate -- ��Ȩƽ������������
        FROM
            t_basic_asset
        WHERE
            project_id = #{projectId,jdbcType=VARCHAR}
        AND asset_bag_id = #{assetBagId,jdbcType=VARCHAR}
    </select>
    <!--
        ������Ŀ���,�ʲ���ID,������� ��ѯ�ֶ�:
        ����������,��Ϣ������,
        ������󱾽����,������С�������,ƽ���������
    -->
    <select id="getPrincipalBalanceInfo" resultType="java.util.Map">
    SELECT
        sum(t1.should_repay_principal) principalBalance, -- ����������
        sum(t1.should_repay_principal+should_repay_interest) principalInterestBalance, -- ��Ϣ������
        max(t2.package_remain_principal) singleMaxPrincipalBalance, -- ������󱾽����
        MIN(t2.package_remain_principal) singleMinPrincipalBalance, -- ������С�������
        ROUND(sum(t1.should_repay_principal)/(SELECT
                            COUNT(*) assetCount
                        FROM
                        t_basic_asset
                        WHERE
                            project_id = #{projectId,jdbcType=VARCHAR}
                        AND asset_bag_id = #{assetBagId,jdbcType=VARCHAR}
                        ),2) avgPrincipalBalance -- ƽ���������
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
        ������Ŀ���,�ʲ���ID,������� ��ѯ�ֶ�:
        �������ծ���,������Сծ���,ƽ��ծ���,��Ȩƽ��ծ���
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
        ������Ŀ���,�ʲ���ID,������� ��ѯ�ֶ�:
        ���������,�����ƽ���������
    -->
    <select id="getBorrowersCountAndRestPrincipalAverage" resultType="java.util.Map">
    SELECT
        t2.borrowersCount borrowerCount,
        -- ���������
        (
            t1.borrowersRestPrincipalAverage / t2.borrowersCount
        ) borrowerAvgPrincipalBalance
        -- �����ƽ���������
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
            -- ���������
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
        ������Ŀ���,�ʲ���ID,������� ��ѯ�ֶ�:
        ��������ͬ����,������С��ͬ����,ƽ����ͬ����,��Ȩƽ����ͬ����
    -->
    <select id="getContractPeriodInfo" resultType="java.util.Map">
        SELECT
            MAX(periods) singleMaxContractPeriods, -- ��������ͬ����
            MIN(periods) singleMinContractPeriods, -- ������С��ͬ����
            ROUND(sum(periods)/COUNT(*),2) avgContractPeriods, -- ƽ����ͬ����
            ROUND(sum(package_remain_principal * periods) / SUM(package_remain_principal),2) weightAvgContractPeriods -- ��Ȩƽ����ͬ����
        FROM
            t_basic_asset
        WHERE
            project_id = #{projectId,jdbcType=VARCHAR}
        AND asset_bag_id = #{assetBagId,jdbcType=VARCHAR}
    </select>
    <!--
        ������Ŀ���,�ʲ���ID,������� ��ѯ�ֶ�:
        �������ʣ������,������Сʣ������,ƽ��ʣ������,��Ȩƽ��ʣ������
    -->
    <select id="getAssetResiduePeriodInfo" resultType="java.util.Map">
        SELECT
            MAX(package_remain_periods) singleMaxRemainPeriods, -- �������ʣ������
            MIN(package_remain_periods) singleMinRemainPeriods, -- ������Сʣ������
            ROUND(sum(package_remain_periods)/COUNT(*),2) avgRemainPeriods, -- ƽ��ʣ������
            ROUND(sum(package_remain_principal * package_remain_periods) / SUM(package_remain_principal),2) weightAvgRemainPeriods -- ��Ȩƽ��ʣ������
        FROM
            t_basic_asset
        WHERE
            project_id = #{projectId,jdbcType=VARCHAR}
        AND asset_bag_id = #{assetBagId,jdbcType=VARCHAR}
    </select>
    <!--
        ������Ŀ���,�ʲ���ID,������� ��ѯ�ֶ�:
        ��������ѻ�����,������С�ѻ�����,ƽ���ѻ�����,��Ȩƽ���ѻ�����
    -->
    <select id="getAssetRepayPeriodInfo" resultType="java.util.Map">
        SELECT
            MAX(periods-package_remain_periods) singleMaxReplayPeriods, -- ��������ѻ�����
            MIN(periods-package_remain_periods) singleMinReplayPeriods, -- ������С�ѻ�����
            ROUND(sum(periods-package_remain_periods)/COUNT(*),2) avgReplayPeriods, -- ƽ���ѻ�����
            ROUND(sum(package_remain_principal * (periods-package_remain_periods)) / SUM(package_remain_principal),2) weightAvgReplayPeriods -- ��Ȩƽ���ѻ�����
        FROM
            t_basic_asset
        WHERE
            project_id = #{projectId,jdbcType=VARCHAR}
        AND asset_bag_id = #{assetBagId,jdbcType=VARCHAR}
    </select>
    <!--
    ������Ŀ���,�ʲ���ID,������� ��ѯ�ֶ�:
    �����������,������С����,ƽ������,��Ȩƽ������
    -->
    <select id="getAgeInfo" resultType="java.util.Map">
    SELECT
        COALESCE (MAX(age), 0) maxBorrowerAge,
        -- �����������
        COALESCE (MIN(age), 0) minBorrowerAge,
        -- ������С����
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
        -- ƽ������
        COALESCE (
            sum(
                packagePrincipalBalance * age
            ) / SUM(packagePrincipalBalance),
            0
        ) weightAvgBorrowerAge -- ��Ȩƽ������
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
    ������Ŀ���,�ʲ���ID,������� ��ѯ�ֶ�:
    ��С����,�������,ƽ������,��Ȩƽ������,��С��ͬʣ������,����ͬʣ������,ƽ����ͬʣ������,��Ȩƽ��ʣ������
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
    ������Ŀ���,�ʲ���ID,������� ��ѯ�ֶ�:
    ��Ѻ���ʲ�����,��Ѻ���ʲ����,�����������۸�,��Ѻ�ʲ����ռ��,��Ѻ�ʲ�����ռ��,��Ȩƽ����Ѻ��
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
                AND asset_type = '������Ѻ����'
                AND guarantee_type = '��Ѻ����'
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
                AND asset_type = '������Ѻ����'
            ) y,
            (
                SELECT
                    ROUND(sum(t.c), 2) mortgageInitValuation,
                    -- �����������۸�,
                    ROUND(
                        (sum(t.a *(t.b / t.c)) / sum(t.a))*100,
                        2
                    ) weightAvgMortgageRate -- ��Ȩƽ����Ѻ��
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
                        AND t1.asset_type = '������Ѻ����'
                        AND t1.guarantee_type = '��Ѻ����'
                    ) t
            ) q
    </select>
