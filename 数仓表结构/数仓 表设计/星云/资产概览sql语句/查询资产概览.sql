SELECT
        IFNULL(count(*), 0) AS 'assetCount', -- �ʲ�����
        IFNULL(count(DISTINCT document_num),0) AS 'borrowersNumber', -- ���������
        IFNULL(MAX(contract_amount), 0) AS 'maximumContractAmount', -- ��������ͬ���
        IFNULL(MIN(contract_amount), 0) AS 'minimumContractAmount', -- ������С��ͬ���
        ROUND(IFNULL(SUM(contract_amount) / count(*),0),2)
        AS 'averageContractAmount', -- ƽ����ͬ���
        IFNULL(MAX(annual_income), 0) AS 'maximumAnnualIncome', -- ��������������
        IFNULL(MIN(annual_income), 0) AS 'minimumAnnualIncome', -- �������С������
        ROUND(IFNULL(SUM(annual_income) / count(*),0),2)
        AS 'averageAnnualIncome', -- �����ƽ��������
        ROUND(IFNULL(SUM(annual_income * balance) / SUM(balance),0),2)
        AS 'weightedAnnualIncome', -- ����˼�Ȩƽ��������
        IFNULL(MAX(COALESCE (annual_income / balance,0)),0)
        AS 'maximumIncomeRatio', -- ��������ծ���
        IFNULL(MIN(COALESCE (annual_income / balance,0)),0)
        AS 'minimumIncomeRatio', -- �������Сծ���
        ROUND(IFNULL(SUM(COALESCE (annual_income / balance,0)) / count(*),0),2)
        AS 'averageIncomeRatio', -- �����ƽ��ծ���
        ROUND(IFNULL(SUM(annual_income) / SUM(balance),0),2)
        AS 'weightedIncomeRatio', -- ����˼�Ȩƽ��ծ���
        COALESCE (MAX(age), 0) AS 'maximumAge', -- �����������
        COALESCE (MIN(age), 0) AS 'minimumAge', -- ������С����
        COALESCE ((SUM(age)), 0) AS 'sumAge', -- ������
        ROUND(IFNULL(sum(balance * age) / SUM(balance),0),2) AS 'weightedAge', -- ��Ȩƽ������
        IFNULL(SUM(balance),0) AS 'principal', -- �������
        IFNULL(SUM(remain_interest + balance),0) AS 'interest', -- ��Ϣ���
        ROUND(IFNULL(SUM(balance*IFNULL(CAST(year_loan_rate AS DECIMAL(20,6)),0))/SUM(balance),0),2)
        AS 'interestRate', -- ��Ȩƽ��������
        ROUND(IFNULL(SUM(balance*remain_term)/SUM(balance),0),2)
        AS 'remainingPeriods', -- ��Ȩƽ��ʣ������
        IFNULL(MAX(balance),0) AS 'maximumPrincipal', -- ������󱾽����
        IFNULL(MIN(balance),0) AS 'minimumPrincipal', -- ������С�������
        IFNULL(MAX(total_term),0) AS 'maximumNumberPeriods', -- ��������ͬ����
        IFNULL(MIN(total_term),0) AS 'minimumberPeriods', -- ������С��ͬ����
        IFNULL(SUM(total_term),0) AS 'sumNumberPeriods', -- �ܺ�ͬ����
        ROUND(IFNULL(SUM(balance*total_term)/SUM(balance),0),2)
        AS 'weightedNumberPeriods', -- ��Ȩƽ����ͬ����
        IFNULL(MAX(remain_term),0) AS 'maximumNumberIssues', -- �������ʣ���ͬ����
        IFNULL(MIN(remain_term),0) AS 'minimumNumberIssues', -- ������Сʣ���ͬ����
        IFNULL(SUM(remain_term),0) AS 'sumNumberIssues', -- �ʲ�ʣ���ͬ������
        ROUND(IFNULL(SUM(balance*remain_term)/SUM(balance),0),2)
        AS 'weightedNumberIssues', -- ��Ȩƽ��ʣ���ͬ����
        IFNULL(MAX(already_repay_term),0) AS 'maximumNumberOrders', -- ��������ѻ���ͬ����
        IFNULL(MIN(already_repay_term),0) AS 'minimumNumberOrders', -- ������С�ѻ���ͬ����
        IFNULL(SUM(already_repay_term),0) AS 'sumNumberOrders', -- �ѻ��ܺ�ͬ����
        ROUND(IFNULL(SUM(balance*already_repay_term)/SUM(balance),0),2)
        AS 'weightedNumberOrders', -- ��Ȩƽ���ѻ���ͬ����
        COALESCE(MAX(CAST(year_loan_rate AS DECIMAL(20,6))),0) AS 'maximumInterestRate', -- �������������
        COALESCE(MIN(CAST(year_loan_rate AS DECIMAL(20,6))),0) AS 'minimumInterestRate', -- ������С������
        COALESCE(SUM(CAST(year_loan_rate AS DECIMAL(20,6))),0) AS 'sumInterestRate', -- ��������
        IFNULL(MIN(aging),0) AS 'minAging', -- ��С����
        IFNULL(MAX(aging),0) AS 'maxAging',	-- �������
        IFNULL(SUM(aging),0) AS 'sumAging', -- ������
        ROUND(IFNULL(SUM(aging * balance) / SUM(balance),0),2) AS 'weightedAging', -- ��Ȩƽ������
        IFNULL(MIN(term-aging),0) AS 'minSurplusTerm', -- ��С��ͬʣ������
        IFNULL(MAX(term-aging),0) AS 'maxSurplusTerm',	-- ����ͬʣ������
        IFNULL(SUM(term-aging),0) AS 'sumSurplusTerm', -- �ܺ�ͬʣ������
        ROUND(IFNULL(SUM((term-aging) * balance) / SUM(balance),0),2) AS 'weightedSurplusTerm' -- ��Ȩƽ��ʣ������
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
            AND asset_status != '�ѽ���'
        </if> ) t2 ON t2.serial_number = t1.serial_number


-- ��ѯ��Ѻ����Ϣ
SELECT
        IFNULL(SUM(t2.remain_principal), 0) AS 'assetsBalance',	-- ��Ѻ���ʲ����
        IFNULL(count(*), 0) AS 'frequencyAssets',	-- ��Ѻ���ʲ�����
        IFNULL(SUM(t3.pawn), 0) AS 'initialAssessedMortgage',	-- ��Ѻ�ĳ�ʼ������ֵ
        ROUND(IFNULL(SUM(t1.contract_amount / t3.pawn * t2.remain_principal) / SUM(t2.remain_principal) * 100,0),2)
        AS 'weightedAverageMortgageRate'	-- ��Ȩƽ����Ѻ��
        FROM
        (
            select project_id, serial_number, contract_amount from t_basic_asset
            where STATUS != - 1 AND project_id = #{queryMap.projectId,jdbcType=VARCHAR}
            and guarantee_type = '��Ѻ����' and asset_type = '������Ѻ����'
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
                AND asset_status != '�ѽ���'
            </if>
        ) t2
        on t1.serial_number = t2.serial_number
        INNER JOIN
        (
            select serial_number, IFNULL(SUM(pawn_value), 0) AS pawn from t_guarantycarinfo
            where project_id = #{queryMap.projectId,jdbcType=VARCHAR} group by serial_number
        ) t3
        on t1.serial_number = t3.serial_number

