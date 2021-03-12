<!-- δ���������ֲ� -->
    <select id="principalBalanceDistribute" resultType="java.util.Map">
        SELECT
        tbl.remain_principal_region AS loanDim,
        ROUND(SUM(remain_principal),2) AS PrincipalAmount,
        ROUND(IFNULL(SUM(remain_principal)/@sal*100,0),2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(IFNULL(COUNT(1)/@rn*100,0),2) AS loanNumRatio,
        ROUND(IFNULL(SUM(remain_principal)/COUNT(1),0),2) AS avgAmount
        FROM (
        SELECT
        remain_principal,
        CASE
        WHEN (remain_principal &gt; 500000 AND remain_principal &lt;= 1000000) THEN '50��Ԫ��������-100��Ԫ������'
        WHEN (remain_principal &gt; 400000 AND remain_principal &lt;= 500000) THEN '40��Ԫ��������-50��Ԫ������'
        WHEN (remain_principal &gt; 300000 AND remain_principal &lt;= 400000) THEN '30��Ԫ��������-40��Ԫ������'
        WHEN (remain_principal &gt; 200000 AND remain_principal &lt;= 300000) THEN '20��Ԫ��������-30��Ԫ������'
        WHEN (remain_principal &gt; 100000 AND remain_principal &lt;= 200000) THEN '10��Ԫ��������-20��Ԫ������'
        WHEN (remain_principal &gt; 50000 AND remain_principal &lt;= 100000) THEN '5��Ԫ��������-10��Ԫ������'
        WHEN (remain_principal &lt;= 50000) THEN '5��Ԫ����������'
        ELSE '��������'
        END AS remain_principal_region,
        CASE
        WHEN (remain_principal &gt; 500000 AND remain_principal &lt;= 1000000) THEN 7
        WHEN (remain_principal &gt; 400000 AND remain_principal &lt;= 500000) THEN 6
        WHEN (remain_principal &gt; 300000 AND remain_principal &lt;= 400000) THEN 5
        WHEN (remain_principal &gt; 200000 AND remain_principal &lt;= 300000) THEN 4
        WHEN (remain_principal &gt; 100000 AND remain_principal &lt;= 200000) THEN 3
        WHEN (remain_principal &gt; 50000 AND remain_principal &lt;= 100000) THEN 2
        WHEN (remain_principal &lt;= 50000) THEN 1
        ELSE 100
        END AS orderSeq
        FROM (
        SELECT
        a.remain_principal,
        @sal := @sal + IFNULL(remain_principal,0) AS remain_principals,
        @rn := @rn + 1 AS rn FROM (SELECT @sal := 0,@rn := 0) t0,
        t_assetaccountcheck a, (
        SELECT project_id, serial_number
        FROM t_basic_asset
        WHERE STATUS != - 1
        AND project_id = #{queryMap.projectId,jdbcType=VARCHAR}
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
        </if> ) b WHERE a.project_id = b.project_id AND a.serial_number = b.serial_number
        AND data_extract_time = #{queryMap.endDate,jdbcType=DATE}
        <if test="queryMap.isAssetStatus != 1">
            AND asset_status != '�ѽ���'
        </if> )t )tbl
        GROUP BY tbl.remain_principal_region,tbl.orderSeq
        order by tbl.orderSeq
    </select>

    <!-- �ʲ����ʷֲ� -->
    <select id="loanRateDistribute" resultType="java.util.Map">
        SELECT
        tbl.remain_principal_region AS loanDim,
        ROUND(SUM(remain_principal),2) AS PrincipalAmount,
        ROUND(IFNULL(SUM(remain_principal)/@sal*100,0),2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(IFNULL(COUNT(1)/@rn*100,0),2) AS loanNumRatio,
        ROUND(IFNULL(SUM(remain_principal)/COUNT(1),0),2) AS avgAmount
        FROM (
        SELECT
        remain_principal,
        CASE
        WHEN (year_loan_rate &gt; 20.0) THEN '20%������������'
        WHEN (year_loan_rate &gt; 15.0 AND year_loan_rate &lt;= 20.0) THEN '15%��������-20%������'
        WHEN (year_loan_rate &gt; 10.0 AND year_loan_rate &lt;= 15.0) THEN '10%��������-15%������'
        WHEN (year_loan_rate &gt; 5.0 AND year_loan_rate &lt;= 10.0) THEN '5%��������-10%������'
        WHEN (year_loan_rate &lt;= 5.0) THEN '5%����������'
        ELSE '��������'
        END AS remain_principal_region,
        CASE
        WHEN (year_loan_rate &gt; 20.0) THEN 5
        WHEN (year_loan_rate &gt; 15.0 AND year_loan_rate &lt;= 20.0) THEN 4
        WHEN (year_loan_rate &gt; 10.0 AND year_loan_rate &lt;= 15.0) THEN 3
        WHEN (year_loan_rate &gt; 5.0 AND year_loan_rate &lt;= 10.0) THEN 2
        WHEN (year_loan_rate &lt;= 5.0) THEN 1
        ELSE 100
        END AS orderSeq
        FROM (
        SELECT
        a.year_loan_rate, a.remain_principal,
        @sal := @sal + IFNULL(remain_principal,0) AS remain_principals,
        @rn := @rn + 1 AS rn FROM (SELECT @sal := 0,@rn := 0) t0,
        t_assetaccountcheck a, (
        SELECT project_id, serial_number
        FROM t_basic_asset
        WHERE STATUS != - 1
        AND project_id = #{queryMap.projectId,jdbcType=VARCHAR}
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
        </if> ) b WHERE a.project_id = b.project_id AND a.serial_number = b.serial_number
        AND data_extract_time = #{queryMap.endDate,jdbcType=DATE}
        <if test="queryMap.isAssetStatus != 1">
            AND asset_status != '�ѽ���'
        </if> )t )tbl
        GROUP BY tbl.remain_principal_region,tbl.orderSeq
        order by tbl.orderSeq
    </select>

    <!-- �����ͬ�����ֲ� -->
    <select id="contractDistribute" resultType="java.util.Map">
        SELECT
        CONCAT(total_term,'��') AS loanDim,
        ROUND(SUM(remain_principal),2) AS PrincipalAmount,
        ROUND(IFNULL(SUM(remain_principal)/@sal*100,0),2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(IFNULL(COUNT(1)/@rn*100,0),2) AS loanNumRatio,
        ROUND(IFNULL(SUM(remain_principal)/COUNT(1),0),2) AS avgAmount
        FROM (
        SELECT
        a.total_term, a.remain_principal,
        @sal := @sal + IFNULL(remain_principal,0) AS remain_principals,
        @rn := @rn + 1 AS rn FROM (SELECT @sal := 0,@rn := 0) t0,
        t_assetaccountcheck a, (
        SELECT project_id, serial_number
        FROM t_basic_asset
        WHERE STATUS != - 1
        AND project_id = #{queryMap.projectId,jdbcType=VARCHAR}
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
        </if> ) b WHERE a.project_id = b.project_id AND a.serial_number = b.serial_number
        AND data_extract_time = #{queryMap.endDate,jdbcType=DATE}
        <if test="queryMap.isAssetStatus != 1">
            AND asset_status != '�ѽ���'
        </if> ) t
        GROUP BY loanDim
        order by loanDim
    </select>

    <!-- �ʲ�ʣ�������ֲ� -->
    <select id="remainingDistribute" resultType="java.util.Map">
        SELECT
        tbl.remain_principal_region AS loanDim,
        ROUND(SUM(remain_principal),2) AS PrincipalAmount,
        ROUND(IFNULL(SUM(remain_principal)/@sal*100,0),2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(IFNULL(COUNT(1)/@rn*100,0),2) AS loanNumRatio,
        ROUND(IFNULL(SUM(remain_principal)/COUNT(1),0),2) AS avgAmount
        FROM (
        SELECT
        remain_principal,
        CASE
        WHEN (remain_term &gt; 48) THEN '48��(����)~���� '
        WHEN (remain_term &gt; 42 AND remain_term &lt;= 48) THEN '43��(��)~48��(��) '
        WHEN (remain_term &gt; 36 AND remain_term &lt;= 42) THEN '37��(��)~42��(��) '
        WHEN (remain_term &gt; 30 AND remain_term &lt;= 36) THEN '31��(��)~36��(��) '
        WHEN (remain_term &gt; 24 AND remain_term &lt;= 30) THEN '25��(��)~30��(��) '
        WHEN (remain_term &gt; 18 AND remain_term &lt;= 24) THEN '19��(��)~24��(��) '
        WHEN (remain_term &gt; 12 AND remain_term &lt;= 18) THEN '13��(��)~18��(��) '
        WHEN (remain_term &gt; 6 AND remain_term &lt;= 12) THEN '7��(��)~12��(��) '
        WHEN (remain_term &gt; 0 AND remain_term &lt;= 6) THEN '1��(��)~6��(��) '
        WHEN (remain_term = 0) THEN '0��(��) '
        ELSE '��������'
        END AS remain_principal_region,
        CASE
        WHEN (remain_term &gt; 48) THEN 9
        WHEN (remain_term &gt; 42 AND remain_term &lt;= 48) THEN 8
        WHEN (remain_term &gt; 36 AND remain_term &lt;= 42) THEN 7
        WHEN (remain_term &gt; 30 AND remain_term &lt;= 36) THEN 6
        WHEN (remain_term &gt; 24 AND remain_term &lt;= 30) THEN 5
        WHEN (remain_term &gt; 18 AND remain_term &lt;= 24) THEN 4
        WHEN (remain_term &gt; 12 AND remain_term &lt;= 18) THEN 3
        WHEN (remain_term &gt; 6 AND remain_term &lt;= 12) THEN 2
        WHEN (remain_term &gt; 0 AND remain_term &lt;= 6) THEN 1
        WHEN (remain_term = 0) THEN 0
        ELSE 100
        END AS orderSeq
        FROM (
        SELECT
        a.remain_term, a.remain_principal,
        @sal := @sal + IFNULL(remain_principal,0) AS remain_principals,
        @rn := @rn + 1 AS rn FROM (SELECT @sal := 0,@rn := 0) t0,
        t_assetaccountcheck a, (
        SELECT project_id, serial_number
        FROM t_basic_asset
        WHERE STATUS != - 1
        AND project_id = #{queryMap.projectId,jdbcType=VARCHAR}
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
        </if> ) b WHERE a.project_id = b.project_id AND a.serial_number = b.serial_number
        AND data_extract_time = #{queryMap.endDate,jdbcType=DATE}
        <if test="queryMap.isAssetStatus != 1">
            AND asset_status != '�ѽ���'
        </if> )t )tbl
        GROUP BY tbl.remain_principal_region,tbl.orderSeq
        order by tbl.orderSeq
    </select>

    <!-- �ʲ��ѻ������ֲ� -->
    <select id="applicableDistribute" resultType="java.util.Map">
        SELECT
        tbl.remain_principal_region AS loanDim,
        ROUND(SUM(remain_principal),2) AS PrincipalAmount,
        ROUND(IFNULL(SUM(remain_principal)/@sal*100,0),2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(IFNULL(COUNT(1)/@rn*100,0),2) AS loanNumRatio,
        ROUND(IFNULL(SUM(remain_principal)/COUNT(1),0),2) AS avgAmount
        FROM (
        SELECT
        remain_principal,
        CASE
        WHEN (already_repay_term = 0) THEN '0�� '
        ELSE CONCAT('�ѻ�',FLOOR((already_repay_term-1)/6)*6+1,'��(��)~',CEILING(already_repay_term/6)*6,'��(��)')
        END AS remain_principal_region,
        CASE
        WHEN (already_repay_term = 0) THEN 0
        ELSE CEILING(already_repay_term/6)
        END AS orderSeq
        FROM (
        SELECT
        a.already_repay_term, a.remain_principal,
        @sal := @sal + IFNULL(remain_principal,0) AS remain_principals,
        @rn := @rn + 1 AS rn FROM (SELECT @sal := 0,@rn := 0) t0,
        t_assetaccountcheck a, (
        SELECT project_id, serial_number
        FROM t_basic_asset
        WHERE STATUS != - 1
        AND project_id = #{queryMap.projectId,jdbcType=VARCHAR}
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
        </if> ) b WHERE a.project_id = b.project_id AND a.serial_number = b.serial_number
        AND data_extract_time = #{queryMap.endDate,jdbcType=DATE}
        <if test="queryMap.isAssetStatus != 1">
            AND asset_status != '�ѽ���'
        </if> )t )tbl
        GROUP BY tbl.remain_principal_region,tbl.orderSeq
        order by tbl.orderSeq
    </select>

    <!--����ֲ�-->
    <select id="agingDistribute" resultType="java.util.Map">
        SELECT
        tbl.remain_principal_region AS loanDim,
        ROUND(SUM(remain_principal),2) AS PrincipalAmount,
        ROUND(IFNULL(SUM(remain_principal)/@sal*100,0),2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(IFNULL(COUNT(1)/@rn*100,0),2) AS loanNumRatio,
        ROUND(IFNULL(SUM(remain_principal)/COUNT(1),0),2) AS avgAmount
        FROM (
        SELECT
        remain_principal,
        CASE
        WHEN (aging = 0) THEN '0(��) '
        ELSE CONCAT(FLOOR((aging-1)/6)*6+1,'(��)~',CEILING(aging/6)*6,'(��)')
        END AS remain_principal_region,
        CASE
        WHEN (aging = 0) THEN 0
        ELSE CEILING(aging/6)
        END AS orderSeq
        FROM (
        SELECT
        b.aging, a.remain_principal,
        @sal := @sal + IFNULL(remain_principal,0) AS remain_principals,
        @rn := @rn + 1 AS rn FROM (SELECT @sal := 0,@rn := 0) t0,
        t_assetaccountcheck a, (
        SELECT project_id, serial_number,
        IF(loan_expiry_date &lt; #{queryMap.endDate,jdbcType=DATE},
        IF(DAYOFMONTH(loan_expiry_date) - DAYOFMONTH(loan_issue_date) > 27,
        FLOOR(TIMESTAMPDIFF(MONTH,loan_issue_date, loan_expiry_date))+1,
        FLOOR(TIMESTAMPDIFF(MONTH,loan_issue_date, loan_expiry_date))),
        IF(loan_issue_date &lt; #{queryMap.endDate,jdbcType=DATE},
        FLOOR(TIMESTAMPDIFF(MONTH,loan_issue_date, #{queryMap.endDate,jdbcType=DATE})),
        0)) AS aging
        FROM t_basic_asset
        WHERE STATUS != - 1
        AND project_id = #{queryMap.projectId,jdbcType=VARCHAR}
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
        </if> ) b WHERE a.project_id = b.project_id AND a.serial_number = b.serial_number
        AND data_extract_time = #{queryMap.endDate,jdbcType=DATE}
        <if test="queryMap.isAssetStatus != 1">
            AND asset_status != '�ѽ���'
        </if> )t )tbl
        GROUP BY tbl.remain_principal_region,tbl.orderSeq
        order by tbl.orderSeq
    </select>

    <!--��ͬʣ�����޷ֲ�-->
    <select id="surplusTermDistribute" resultType="java.util.Map">
        SELECT
        tbl.remain_principal_region AS loanDim,
        ROUND(SUM(remain_principal),2) AS PrincipalAmount,
        ROUND(IFNULL(SUM(remain_principal)/@sal*100,0),2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(IFNULL(COUNT(1)/@rn*100,0),2) AS loanNumRatio,
        ROUND(IFNULL(SUM(remain_principal)/COUNT(1),0),2) AS avgAmount
        FROM (
        SELECT
        remain_principal,
        CASE
        WHEN (surplusTerm = 0) THEN '0(��) '
        ELSE CONCAT(FLOOR((surplusTerm-1)/6)*6+1,'(��)~',CEILING(surplusTerm/6)*6,'(��)')
        END AS remain_principal_region,
        CASE
        WHEN (surplusTerm = 0) THEN 0
        ELSE CEILING(surplusTerm/6)
        END AS orderSeq
        FROM (
        SELECT
        (b.term-b.aging) as surplusTerm, a.remain_principal,
        @sal := @sal + IFNULL(remain_principal,0) AS remain_principals,
        @rn := @rn + 1 AS rn FROM (SELECT @sal := 0,@rn := 0) t0,
        t_assetaccountcheck a, (
        SELECT project_id, serial_number,
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
        FROM t_basic_asset
        WHERE STATUS != - 1
        AND project_id = #{queryMap.projectId,jdbcType=VARCHAR}
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
        </if> ) b WHERE a.project_id = b.project_id AND a.serial_number = b.serial_number
        AND data_extract_time = #{queryMap.endDate,jdbcType=DATE}
        <if test="queryMap.isAssetStatus != 1">
            AND asset_status != '�ѽ���'
        </if> )t )tbl
        GROUP BY tbl.remain_principal_region,tbl.orderSeq
        order by tbl.orderSeq
    </select>

    <!-- �ʲ����ʽ�ֲ� -->
    <select id="reimbursementDistribute" resultType="java.util.Map">
        SELECT
        repayment_type AS loanDim,
        ROUND(SUM(remain_principal),2) AS PrincipalAmount,
        ROUND(IFNULL(SUM(remain_principal)/@sal*100,0),2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(IFNULL(COUNT(1)/@rn*100,0),2) AS loanNumRatio,
        ROUND(IFNULL(SUM(remain_principal)/COUNT(1),0),2) AS avgAmount
        FROM (
        SELECT
        b.repayment_type, a.remain_principal,
        @sal := @sal + IFNULL(remain_principal,0) AS remain_principals,
        @rn := @rn + 1 AS rn FROM (SELECT @sal := 0,@rn := 0) t0,
        t_assetaccountcheck a, (
        SELECT
        project_id, serial_number, repayment_type
        FROM t_basic_asset
        WHERE STATUS != - 1
        AND project_id = #{queryMap.projectId,jdbcType=VARCHAR}
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
        </if> ) b WHERE a.project_id = b.project_id AND a.serial_number = b.serial_number
        AND data_extract_time = #{queryMap.endDate,jdbcType=DATE}
        <if test="queryMap.isAssetStatus != 1">
            AND asset_status != '�ѽ���'
        </if> ) t
        GROUP BY loanDim
    </select>

    <!-- ���������ֲ� -->
    <select id="ageDistribute" resultType="java.util.Map">
        SELECT
        tbl.remain_principal_region AS loanDim,
        ROUND(SUM(remain_principal),2) AS PrincipalAmount,
        ROUND(IFNULL(SUM(remain_principal)/@sal*100,0),2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(IFNULL(COUNT(1)/@rn*100,0),2) AS loanNumRatio,
        ROUND(IFNULL(SUM(remain_principal)/COUNT(1),0),2) AS avgAmount
        FROM (
        SELECT
        remain_principal,
        CASE
        WHEN (age &gt; 50) THEN '50�꣨���������� '
        WHEN (age &gt; 45 AND age &lt;= 50) THEN '45�꣨������-50�꣨����'
        WHEN (age &gt; 40 AND age &lt;= 45) THEN '40�꣨������-45�꣨����'
        WHEN (age &gt; 35 AND age &lt;= 40) THEN '35�꣨������-40�꣨����'
        WHEN (age &gt; 30 AND age &lt;= 35) THEN '30�꣨������-35�꣨����'
        WHEN (age &gt; 25 AND age &lt;= 30) THEN '25�꣨������-30�꣨����'
        WHEN (age &gt; 20 AND age &lt;= 25) THEN '20�꣨������-25�꣨����'
        WHEN (age &gt;= 18 AND age &lt;= 20) THEN '18�꣨����-20�꣨����'
        WHEN (age &lt; 18 ) THEN '18�����£�������'
        ELSE '��������'
        END AS remain_principal_region,
        CASE
        WHEN (age &gt; 50) THEN 8
        WHEN (age &gt; 45 AND age &lt;= 50) THEN 7
        WHEN (age &gt; 40 AND age &lt;= 45) THEN 6
        WHEN (age &gt; 35 AND age &lt;= 40) THEN 5
        WHEN (age &gt; 30 AND age &lt;= 35) THEN 4
        WHEN (age &gt; 25 AND age &lt;= 30) THEN 3
        WHEN (age &gt; 20 AND age &lt;= 25) THEN 2
        WHEN (age &gt;= 18 AND age &lt;= 20) THEN 1
        WHEN (age &lt; 18 ) THEN 0
        ELSE 100
        END AS orderSeq
        FROM (
        SELECT
        b.age, a.remain_principal,
        @sal := @sal + IFNULL(remain_principal,0) AS remain_principals,
        @rn := @rn + 1 AS rn FROM (SELECT @sal := 0,@rn := 0) t0,
        t_assetaccountcheck a, (
        SELECT
        project_id, serial_number, (floor((DATEDIFF(loan_issue_date, birthday)) / 365)) AS age
        FROM t_basic_asset
        WHERE STATUS != - 1
        AND project_id = #{queryMap.projectId,jdbcType=VARCHAR}
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
        </if> ) b WHERE a.project_id = b.project_id AND a.serial_number = b.serial_number
        AND data_extract_time = #{queryMap.endDate,jdbcType=DATE}
        <if test="queryMap.isAssetStatus != 1">
            AND asset_status != '�ѽ���'
        </if> )t )tbl
        GROUP BY tbl.remain_principal_region,tbl.orderSeq
        order by tbl.orderSeq
    </select>

    <!-- �������ҵ�ֲ� -->
    <select id="industryDistribute" resultType="java.util.Map">
        SELECT
        borrower_industry AS loanDim,
        ROUND(SUM(remain_principal),2) AS PrincipalAmount,
        ROUND(IFNULL(SUM(remain_principal)/@sal*100,0),2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(IFNULL(COUNT(1)/@rn*100,0),2) AS loanNumRatio,
        ROUND(IFNULL(SUM(remain_principal)/COUNT(1),0),2) AS avgAmount
        FROM (
        SELECT
        b.borrower_industry, a.remain_principal,
        @sal := @sal + IFNULL(remain_principal,0) AS remain_principals,
        @rn := @rn + 1 AS rn FROM (SELECT @sal := 0,@rn := 0) t0,
        t_assetaccountcheck a, (
        SELECT
        project_id, serial_number, borrower_industry
        FROM t_basic_asset
        WHERE STATUS != - 1
        AND project_id = #{queryMap.projectId,jdbcType=VARCHAR}
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
        </if> ) b WHERE a.project_id = b.project_id AND a.serial_number = b.serial_number
        AND data_extract_time = #{queryMap.endDate,jdbcType=DATE}
        <if test="queryMap.isAssetStatus != 1">
            AND asset_status != '�ѽ���'
        </if> ) t
        GROUP BY loanDim
    </select>

    <!-- �����������ֲ� -->
    <select id="annualIncomeDistribute" resultType="java.util.Map">
        SELECT
        tbl.remain_principal_region AS loanDim,
        ROUND(SUM(remain_principal),2) AS PrincipalAmount,
        ROUND(IFNULL(SUM(remain_principal)/@sal*100,0),2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(IFNULL(COUNT(1)/@rn*100,0),2) AS loanNumRatio,
        ROUND(IFNULL(SUM(remain_principal)/COUNT(1),0),2) AS avgAmount
        FROM (
        SELECT
        remain_principal,
        CASE
        WHEN (annual_income &gt; 1000000) THEN '100��Ԫ������������'
        WHEN (annual_income &gt; 500000 AND annual_income &lt;= 1000000) THEN '50��Ԫ��������-100��Ԫ������'
        WHEN (annual_income &gt; 400000 AND annual_income &lt;= 500000) THEN '40��Ԫ��������-50��Ԫ������'
        WHEN (annual_income &gt; 300000 AND annual_income &lt;= 400000) THEN '30��Ԫ��������-40��Ԫ������'
        WHEN (annual_income &gt; 200000 AND annual_income &lt;= 300000) THEN '20��Ԫ��������-30��Ԫ������'
        WHEN (annual_income &gt; 100000 AND annual_income &lt;= 200000) THEN '10��Ԫ��������-20��Ԫ������'
        WHEN (annual_income &gt; 50000 AND annual_income &lt;= 100000) THEN '5��Ԫ��������-10��Ԫ������'
        WHEN (annual_income &lt;= 50000) THEN '5��Ԫ����������'
        ELSE '��������'
        END AS remain_principal_region,
        CASE
        WHEN (annual_income &gt; 1000000) THEN 8
        WHEN (annual_income &gt; 500000 AND annual_income &lt;= 1000000) THEN 7
        WHEN (annual_income &gt; 400000 AND annual_income &lt;= 500000) THEN 6
        WHEN (annual_income &gt; 300000 AND annual_income &lt;= 400000) THEN 5
        WHEN (annual_income &gt; 200000 AND annual_income &lt;= 300000) THEN 4
        WHEN (annual_income &gt; 100000 AND annual_income &lt;= 200000) THEN 3
        WHEN (annual_income &gt; 50000 AND annual_income &lt;= 100000) THEN 2
        WHEN (annual_income &lt;= 50000) THEN 1
        ELSE 100
        END AS orderSeq
        FROM (
        SELECT
        b.annual_income, a.remain_principal,
        @sal := @sal + IFNULL(remain_principal,0) AS remain_principals,
        @rn := @rn + 1 AS rn FROM (SELECT @sal := 0,@rn := 0) t0,
        t_assetaccountcheck a, (
        SELECT
        project_id, serial_number, annual_income
        FROM t_basic_asset
        WHERE STATUS != - 1
        AND project_id = #{queryMap.projectId,jdbcType=VARCHAR}
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
        </if> ) b WHERE a.project_id = b.project_id AND a.serial_number = b.serial_number
        AND data_extract_time = #{queryMap.endDate,jdbcType=DATE}
        <if test="queryMap.isAssetStatus != 1">
            AND asset_status != '�ѽ���'
        </if> )t )tbl
        GROUP BY tbl.remain_principal_region,tbl.orderSeq
        order by tbl.orderSeq
    </select>

    <!-- ��������õȼ��ֲ� -->
    <select id="creditRatingDistribute" resultType="java.util.Map">
        SELECT
        score_level AS loanDim,
        ROUND(SUM(remain_principal),2) AS PrincipalAmount,
        ROUND(IFNULL(SUM(remain_principal)/@sal*100,0),2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(IFNULL(COUNT(1)/@rn*100,0),2) AS loanNumRatio,
        ROUND(IFNULL(SUM(remain_principal)/COUNT(1),0),2) AS avgAmount
        FROM (
        SELECT
        b.score_level, a.remain_principal,
        @sal := @sal + IFNULL(remain_principal,0) AS remain_principals,
        @rn := @rn + 1 AS rn FROM (SELECT @sal := 0,@rn := 0) t0,
        t_assetaccountcheck a, (
        SELECT
        project_id, serial_number, score_level
        FROM
        (
        SELECT
        t4.project_id,
        t4.serial_number,
        t5.asset_bag_id,
        t4.`status`,
        IFNULL(t5.wind_control_status,'Without') as wind_control_status,
        IFNULL(t5.cheat_level,'-1') as cheat_level ,
        IFNULL(t5.score_range,'-1') as score_range,
        IFNULL(t5.score_level,'-1') as score_level
        from t_basic_asset t4
        left join (
        SELECT
        t1.project_id,
        t1.asset_bag_id,
        t1.serial_number,
        t1.`status`,
        t1.statistics_date,
        t1.wind_control_status,
        t1.wind_control_status_pool,
        t1.cheat_level,
        t1.score_range,
        t1.score_level
        FROM
        (
        SELECT
        t.*,
        IF
        ( @serial_number = t.serial_number, @rank := @rank + 1, @rank := 0 ) AS rank,
        @serial_number := t.serial_number,
        @number := @number + 1,
        @rank,
        @serial_number
        FROM
        (
        SELECT
        project_id,
        asset_bag_id,
        serial_number,
        `status`,
        statistics_date,
        wind_control_status,
        wind_control_status_pool,
        cheat_level,
        score_range,
        score_level
        FROM
        t_asset_wind_control_history
        WHERE
        project_id = #{queryMap.projectId,jdbcType=VARCHAR}
        <![CDATA[
        AND ( statistics_date <= #{queryMap.statisticsDate,jdbcType=VARCHAR})
        ]]>
        ORDER BY
        serial_number,
        create_time DESC
        ) t,
        ( SELECT @rank := 0, @number := 0, @serial_number := '' ) b
        ) t1
        WHERE
        <![CDATA[
        rank < 1
        ]]>
        )t5 on t4.serial_number=t5.serial_number
        WHERE t4.STATUS != - 1 AND t4.project_id = #{queryMap.projectId,jdbcType=VARCHAR}
        ) t2
        WHERE t2.`status` != - 1
        AND t2.project_id = #{queryMap.projectId,jdbcType=VARCHAR}
        <if test="queryMap.bagIdList != null and queryMap.bagIdList.size()>0">
            and (t2.asset_bag_id in
            <foreach close=")" collection="queryMap.bagIdList" item="listItem" open="(" separator=",">
                #{listItem}
            </foreach>
            <if test="queryMap.isNotPackage == 1">
                or t2.asset_bag_id is null
            </if>
            )
        </if>
        <if test="(queryMap.bagIdList == null or queryMap.bagIdList.size() == 0) and queryMap.isNotPackage == 1">
            and t2.asset_bag_id is null
        </if> ) b WHERE a.project_id = b.project_id AND a.serial_number = b.serial_number
        AND data_extract_time = #{queryMap.endDate,jdbcType=DATE}
        <if test="queryMap.isAssetStatus != 1">
            AND asset_status != '�ѽ���'
        </if> ) t3
        GROUP BY loanDim
        order by loanDim
    </select>
    <!-- ����˷���թ�ȼ��ֲ� -->
    <select id="fraudRatingDistribute" resultType="java.util.Map">
        SELECT
        cheat_level AS loanDim,
        ROUND(SUM(remain_principal),2) AS PrincipalAmount,
        ROUND(IFNULL(SUM(remain_principal)/@sal*100,0),2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(IFNULL(COUNT(1)/@rn*100,0),2) AS loanNumRatio,
        ROUND(IFNULL(SUM(remain_principal)/COUNT(1),0),2) AS avgAmount
        FROM (
        SELECT
        b.cheat_level, a.remain_principal,
        @sal := @sal + IFNULL(remain_principal,0) AS remain_principals,
        @rn := @rn + 1 AS rn FROM (SELECT @sal := 0,@rn := 0) t0,
        t_assetaccountcheck a, (
        SELECT
        project_id, serial_number, cheat_level
        FROM
        (
        SELECT
        t4.project_id,
        t4.serial_number,
        t5.asset_bag_id,
        t4.`status`,
        IFNULL(t5.wind_control_status,'Without') as wind_control_status,
        IFNULL(t5.cheat_level,'-1') as cheat_level ,
        IFNULL(t5.score_range,'-1') as score_range,
        IFNULL(t5.score_level,'-1') as score_level
        from t_basic_asset t4
        left join (
        SELECT
        t1.project_id,
        t1.asset_bag_id,
        t1.serial_number,
        t1.`status`,
        t1.statistics_date,
        t1.wind_control_status,
        t1.wind_control_status_pool,
        t1.cheat_level,
        t1.score_range,
        t1.score_level
        FROM
        (
        SELECT
        t.*,
        IF
        ( @serial_number = t.serial_number, @rank := @rank + 1, @rank := 0 ) AS rank,
        @serial_number := t.serial_number,
        @number := @number + 1,
        @rank,
        @serial_number
        FROM
        (
        SELECT
        project_id,
        asset_bag_id,
        serial_number,
        `status`,
        statistics_date,
        wind_control_status,
        wind_control_status_pool,
        cheat_level,
        score_range,
        score_level
        FROM
        t_asset_wind_control_history
        WHERE
        project_id = #{queryMap.projectId,jdbcType=VARCHAR}
        <![CDATA[
        AND ( statistics_date <= #{queryMap.statisticsDate,jdbcType=VARCHAR})
        ]]>
        ORDER BY
        serial_number,
        create_time DESC
        ) t,
        ( SELECT @rank := 0, @number := 0, @serial_number := '' ) b
        ) t1
        WHERE
        <![CDATA[
        rank < 1
        ]]>
        )t5 on t4.serial_number=t5.serial_number
        WHERE t4.STATUS != - 1 AND t4.project_id = #{queryMap.projectId,jdbcType=VARCHAR}
        ) t2
        WHERE t2.`status` != - 1
        AND t2.project_id = #{queryMap.projectId,jdbcType=VARCHAR}
        <if test="queryMap.bagIdList != null and queryMap.bagIdList.size()>0">
            and (t2.asset_bag_id in
            <foreach close=")" collection="queryMap.bagIdList" item="listItem" open="(" separator=",">
                #{listItem}
            </foreach>
            <if test="queryMap.isNotPackage == 1">
                or t2.asset_bag_id is null
            </if>
            )
        </if>
        <if test="(queryMap.bagIdList == null or queryMap.bagIdList.size() == 0) and queryMap.isNotPackage == 1">
            and t2.asset_bag_id is null
        </if> ) b WHERE a.project_id = b.project_id AND a.serial_number = b.serial_number
        AND data_extract_time = #{queryMap.endDate,jdbcType=DATE}
        <if test="queryMap.isAssetStatus != 1">
            AND asset_status != '�ѽ���'
        </if> ) t3
        GROUP BY loanDim
        order by loanDim
    </select>

    <!-- ������ʲ��ȼ��ֲ� -->
    <select id="assetsRatingDistribute" resultType="java.util.Map">
        SELECT
        cast(score_range as SIGNED) AS loanDim,
        ROUND(SUM(remain_principal),2) AS PrincipalAmount,
        ROUND(IFNULL(SUM(remain_principal)/@sal*100,0),2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(IFNULL(COUNT(1)/@rn*100,0),2) AS loanNumRatio,
        ROUND(IFNULL(SUM(remain_principal)/COUNT(1),0),2) AS avgAmount
        FROM (
        SELECT
        b.score_range, a.remain_principal,
        @sal := @sal + IFNULL(remain_principal,0) AS remain_principals,
        @rn := @rn + 1 AS rn FROM (SELECT @sal := 0,@rn := 0) t0,
        t_assetaccountcheck a, (
        SELECT
        project_id, serial_number, score_range
        FROM
        (
        SELECT
        t4.project_id,
        t4.serial_number,
        t5.asset_bag_id,
        t4.`status`,
        IFNULL(t5.wind_control_status,'Without') as wind_control_status,
        IFNULL(t5.cheat_level,'-1') as cheat_level ,
        IFNULL(t5.score_range,'-1') as score_range,
        IFNULL(t5.score_level,'-1') as score_level
        from t_basic_asset t4
        left join (
        SELECT
        t1.project_id,
        t1.asset_bag_id,
        t1.serial_number,
        t1.`status`,
        t1.statistics_date,
        t1.wind_control_status,
        t1.wind_control_status_pool,
        t1.cheat_level,
        t1.score_range,
        t1.score_level
        FROM
        (
        SELECT
        t.*,
        IF
        ( @serial_number = t.serial_number, @rank := @rank + 1, @rank := 0 ) AS rank,
        @serial_number := t.serial_number,
        @number := @number + 1,
        @rank,
        @serial_number
        FROM
        (
        SELECT
        project_id,
        asset_bag_id,
        serial_number,
        `status`,
        statistics_date,
        wind_control_status,
        wind_control_status_pool,
        cheat_level,
        score_range,
        score_level
        FROM
        t_asset_wind_control_history
        WHERE
        project_id = #{queryMap.projectId,jdbcType=VARCHAR}
        <![CDATA[
        AND ( statistics_date <= #{queryMap.statisticsDate,jdbcType=VARCHAR})
        ]]>
        ORDER BY
        serial_number,
        create_time  DESC
        ) t,
        ( SELECT @rank := 0, @number := 0, @serial_number := '' ) b
        ) t1
        WHERE
        <![CDATA[
        rank < 1
        ]]>
        )t5 on t4.serial_number=t5.serial_number
        WHERE t4.STATUS != - 1 AND t4.project_id = #{queryMap.projectId,jdbcType=VARCHAR}
        ) t2
        WHERE t2.`status` != - 1
        AND t2.project_id = #{queryMap.projectId,jdbcType=VARCHAR}
        <if test="queryMap.bagIdList != null and queryMap.bagIdList.size()>0">
            and (t2.asset_bag_id in
            <foreach close=")" collection="queryMap.bagIdList" item="listItem" open="(" separator=",">
                #{listItem}
            </foreach>
            <if test="queryMap.isNotPackage == 1">
                or t2.asset_bag_id is null
            </if>
            )
        </if>
        <if test="(queryMap.bagIdList == null or queryMap.bagIdList.size() == 0) and queryMap.isNotPackage == 1">
            and asset_bag_id is null
        </if> ) b WHERE a.project_id = b.project_id AND a.serial_number = b.serial_number
        AND data_extract_time = #{queryMap.endDate,jdbcType=DATE}
        <if test="queryMap.isAssetStatus != 1">
            AND asset_status != '�ѽ���'
        </if> ) t3
        GROUP BY loanDim
        order by loanDim
    </select>

    <!-- �����ʡ�ݷֲ� -->
    <select id="provincesDistribute" resultType="java.util.Map">
        SELECT
        province AS loanDim,
        ROUND(SUM(remain_principal),2) AS PrincipalAmount,
        ROUND(IFNULL(SUM(remain_principal)/@sal*100,0),2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(IFNULL(COUNT(1)/@rn*100,0),2) AS loanNumRatio,
        ROUND(IFNULL(SUM(remain_principal)/COUNT(1),0),2) AS avgAmount
        FROM (
        SELECT
        a.remain_principal,
        substring(b.province, 1, 2) AS province,
        @sal := @sal + IFNULL(remain_principal,0) AS remain_principals,
        @rn := @rn + 1 AS rn FROM (SELECT @sal := 0,@rn := 0) t0,
        t_assetaccountcheck a, (
        SELECT
        project_id, serial_number, province
        FROM t_basic_asset
        WHERE STATUS != - 1
        AND project_id = #{queryMap.projectId,jdbcType=VARCHAR}
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
        </if> ) b WHERE a.project_id = b.project_id AND a.serial_number = b.serial_number
        AND data_extract_time = #{queryMap.endDate,jdbcType=DATE}
        <if test="queryMap.isAssetStatus != 1">
            AND asset_status != '�ѽ���'
        </if> ) t
        GROUP BY loanDim
        order by AmountRatio DESC,loanDim
    </select>

    <!-- ��Ѻ�ʷֲ� -->
    <select id="mortgageRatesDistribute" resultType="java.util.Map">
        SELECT
        tbl.remain_principal_region AS loanDim,
        ROUND(SUM(remain_principal),2) AS PrincipalAmount,
        ROUND(IFNULL(SUM(remain_principal)/@sal*100,0),2) AS AmountRatio,
        COUNT(1) AS loanNum,
        ROUND(IFNULL(COUNT(1)/@rn*100,0),2) AS loanNumRatio,
        ROUND(IFNULL(SUM(remain_principal)/COUNT(1),0),2) AS avgAmount
        FROM (
        SELECT
        remain_principal,
        CASE
        WHEN (rates &gt; 90.0) THEN '90%������������'
        WHEN (rates &gt; 80.0 AND rates &lt;= 90.0) THEN '80%��������-90%������'
        WHEN (rates &gt; 70.0 AND rates &lt;= 80.0) THEN '70%��������-80%������'
        WHEN (rates &gt; 60.0 AND rates &lt;= 70.0) THEN '60%��������-70%������'
        WHEN (rates &gt; 50.0 AND rates &lt;= 60.0) THEN '50%��������-60%������'
        WHEN (rates &gt; 40.0 AND rates &lt;= 50.0) THEN '40%��������-50%������'
        WHEN (rates &gt; 30.0 AND rates &lt;= 40.0) THEN '30%��������-40%������'
        WHEN (rates &gt; 20.0 AND rates &lt;= 30.0) THEN '20%��������-30%������'
        WHEN (rates &gt; 10.0 AND rates &lt;= 20.0) THEN '10%��������-20%������'
        WHEN (rates &gt; 0.0 AND rates &lt;= 10.0) THEN '10%����������'
        WHEN (rates = 0.0 ) THEN 'ȱʧ����Ѻ����Ϣ��'
        ELSE '��������'
        END AS remain_principal_region,
        CASE
        WHEN (rates &gt; 90.0) THEN 10
        WHEN (rates &gt; 80.0 AND rates &lt;= 90.0) THEN 9
        WHEN (rates &gt; 70.0 AND rates &lt;= 80.0) THEN 8
        WHEN (rates &gt; 60.0 AND rates &lt;= 70.0) THEN 7
        WHEN (rates &gt; 50.0 AND rates &lt;= 60.0) THEN 6
        WHEN (rates &gt; 40.0 AND rates &lt;= 50.0) THEN 5
        WHEN (rates &gt; 30.0 AND rates &lt;= 40.0) THEN 4
        WHEN (rates &gt; 20.0 AND rates &lt;= 30.0) THEN 3
        WHEN (rates &gt;= 10.0 AND rates &lt;= 20.0) THEN 2
        WHEN (rates &lt;= 10.0 ) THEN 1
        WHEN (rates = 0.0 ) THEN 0
        ELSE 100
        END AS orderSeq
        FROM (
        SELECT
        t1.serial_number,
        ROUND(IFNULL(t1.contract_amount/t2.pawn*100,0),2) AS rates,
        t1.remain_principal,
        @sal := @sal + IFNULL(t1.remain_principal,0) AS remain_principals,
        @rn := @rn + 1 AS rn
        FROM (SELECT @sal := 0,@rn := 0) a0,(
        SELECT
        a.serial_number, b.contract_amount, a.remain_principal
        FROM t_assetaccountcheck a, (
        SELECT
        project_id, serial_number, contract_amount
        FROM t_basic_asset
        WHERE STATUS != - 1
        AND project_id = #{queryMap.projectId,jdbcType=VARCHAR}
        AND guarantee_type = '��Ѻ����'
        AND asset_type = '������Ѻ����'
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
        </if> ) b WHERE a.project_id = b.project_id AND a.serial_number = b.serial_number
        AND data_extract_time = #{queryMap.endDate,jdbcType=DATE}
        <if test="queryMap.isAssetStatus != 1">
            AND asset_status != '�ѽ���'
        </if> ) t1
        LEFT JOIN (
        SELECT
        serial_number,
        IFNULL(SUM(pawn_value), 0) AS pawn
        FROM
        t_guarantycarinfo
        WHERE
        project_id = #{queryMap.projectId,jdbcType=VARCHAR} group by serial_number
        ) t2 ON t2.serial_number = t1.serial_number
        )t )tbl
        GROUP BY tbl.remain_principal_region,tbl.orderSeq
        order by tbl.orderSeq
    </select>

    <!-- ��ѯ������Ϣ -->
    <select id="carDistribute" resultType="java.util.Map">
        SELECT
        -- REPLACE(b.car_brand, 'N/A',' ȱʧ����Ѻ����Ϣ��' ) AS brand,
        -- REPLACE(b.car_type, 'N/A',' ȱʧ����Ѻ����Ϣ��' ) AS type
        b.car_brand AS brand,
        b.car_type AS type,
        ROUND(IFNULL(b.contract_amount/b.pawn*100,0),2) AS rates,
        ROUND(a.remain_principal,2) AS principal,
        ROUND(@sal := @sal + IFNULL(remain_principal,0),2) AS principals
        FROM (SELECT @sal := 0) a0, t_assetaccountcheck a, (
        SELECT
        t1.project_id, t1.serial_number, t1.contract_amount,
        IF(LENGTH(t2.car_brand)>0,t2.car_brand,'N/A') AS car_brand,
        IF(LENGTH(t2.car_type)>0,t2.car_type,'N/A') AS car_type,
        t2.pawn
        FROM (
        SELECT
        project_id, serial_number, contract_amount
        FROM t_basic_asset
        WHERE STATUS != - 1
        AND project_id = #{queryMap.projectId,jdbcType=VARCHAR}
        AND asset_type = '������Ѻ����'
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
        LEFT JOIN (
        SELECT
        serial_number,
        GROUP_CONCAT(DISTINCT car_type separator ' , ' ) AS car_type,
        GROUP_CONCAT(DISTINCT car_brand separator ' , ' ) AS car_brand,
        IFNULL(SUM(pawn_value), 0) AS pawn
        FROM
        t_guarantycarinfo
        WHERE project_id = #{queryMap.projectId,jdbcType=VARCHAR}
        group by serial_number ) t2
        ON t2.serial_number = t1.serial_number
        ) b WHERE a.project_id = b.project_id AND a.serial_number = b.serial_number
        AND data_extract_time = #{queryMap.endDate,jdbcType=DATE}
        <if test="queryMap.isAssetStatus != 1">
            AND asset_status != '�ѽ���'
        </if>
    </select>