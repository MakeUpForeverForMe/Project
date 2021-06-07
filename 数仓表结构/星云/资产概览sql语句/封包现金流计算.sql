<!-- 现金流分析计算(资产包) -->
    <select id="cashflowAnalyzeCalculate" resultMap="CashFlowResultMap">
       SELECT
        COALESCE (
            sum(t1.should_repay_principal + t1.should_repay_interest + t1.should_repay_cost ),
            0
        ) amount,
        COALESCE (
            sum(t1.should_repay_principal),
            0
        ) principal,
        COALESCE (
            sum(t1.should_repay_interest),
            0
        ) interest,
        COALESCE (
            sum(t1.should_repay_cost),
            0
        ) cost,
        COALESCE (
            sum(t1.begin_principal_balance),
            0
        ) begin_principal_balance,
        COALESCE (
            sum(t1.end_principal_balance),
            0
        ) end_principal_balance,
        t2.asset_bag_id asset_bag_id,
        t1.project_id project_id,
        t1.should_repay_date collection_date

        FROM
            t_repaymentplan t1
        JOIN (
            SELECT
                asset_bag_id,serial_number
            FROM
                t_basic_asset
            WHERE
                project_id = #{projectId,jdbcType=VARCHAR}
                AND
                asset_bag_id = #{assetBagId,jdbcType=VARCHAR}
        ) t2 ON t1.serial_number = t2.serial_number
        WHERE
            project_id = #{projectId,jdbcType=VARCHAR}
            AND (t1.repay_status != 1
            OR t1.repay_status IS NULL)
        GROUP BY
            should_repay_date
  </select>