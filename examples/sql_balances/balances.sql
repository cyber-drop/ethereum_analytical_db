SELECT address, balance_without_income + income AS balance
FROM (
    SELECT address, balance_without_income_and_reward + reward AS balance_without_income
    FROM (
        SELECT address, fee_reward - fee - outcome AS balance_without_income_and_reward
        FROM (
            SELECT address, fee_reward
            FROM (
                SELECT distinct(address)
                FROM (
                    SELECT to AS address FROM eth_internal_transaction
                    UNION ALL
                    SELECT from AS address FROM eth_internal_transaction
                    UNION ALL
                    SELECT author AS address FROM eth_internal_transaction
                )
            )
            ANY LEFT JOIN (
                SELECT address, sum(fee) AS fee_reward
                FROM (
                    SELECT blockNumber, sum(gasPrice * gasUsed) AS fee
                    FROM eth_internal_transaction
                    WHERE id LIKE '%.0'
                    GROUP BY blockNumber
                )
                ANY INNER JOIN (
                    SELECT author AS address, blockNumber
                    FROM eth_internal_transaction
                    WHERE type = 'reward'
                    AND rewardType = 'block'
                )
                USING blockNumber
                GROUP BY address
            )
            USING address
        )
        ANY LEFT JOIN (
            SELECT from AS address, sum(gasPrice * gasUsed) AS fee, sum(value) AS outcome
            FROM eth_internal_transaction
            GROUP BY from
        )
        USING address
    )
    ANY LEFT JOIN (
        SELECT address, reward
        FROM (
            SELECT author AS address, sum(value) AS reward
            FROM eth_internal_transaction
            WHERE type = 'reward'
            GROUP BY author
        )
    )
    USING address
)
ANY LEFT JOIN (
    SELECT to AS address, sum(value) AS income
    FROM eth_internal_transaction
    WHERE value > 0 AND type != 'reward'
    GROUP BY to
)
USING address
INTO OUTFILE 'balances.csv'
FORMAT CSVWithNames