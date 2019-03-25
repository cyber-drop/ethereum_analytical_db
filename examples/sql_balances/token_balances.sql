SELECT token, address, income - outcome AS balance FROM (
    SELECT token, from AS address, sum(value) AS outcome
    FROM eth_token_transaction
    GROUP BY token, from
)
ANY INNER JOIN (
    SELECT token, to AS address, sum(value) AS income
    FROM eth_token_transaction
    GROUP BY token, to
)
USING token, address
WHERE balance > 0.01
INTO OUTFILE 'token_balances.csv'
FORMAT CSVWithNames