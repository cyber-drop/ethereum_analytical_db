var LS_SESSION_DATA = "ls.sessionData"
var DASHBOARD_SQL = `
SELECT '1. Actual block' as state, toFloat64(max(number)) AS value FROM eth_block
UNION ALL
SELECT '2. Actual block for event extraction' as state, max(toFloat64(id)) AS value FROM eth_block_flag WHERE name = 'events_extracted'
UNION ALL
SELECT '3. Blocks without extracted events' as state, toFloat64(count(*)) AS value FROM eth_block WHERE id NOT IN(SELECT id FROM eth_block_flag WHERE name = 'events_extracted')
UNION ALL
SELECT '4. Actual block for traces extraction' as state, max(toFloat64(id)) AS value FROM eth_block_flag WHERE name = 'traces_extracted'
UNION ALL
SELECT '5. Blocks without extracted traces' as state, toFloat64(count(*)) AS value FROM eth_block WHERE id NOT IN(SELECT id FROM eth_block_flag WHERE name = 'traces_extracted')
UNION ALL
SELECT '6. Contracts with descriptions' as state, toFloat64(count(*)) AS value FROM eth_contract_description
UNION ALL
SELECT '7. Contracts with ABI' as state, toFloat64(count(*)) AS value FROM eth_contract_abi
UNION ALL
SELECT '8. Actual block for input parsing' as state, toFloat64(max(value)) as value FROM eth_contract_block
ORDER BY state
;;

SELECT name, CEILING(toFloat64(id) / 100000) * 100000 AS blockRange, count(*) / 100000 AS blocksCount
FROM eth_block_flag
WHERE name = 'traces_extracted'
GROUP BY name, blockRange
ORDER BY blockRange
DRAW_BAR
{
    'xAxis': "blockRange",
    'yAxis': "blocksCount"
}
;;

SELECT name, CEILING(toFloat64(id) / 100000) * 100000 AS blockRange, count(*) / 100000 AS blocksCount
FROM eth_block_flag
WHERE name = 'events_extracted'
GROUP BY name, blockRange
ORDER BY blockRange
DRAW_BAR
{
    'xAxis': "blockRange",
    'yAxis': "blocksCount"
}
;;
`
var DASHBOARDS = [{
    "name":"eth SQL",
    "sql":DASHBOARD_SQL,
    "buttonTitle": "Run all ⇧ + ⌘ + ⏎",
    "format":{},
    "results":[],
    "editor":null,
    "selectedResultTab":0
}]

console.log("Injecting pre-saved dashboards...")

setTimeout(function() {
    console.log("Loading pre-saved dashboards...")
    var sessionData = JSON.parse(localStorage.getItem(LS_SESSION_DATA) || "[]")
    var presented = sessionData.filter(x => x.name.indexOf("eth") != -1).length
    if (presented < DASHBOARDS.length) {
        console.log("Dashboards added")
        sessionData = DASHBOARDS.concat(sessionData)
        localStorage.setItem(LS_SESSION_DATA, JSON.stringify(sessionData))
        window.location.reload(false)
    }
}, 3000)