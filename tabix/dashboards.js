var LS_SESSION_DATA = "ls.sessionData"
var DASHBOARD_SQL = `
SELECT 'Current block' as name, toFloat64(max(number)) AS current_state FROM ethereum_block
UNION ALL
SELECT 'Transactions' as name, toFloat64(count(*)) AS current_state FROM ethereum_internal_transaction
UNION ALL
SELECT 'Ethereum transferred, ETH' as name, toFloat64(sum(value)) AS current_state FROM ethereum_internal_transaction
UNION ALL
SELECT 'Fees paid, ETH' as name, toFloat64(sum(gasPrice * gasUsed)) AS current_state FROM ethereum_internal_transaction
UNION ALL
SELECT 'Total senders' as name, toFloat64(count(distinct(from))) AS current_state FROM ethereum_internal_transaction
UNION ALL
SELECT 'Total receivers' as name, toFloat64(count(distinct(to))) AS current_state FROM ethereum_internal_transaction
UNION ALL
SELECT 'Total miners and mining pools' as name, toFloat64(count(distinct(author))) AS current_state FROM ethereum_internal_transaction
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
        sessionData = sessionData.concat(DASHBOARDS)
        localStorage.setItem(LS_SESSION_DATA, JSON.stringify(sessionData))
        window.location.reload(false)
    }
}, 3000)