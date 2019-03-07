var LS_SESSION_DATA = "ls.sessionData"
var DASHBOARDS = [{
    "name":"eth SQL",
    "sql":"Dashboard SQL for monitoring",
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