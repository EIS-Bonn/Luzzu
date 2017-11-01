getStatisticsForRequest(){
 curl --data "RequestID=$1" http://localhost:8080/Luzzu/getStatisticsForRequest
}

getStatus(){
 curl --data "RequestID=$1" http://localhost:8080/Luzzu/status
}

cancelRequest(){
 curl --data "RequestID=$1" http://localhost:8080/Luzzu/cancelRequest
}

alias pendingRequests="curl http://localhost:8080/Luzzu/getPendingRequests"
alias successfulRequests="curl http://localhost:8080/Luzzu/getSuccessfulRequests"
alias failedRequests="curl http://localhost:8080/Luzzu/getFailedRequests"
alias status=getStatus
alias statistics=getStatisticsForRequest
alias cancelRequest=cancelRequest
