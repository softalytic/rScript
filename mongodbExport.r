setwd("~/rawData/")
source("~/rScript/coreFunc.r")
pgName <- "vtAppServer"
funcArgs() # Load any args

if(!exists("dtFolder")) dtFolder = T
if(!exists("dtUpdate")) dtUpdate = T

# Below setting for chinese character handling
Sys.setenv(NLS_LANG="TRADITIONAL CHINESE_TAIWAN.AL32UTF8")
Sys.setenv(TZ="Asia/Taipei")

# Custom function for mongoDB query
mongoQuery <- function(conn,objName,queryTs){
  assign(funcConcat(objName,"Data") , data.table(conn$find(
    funcConcat('{"$or":[{"exported":null},{"exported":0},{"exported":false}],"updated" : { "$lte" : {"$date" : "',queryTs,'"}}}')))
    ,envir = .GlobalEnv)
  assign(funcConcat(objName,"DataId") , data.table(conn$find(
    funcConcat('{"$or":[{"exported":null},{"exported":0},{"exported":false}],"updated" : { "$lte" : {"$date" : "',queryTs,'"}}}'),
    fields = "{\"_id\":1}"))
    ,envir = .GlobalEnv)
}

# 1. Setup the MongoDB connections
erp <- mongo(collection="erp",db="test", verbose = T)
wf1Conn <- mongo(collection="workflow1",db="test", verbose = T)
wf2Conn <- mongo(collection="workflow2",db="test", verbose = T)
wf3Conn <- mongo(collection="workflow3",db="test", verbose = T)

# 2. Set a seed point for the record retrieval
# 1 min before the process
queryTs <- strftime(as.POSIXlt(Sys.time()-60, "UTC", "%Y-%m-%dT%H:%M:%S"), "%Y-%m-%dT%H:%M:%S%z")

# 3. Loop through the operations on wf1, wf2, wf3
lapply(c("wf1","wf2","wf3"),function(name){
  eval(parse(text = funcConcat("mongoQuery(",name,"Conn,\"",name,"\",queryTs)")))
  # Assume data.table has been converted when query from mongodb
  dt.data <- get(funcConcat(name,"Data"))
  dt.data.id <- get(funcConcat(name,"DataId"))
  
  # First check if there is any record
  if(dt.data[,.N] == 0 && dt.data.id[,.N] == 0){
    funcPrint("There is no record for ",name," and skipping the export!!")
    
  } else {
    # Then check if the record length are match
    if(dt.data[,.N] == dt.data.id[,.N]){
      funcPrint("This process ",name," is valided!")
      # Export the files
      # funcDtOutputFiles(pattern = funcConcat(name,"Data$"), oPath = "~/vtERP/",gsUpload = F)
      write.xlsx2(x = get(funcConcat(name,"Data")),file = funcConcat("/home/appSA01/vtERP/",Sys.time(),"_",name,".xlsx"), row.names = F)
      # Should load the exported id into the mongoDB for record keep
      
      # For each unique data id, mark exported
      # Setup the export Timestamp
      exportTs <- strftime(as.POSIXlt(Sys.time(), "UTC", "%Y-%m-%dT%H:%M:%S"), "%Y-%m-%dT%H:%M:%S%z")
      # Setup the dynamic connection
      con <- get(funcConcat(name,"Conn"))
      # loop through each record and mark exported
      lapply(dt.data.id$`_id`, function(id){
        funcPrint("Processing ",id)
        con$update(funcConcat('{"_id" : {"$oid":"',id,'"}}'), 
                   funcConcat('{"$set":{"exported":true, "exportTS":{"$date":"',exportTs,'"}}}'))
      })
      
    } else {
      # Error handling
      funcPrint("This process ",name," has error and please email Admin!")
      
    }
  }
})

