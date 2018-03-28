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
wfConn <- mongo(collection="workflows",db="test", verbose = T)
logConn <- mongo(collection="exportlog",db="test", verbose = T)

# 2. Set a seed point for the record retrieval
# 1 min before the process
queryTs <- strftime(as.POSIXlt(Sys.time()-60, "UTC", "%Y-%m-%dT%H:%M:%S"), "%Y-%m-%dT%H:%M:%S%z")

# Check latest record
log.tmp <- logConn$find(sort = '{"_id":-1}',limit = 1)
if(length(log.tmp) != 0){
  if(log.tmp$dt == format(Sys.time(),"%Y%m%d") ){
    log.tmp$log <- (log.tmp$log + 1)
  } else {
    log.tmp$log <- 1
    log.tmp$dt <- format(Sys.time(),"%Y%m%d")
  }
} else {
  log.tmp <- data.table(log=1,dt=format(Sys.time(),"%Y%m%d"))
}

smb.log <- data.table( dt = format(Sys.time(),"%Y%m%d"), log = log.tmp$log, time = Sys.time())

# 3. Loop through the operations on wf1, wf2, wf3
lapply(c("wf"),function(name){
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
      
      # Below process for the additional requirement to re-arrange the column based on intial input
      dt.data$wfSalesOrderNote <- ""
      # Re-arrange the order of the fields base on original xls requirement
      # Any new fields added in the future will need to be added below
      dt.data <- setcolorder(dt.data, c("wfProcess",
                                         "wfProcessName",
                                         "wfForm",
                                         "wfFormId",
                                         "wfFormSplit",
                                         "wfOptMachineId",
                                         "wfSpecCap",
                                         "wfSpecDF",
                                         "wfSpecLC",
                                         "wfSpecZESR",
                                         "wfAgeVoltSet",
                                         "wfPriorWfFormId",
                                         "wfNakedProductSpec",
                                         "wfOrderId",
                                         "wfOrderSeries",
                                         "wfOrderBatchId",
                                         "wfOrderRMId",
                                         "wfOrderSpec",
                                         "wfOrderDim",
                                         "wfOrderBatchQty",
                                         "wfOrderTotalQty",
                                         "wfSalesOrderQty",
                                         "wfClientId",
                                         "wfOrderFormNote",
                                         "wfOrderNote",
                                         "wfOrderBOMNote",
                                         "wfSalesOrderNote",
                                         "wfOrderDate",
                                         "wfOrderStartDate",
                                         "wfOrderEstFinishDate",
                                         "wfOrderDeliveryDate",
                                         "wfOrderTK",
                                         "wfRMFoilPosName",
                                         "wfRMFoilPosLName",
                                         "wfRMFoilPosCapFrom",
                                         "wfRMFoilPosCapTo",
                                         "wfRMFoilPosWidth",
                                         "wfRMFoilPosLength",
                                         "wfRMFoilNegName",
                                         "wfRMFoilNegLName",
                                         "wfRMFoilNegCapFrom",
                                         "wfRMFoilNegCapTo",
                                         "wfRMFoilNegWidth",
                                         "wfRMFoilNegLength",
                                         "wfRMFoilNegQty",
                                         "wfRMPaperName",
                                         "wfRMPaperQty",
                                         "wfRMPinPosName",
                                         "wfRMPinNegName",
                                         "wfRMPinPosQty",
                                         "wfRMPinNegQty",
                                         "wfRMGlueName",
                                         "wfRMSolName",
                                         "wfRMSolQty",
                                         "wfRMShellName",
                                         "wfRMShellQty",
                                         "wfRMPlasticName",
                                         "wfRMPlasticQty",
                                         "wfRMCoverName",
                                         "wfRMCoverQty",
                                         "wfRMUpBeltName",
                                         "wfRMDownBeltName",
                                         "wfRMBaseName",
                                         "wfRMCircleName",
                                         "wfRMFoilPosSerial",
                                         "wfRMFoilNegSerial",
                                         "wfRMFoilPosLSerial",
                                         "wfRMFoilNegLSerial",
                                         "wfRMPaperSerial",
                                         "wfRMGlueSerial",
                                         "wfRMSolSerial",
                                         "wfRMPinPosSerial",
                                         "wfRMPinNegSerial",
                                         "wfRMPlasticSerial",
                                         "wfRMShellSerial",
                                         "wfRMCoverSerial",
                                         "wfSalesOrderId",
                                         "wfRMFoilPosQty",
                                         "wfRMCoverCheck",
                                         "wfRMWindingTime",
                                         "wfRMWindingDeg",
                                         "wfDryWindingDeg",
                                         "wfWetEmptyAir",
                                         "wfWetAir",
                                         "wfWashWindingDeg",
                                         "wfWashDryWindingDeg",
                                         "wfWashDryTime",
                                         "wfQCCheck",
                                         "wfRandomCheckInfo",
                                         "wfSpecNote",
                                         "wfAgeDetailAG1",
                                         "wfAgeDetailAG2",
                                         "wfAgeDetailAG3",
                                         "wfAgeDetailAG4",
                                         "wfAgeDetailAG5",
                                         "wfAgeDetailAG6",
                                         "wfAgeDetailLCT",
                                         "wfAgeDetailLC",
                                         "wfAgeDetailCAP",
                                         "wfAgeDetailDF",
                                         "wfAgeDetailStaffConfirm",
                                         "wfOptInputDate",
                                         "wfOptInputEndDate",
                                         "wfOptWashMachine",
                                         "wfOptStartTime",
                                         "wfOptFinishTime",
                                         "wfOptBadQtyItem",
                                         "wfOptBadQty",
                                         "wfOptGoodQty",
                                         "wfBadItem1",
                                         "wfBadQty1",
                                         "wfBadItem2",
                                         "wfBadQty2",
                                         "wfBadItem3",
                                         "wfBadQty3",
                                         "wfBadItem4",
                                         "wfBadQty4",
                                         "wfBadItem5",
                                         "wfBadQty5",
                                         "wfBadItem6",
                                         "wfBadQty6",
                                         "wfBadItemTotal",
                                         "wfAgeDegSet",
                                         "wfAgeDegAct",
                                         "wfAgeVoltAct",
                                         "wfAgeCurrentSet",
                                         "wfAgeCurrentAct",
                                         "wfAgeTimeSet",
                                         "wfAgeTimeAct",
                                         "wfAgeNote",
                                         "wfAutoAgeVoltAct1",
                                         "wfAutoAgeVoltAct2",
                                         "wfAutoAgeVoltAct3",
                                         "wfAutoAgeVoltAct4",
                                         "wfStaffOptId",
                                         "wfStaffOptName",
                                         "wfStaffOptShift",
                                         "wfStaffLeadName",
                                         "wfStaffLeadId",
                                         "wfStaffTechId",
                                         "wfStaffTechName",
                                         "wfStaffXrayId",
                                         "wfStaffXrayName",
                                         "wfStaffQCId",
                                         "wfStaffQCName",
                                         "wfQCPass",
                                         "wfQCSignOff",
                                         "wfQCInputNote",
                                         "wfOrderSupNote",
                                         "wfNakedProductSerial",
                                         "wfRMUpBeltSerial",
                                         "wfRMDownBeltSerial",
                                         "wfRMBaseSerial",
                                         "wfRMCricleSerial",
                                         "wfRMPrintName",
                                         "wfRMPrintSerial",
                                         "wfFinalCheckInfo",
                                         "wfElecPass",
                                         "wfLookPass",
                                         "wfOptStartQty",
                                         "wfBadTotal",
                                         "wfGoodTotal",
                                         "wfFormStatus",
                                         "wfProcessStatus",
                                         "created",
                                         "appUpload",
                                         "wfFormExcept",
                                         "wfReadOnly",
                                         "wfProcessNew",
                                         "wfLastCompletedWf",
                                         "wfErrorMsg",
                                         "updated",
                                         "exported",
                                         "__v",
                                         "wfOptGoodQty2"))

      # Assign back to the object in the background
      assign(funcConcat(name,"Data"), dt.data, envir = .GlobalEnv)
      
      write.xlsx2(x = get(funcConcat(name,"Data")),file = funcConcat("/home/adminSA01/vtERP/",log.tmp$dt,"_",formatC(log.tmp$log, width = 3,flag = "0"),"_vtApp.xlsx"), row.names = F)
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
      
      logConn$insert(smb.log)
      
    } else {
      # Error handling
      funcPrint("This process ",name," has error and please email Admin!")
      
    }
  }
})

