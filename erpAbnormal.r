setwd("~/rawData/")
source("~/rScript/coreFunc.r")
pgName <- "ERPAbnormal"
funcArgs() # Load any args
library(xlsx)

if(!exists("dtFolder")) dtFolder = T
if(!exists("dtUpdate")) dtUpdate = T

# Below setting for chinese character handling
Sys.setenv(NLS_LANG="TRADITIONAL CHINESE_TAIWAN.AL32UTF8")
Sys.setenv(TZ="Asia/Taipei")

funcMvFiles(fromPath = "~/vtERP/",fileNames = funcConcat("*",pgName,".xls"),filesPath = "ERP_Data/")

checkERPFiles <- system("ls ~/rawData/ERP_Data/ | grep ERPYC", intern = T)

if (length(checkERPFiles) > 0) {
  smb.log <- data.table( dt = Sys.time(), log = "ERP Abnormal Data found", data = checkERPFiles)
  conn <- mongo(collection="test",db="smbLog", verbose = T)
  conn$insert(smb.log)
  
  # Main operation here
  try(
    lapply(
      funcFileList(dtFolder = dtFolder,pgName = "ERP_Data", pattern = funcConcat("*",pgName,".xls")),function(f){
        funcDtTimer(T)
        funcPrint("Uploading file: ",f)
        
        # Process the data
        # dt <- fread(f,encoding="UTF-8")
        # Read input from excel
        # Assume there is only 1 worksheet
        dt <- data.table(read.xlsx2(f,sheetIndex = 1))
        # Backup code for testing
        # dt <- data.table(read.xlsx2("/home/appSA01/rawData/ERP_Data/20171128_ERPAbnormal.xls",sheetIndex = 1,startRow = 4))
        
        funcPrint("loading the data to db")
        
        # Dump the name of the dt
        names(dt)
        
        # Hard coded to select the variables 
        # dt <- subset(dt, select = c(1,2,3,4,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,23,24,25,26,28,29,30,31,32,35,39,43,47,51,53,55,57,59,61,63,66,67,68,69,70,71,72,73,74,75,76,77,78,79))
        
        # Hard coded to rename the dt
        names(dt) <- c("wfRow",
                       "wfFormId",
                       "wfFormClass",
                       "wfCurrentProcessName",
                       "wfOrderFormNote",
                       "wfFormERPName",
                       "wfOrderRMId",
                       "wfOrderSpec",
                       "wfOrderDim",
                       "wfOrderBatchQty",
                       "wfNewAddDate")
        print(head(dt))
        dt[,wfFormId:=toupper(wfFormId)]
        dt$created <- strftime(as.POSIXlt(Sys.time(), "UTC", "%Y-%m-%dT%H:%M:%S"), "%Y-%m-%dT%H:%M:%S%z")
        dt[wfFormERPName == "裸品", wfForm := 1][wfFormERPName == "贴片铝电解电容", wfForm := 2][! wfForm %in% c(1,2), wfForm := 3]
        
        print(head(dt))
        # Setup the connection and load data into the db
        conn <- mongo(collection="erpexcdatas",db="test", verbose = T)
        conn$insert(dt[wfFormId != ""])
        
        # House keeping tasks
        rm(conn)
        gc()
        
        # Move the file to back up
        funcMvFiles(fromPath = f, fileNames = "", filesPath = "ERPExcData_backup/")
        funcDtTimer(F)
      }))
} else {
  funcPrint("No such files")
  smb.log <- data.table( dt = Sys.time(), log = "No ERP Abnormal data for input")
  conn <- mongo(collection="test",db="smbLog", verbose = T)
  conn$insert(smb.log)
  
}
