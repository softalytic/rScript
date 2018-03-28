setwd("~/rawData/")
source("~/rScript/coreFunc.r")
pgName <- "vtStaff"
funcArgs() # Load any args
library(xlsx)

if(!exists("dtFolder")) dtFolder = T
if(!exists("dtUpdate")) dtUpdate = T

# Below setting for chinese character handling
Sys.setenv(NLS_LANG="TRADITIONAL CHINESE_TAIWAN.AL32UTF8")
Sys.setenv(TZ="Asia/Taipei")

funcMvFiles(fromPath = "~/vtERP/",fileNames = funcConcat("*",pgName,"*"),filesPath = "staffData/")

checkERPFiles <- system("ls ~/rawData/staffData | grep vtStaff", intern = T)

if (length(checkERPFiles) > 0) {
  smb.log <- data.table( dt = Sys.time(), log = "Staff Data found", data = checkERPFiles)
  conn <- mongo(collection="test",db="smbLog", verbose = T)
  conn$insert(smb.log)
  
  # Main operation here
  try(
    lapply(
      funcFileList(dtFolder = dtFolder,pgName = "staffData", pattern = funcConcat("*",pgName,".xls*")),function(f){
        funcDtTimer(T)
        funcPrint("Uploading file: ",f)
        
        # Process the data
        # dt <- fread(f,encoding="UTF-8")
        # Read input from excel
        # Assume there is only 1 worksheet
        # dt <- data.table(read.xlsx2(f,sheetIndex = 1))
        # Backup code for testing
        staff <<- data.table(read.xlsx2(f,sheetName = "Staff",startRow = 2))
        print(names(staff))
        staff <<- subset(staff,select = c(1:14))
        names(staff) <- c("wfProcessName",
                          "wfRow",
                          "wfStaffOptName",
                          "wfStaffOptId",
                          "wfStaffOptShift",
                          "wfStaffRole",
                          "wfStaffTechName",
                          "wfStaffTechId",
                          "wfStaffLeadName",
                          "wfStaffLeadId",
                          "wfStaffXrayName",
                          "wfStaffXrayId",
                          "wfStaffQCName",
                          "wfStaffQCId")
        staff <- staff[wfStaffOptId != ""]
        funcPrint("staff table has been finished")
        
        machine <<- data.table(read.xlsx2(f,sheetName = "Machine",startRow = 2))
        print(names(machine))
        names(machine) <- c("wfOptMachineId",
                            "wfStaffTechName",
                            "wfStaffTechId",
                            "wfStaffOptShift")
        machine <- machine[wfOptMachineId != ""]
        
        funcPrint("loading the data to db")
        
        staff <- jsonlite::toJSON(staff)
        machine <- jsonlite::toJSON(machine)
        dttm <- strftime(as.POSIXlt(Sys.time(), "UTC", "%Y-%m-%dT%H:%M:%S"), "%Y-%m-%dT%H:%M:%S%z")
        
        dt <- data.table(dttm, staff, machine)
        
        print(head(dt))
        # Setup the connection and load data into the db
        conn <- mongo(collection="staffdatas",db="test", verbose = T)
        conn$insert(dt)
        rm(conn)
        
        gc()
        
        # Move the file to back up
        funcMvFiles(fromPath = f, fileNames = "", filesPath = "staffData_backup/")
        funcDtTimer(F)
      }))
} else {
  funcPrint("No such files")
  smb.log <- data.table( dt = Sys.time(), log = "No Staff data for input")
  conn <- mongo(collection="test",db="smbLog", verbose = T)
  conn$insert(smb.log)
  
}
