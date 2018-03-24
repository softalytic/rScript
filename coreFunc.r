library(data.table)
library(urltools)
library(stringr)
library(bit64)
library(jsonlite)
library(dplyr)
library(ggplot2)
#library(googlesheets)
library(lubridate)
library(scales)
library(zoo)
library(pryr)
library(sas7bdat)
# library(quantmod)
# library(Quandl)
# library(TTR)
#library(FinancialInstrument)
# library(blotter)
# library(quantstrat)
library(readxl)
# library(neuralnet)
library(randomForest)
library(rpart)
library(boot)
library(caret)
library(nnet)
library(mongolite)
library(xlsx)

funcPLog <- function(base, inc, x, msg){
  # Assume dtStatus is available from global
  if (!exists("dtStatus")){
    dtStatus <<- NULL
  }
  
  dtStatus <<- inc + x
  print(paste("Progress at", round(dtStatus/base*100),"%", sep = " "))
  print(paste(">>", msg, sep = " "))
  
}

funcPrimeNum<- function(n){
  start <- Sys.time()
  n <- as.integer(n)
  if(n > 1e6) stop ("n too large")
  primes <- rep(TRUE, n)
  primes[1] <- FALSE
  last <- 2L #optimized for calculation as int rather than num
  for(i in last:floor(sqrt(n))){ #If the prime * prime greater the size of n, the loop can stop
    primes[seq.int(2L*last, n, last)] <- FALSE #Eliminate all the numbers divided by the prime in the array size of n
    last <- last + min(which(primes[(last+1):n])) #The next undivided number remains in the array will be a prime and use it for next loop operations
  }
  print(format(Sys.time()-start, format="%s"))
  which(primes)
}

funcDtTimer <- function(x){
  if(x){
    dtStartTime <<- Sys.time()
  }else{
    print(paste("This process has taken",as.character.Date((Sys.time()-dtStartTime)), sep = " "))
    # Self Cleanup
    rm("dtStartTime", envir = .GlobalEnv)
  }
}

funcDtSave <- function(saveGit = T, saveRData = T, pattern = "DT..*$", fileName = pgName){
  # This function requires pgName from the global environment
  if(saveGit){
    print("Updating Git")
    system(paste("cd ~/rScript && git add ",pgName,".r && git commit -m update && git push", sep = ""))
    system("cd ~/rScript && git add coreFunc.r && git commit -m update && git push")
  }
  if(saveRData){
    print("Saving the RData")
    dtTmpObjects <- ls(envir = .GlobalEnv)[!(ls(envir = .GlobalEnv) %in% ls(envir = .GlobalEnv, pattern = funcConcat(pattern,"|pgName")))]
    print(paste("Removing tmp objects:", dtTmpObjects, sep = " "))
    rm(list = dtTmpObjects, envir = .GlobalEnv)
    save.image(paste("~/rData/",fileName,".RData", sep = ""))
  }
}

funcRGitUpdate <- function() {
  system("cd ~/rScript/ && git pull")
  system("cd ~/rScript && git add coreFunc.r && git commit -m update && git push") 
}

funcGitPull <- function(path = "~/rScript"){
  system(paste("cd ",path," && git pull", sep = ""))
}

funcGitPush <- function(file = "coreFunc.r", path = "~/rScript",force = F){
  if(force){
    system(paste("cd ",path," && git push"))
  } else {
    system(paste("cd ",path," && git add ", file, " && git commit -m update && git push", sep = ""))
  }
}

funcDiskSize <- function(path = "~/"){
  system(paste("du -sh ", path, sep = ""))
}

funcAddDate <- function(DT, field = "main.dt", dateFormat = "%Y%m%d"){
  # Add R date, Year, Mth, Weeks into the data.table
  DT.tmp <- DT
  DT.tmp$main.date <- as.Date.factor(getElement(DT.tmp,field), format = dateFormat)
  DT.tmp$main.dateYr <- strftime(DT.tmp$main.date, format = "%Y")
  DT.tmp$main.dateQtr <- funcAddDateQtr(DT.tmp$main.date)
  DT.tmp$main.dateMth <- strftime(DT.tmp$main.date, format = "%m")
  DT.tmp$main.dateWk <- strftime(DT.tmp$main.date, format = "%W")
  DT.tmp$main.dateDay <- strftime(DT.tmp$main.date, format = "%d")
  DT.tmp$main.dateWd <- as.integer(factor(weekdays(DT.tmp$main.date,T), levels = c("Mon", "Tue", "Wed","Thu", "Fri", "Sat", "Sun"),ordered = TRUE))
  return(DT.tmp)
}

funcConcat <- function(...){
  paste(..., sep = "")
}

funcMvFiles <- function( fromPath = "~/rawData/", fileNames, toPath = "~/rawData/", filesPath){
  if(! dir.exists(funcConcat(toPath,filesPath))){
    dir.create(funcConcat(toPath,filesPath))
  }
  system(paste("mv ", fromPath, fileNames, " ", toPath, filesPath, sep = ""))
  print(funcConcat(fromPath, fileNames," has been moved to ", toPath, filesPath))
}

funcDayRangeGen <- function(start, end, dateFormat = "%Y%m%d"){
  sDate <- as.Date(start)
  eDate <- as.Date(end)
  dRange <- format(seq(as.Date(sDate),as.Date(eDate), by = "days"), dateFormat)
  return(dRange)
}

funcDateList <- function(frequency, origin = Sys.Date()){
  # This function only to be used for weekly or monthly date generator
  origin <- as.Date(origin)
  dRange <- switch(frequency,
                   "week" = funcDayRangeGen(as.Date(floor_date(origin-weeks(1),"week")),as.Date(ceiling_date(origin - weeks(1),"week")-days(1))),
                   "month" = funcDayRangeGen(as.Date(floor_date(origin-months(1),"month")),as.Date(ceiling_date(origin - months(1),"month")-days(1)))
  )
  return(dRange)
}

funcPercent <- function(x, digits = 2){
  return(funcConcat(round(x * 100, digits),"%"))
}

funcDateFqName <- function(frequency, offset = 0, origin = Sys.Date()){
  # This function only to be used for weekly or monthly date generator
  origin <- as.Date(origin)
  dRange <- switch(frequency,
                   "week" = format(as.Date(floor_date(origin-weeks(offset),"week")),"%Y%mW%U"),
                   "month" = format(as.Date(floor_date(origin-months(offset),"month")),"%Y%m")
  )
  return(dRange)
}

funcFileChecking <- function(files, fileList){
  if(length(files[files %in% fileList]) != length(fileList)){
    print("Below files are missing")
    print(fileList[! fileList %in% files])
    stop("files does not match")
  }
}

funcRM <- function(pattern){
  rm(list = ls(pattern = pattern, envir = .GlobalEnv), envir = .GlobalEnv)
}

funcGsUpload <- function(pattern, path = "~/rOutput/", gsFolder = "", delete = F){
  print("Uploading to gDrive")
  lapply(list.files(path = path,pattern = pattern), function(f){
    system(funcConcat("~/gdrive upload",if(gsFolder != ""){funcConcat(" --parent ",gsFolder," ")}else{" "},funcConcat(path,f)))
  })
  if(delete){funcRmFiles(pattern = pattern, path = path)}
}

funcPrint <- function(...){
  print(funcConcat(...))
}

funcAddDateQtr <- function(x){
  return(ceiling(as.integer(format(x,"%m"))/3))
}

funcMemFree <- function(){
  system("grep MemFree /proc/meminfo")
}

funcCheckDup <- function(DT, field){
  x <- DT[,.N,by = eval(parse(text = field))][N > 1]
  return(DT[eval(parse(text = field)) %in% x$parse])
}

funcPerValue <- function(x){
  as.numeric(substr(x,1,nchar(x)-1))
}

funcls <- function(dir = "", grep = ""){
  # funcPrint("ls", if(dir != "") {dir}, if (grep!="") {funcConcat(" | grep ", grep)})
  system(funcConcat("ls", if(dir != "") {funcConcat(" ",dir)}, if (grep!="") {funcConcat(" | grep ", grep)}))
}

funcUnzip <- function(file, dir = getwd()) {
  system(funcConcat("unzip ", dir, "/", file))
}

funcRmFiles <- function(pattern, path = getwd()){
  # Remove files based on either patten or exact name
  system(funcConcat("rm -rf ", path, pattern))
}

funcDt <- function(DT,filter = "",operator,byTerm){
  filter <- funcDtTerms(filter,"filter")
  operator <- funcDtTerms(operator,"operator")
  byTerm <- funcDtTerms(byTerm,"byTerm")
  # orderBy <- funcDtTerms(byTerm,"orderBy")
  if(nchar(filter)>0){
    return(DT[eval(parse(text = filter)), eval(parse(text = operator)), by = eval(parse(text = byTerm))])
  } else {
    return(DT[, eval(parse(text = operator)), by = eval(parse(text = byTerm))])
  }
}

funcIfNaN <- function(value){
  return(ifelse(is.nan(value)|is.na(value),0,value))
}

funcOracleSql <- function(sql){
  #require(ROracle)
  #require(ora)
  #Specific system params for Oracle DB
  Sys.setenv(NLS_LANG="TRADITIONAL CHINESE_TAIWAN.AL32UTF8")
  Sys.setenv(TZ="Asia/Taipei")
  
  drv <- dbDriver("Oracle")
  host <- ""
  port <- 1521
  svc <- ""
  connect.string <- paste(
    "(DESCRIPTION=",
    "(ADDRESS=(PROTOCOL=tcp)(HOST=", host, ")(PORT=", port, "))",
    "(CONNECT_DATA=(SERVICE_NAME=", svc, ")))", sep = "")
  try(ora::sql(sql, tolower=TRUE, dots=TRUE, encoding="UTF8", useBytes=TRUE, stringsAsFactors=FALSE, warn=-1, debug=FALSE,username = "", password = "",dbname = connect.string))
}

funcArgs <- function(){
  x <- (commandArgs(TRUE))
  ## args is now a list of character vectors
  ## First check to see if arguments are passed.
  ## Then cycle through each element of the list and evaluate the expressions.
  if(length(x)==0){
    print("No arguments supplied.")
    ##supply default values
  }else{
    for(i in 1:length(x)){
      eval(parse(text = x[[i]]),envir = .GlobalEnv)
    }
  }
}

funcGit <- function(){
  funcGitPull()
  funcGitPush(".")
}

funcFileList <- function(dtFolder = F, pgName = "", pattern = funcConcat("*",pgName,".tsv"), path = funcConcat(getwd(),"/",pgName)){
  # handy function for getting the file list from getwd() or sub folder within getwd()
  if(dtFolder){
    # Get all the files under the wd
    funcPrint("Looking for folder under: ",getwd(),"/",pgName)
    fileList <- funcConcat(getwd(),"/",pgName,"/",list.files(path = path,pattern = pattern))
  } else {
    funcPrint("Looking for folder under: ",getwd())
    fileList <- funcConcat(getwd(),"/",list.files(path = getwd(),pattern = pattern))
    # Move the files to dedicated folder
    dir.create(path = funcConcat(getwd(),"/",pgName),showWarnings = F)
  }
  print(fileList)
  return(fileList)
}

funcDtReplaceNA = function(dt) {
  na.replace = function(v,value=0) { v[is.na(v)] = value; v }
  for (i in names(dt))
    eval(parse(text=paste("dt[,",i,":=na.replace(",i,")]")))
}

funcOracleSqlWriteTable <- function(tableName,dt){
  #require(ROracle)
  #require(ora)
  #Specific system params for Oracle DB
  funcDtTimer(T)
  Sys.setenv(NLS_LANG="TRADITIONAL CHINESE_TAIWAN.AL32UTF8")
  Sys.setenv(TZ="Asia/Taipei")
  
  drv <- dbDriver("Oracle")
  host <- ""
  port <- 1521
  svc <- ""
  connect.string <- paste(
    "(DESCRIPTION=",
    "(ADDRESS=(PROTOCOL=tcp)(HOST=", host, ")(PORT=", port, "))",
    "(CONNECT_DATA=(SERVICE_NAME=", svc, ")))", sep = "")
  con <- ROracle::dbConnect(drv = drv,username = "",password = "",dbname = connect.string);
  try(
    ROracle::dbWriteTable(
      conn = con,
      name = tableName,
      value = dt,
      row.names = F,
      append = T
    )
  )
  ROracle::dbDisconnect(con)
  funcDtTimer(F)
}

funcGApull <- function(table ,offset = 1,date = Sys.Date(),start = NULL,dim,metrics,max=1e+7){
  funcDtTimer(T)
  tryCatch(
    if (!exists("oauth_token")) { load("~/rawData/ga_oauth_token") }
    ,error = function(e) {funcPrint("cant find the oauth_token file, make sure it is saved in wd")}
  )

  funcPrint("Processing ga:table",table)
  
  if(!grepl("ga:date",dim)){ dim <- funcConcat("ga:date,",dim) }
  tmp <- try(data.table(GetReportData(
    QueryBuilder(Init(start.date = strftime(if(is.null(start)){date(date)-offset}else{start},"%Y-%m-%d")
                      ,end.date = strftime(date(date), "%Y-%m-%d")
                      ,dimensions = dim
                      ,metrics = metrics
                      ,max.results = max
                      ,sort = "-ga:date"
                      ,table.id = table))
    ,oauth_token,split_daywise = T,delay = 3)))
  funcDtTimer(F)
  head(tmp)
  return(tmp)
}

funcDtGsub <- function(obj,target,replace) {
  eval(parse(text=paste(obj, "<<- gsub(target,replace,",obj,") ", sep=""))) 
}

funcGsDownload <- function(path, parent){
  cmd <- funcConcat("cd ",path," && ~/gdrive download query -f \" \'",parent, "\' in parents\"")
  system(command = cmd)
}
