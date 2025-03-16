###############################################################################
## Run Achilles                                                              ##


if (!require("DbDiagnostics")){
  remotes::install_github("OHDSI/DbDiagnostics")  
}

if (!require("DatabaseConnector")){
  remotes::install_github("OHDSI/DatabaseConnector")  
}

# Get started: https://ohdsi.github.io/DbDiagnostics/articles/RunAndUploadDbProfile.html
# Call the library
library(DbDiagnostics)

# Turn off the connection pane to speed up run time
options(connectionObserver = NULL)

if (!require('RSQLite')) {
  install.packages("RSQLite")
}

# if (!require('shiny')) {
#   install.packages("shiny")
# }

if (!require('DT')) {
  install.packages('DT')
}

args <- commandArgs(trailingOnly = TRUE)

sqliteDb = args[1]
outputFolder = args[2]
outputFile = args[3]

if (!file.exists(sqliteDb)) {
  stop("Error: SQLite database file not exists")
}

if (!dir.exists(outputFolder)) {
  stop("Error: output folder not exists")
}

# Create connection details
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "sqlite",
  server = sqliteDb
)

## Run in postgres database interface:
#\c ivan;
#CREATE SCHEMA reference;
#CREATE SCHEMA qc_result;

# The schema where your CDM-structured data are housed
cdmDatabaseSchema <- "main"

# The schema where your achilles results are or will be housed
resultsDatabaseSchema <- "qc_result"

# The schema where your vocabulary tables are housed, typically the same as the cdmDatabaseSchema
vocabDatabaseSchema <- cdmDatabaseSchema

# A unique, identifiable name for your database
cdmSourceName <- "export"

# The version of the OMOP CDM you are currently on, v5.3 and v5.4 are supported.
cdmVersion <- "5.4"

# Whether the function should append existing Achilles tables or create new ones
appendAchilles <- FALSE

# The schema where any missing achilles analyses should be written. Only set if appendAchilles = FALSE
writeTo <- "qc_result"

# Whether to round to the 10s or 100s place. Valid inputs are 10 or 100, default is 10.
roundTo <- 10

# Vector of concepts to exclude from the output. Note: No patient-level data is pulled as part of the package or included as part of the output
excludedConcepts <- c()

# Whether the DQD should be run as part of the profile exercise
addDQD <- FALSE


################################################################################
## Run achilles 
result <- Achilles::achilles(
  connectionDetails,
  cdmDatabaseSchema = "main",
  cdmVersion = cdmVersion,
  createTable = TRUE
)


##
                                                            ##


if (!require("DbDiagnostics")){
  remotes::install_github("OHDSI/DbDiagnostics")  
}

if (!require("DatabaseConnector")){
  remotes::install_github("OHDSI/DatabaseConnector")  
}

# Get started: https://ohdsi.github.io/DbDiagnostics/articles/RunAndUploadDbProfile.html
# Call the library
library(DbDiagnostics)

# Turn off the connection pane to speed up run time
options(connectionObserver = NULL)



# Create connection details
connectionDetails  <- DatabaseConnector::createConnectionDetails(
  dbms = "sqlite",
  server = sqliteDb,
)

#\c export;
#CREATE SCHEMA reference;
#CREATE SCHEMA qc_result;

# The schema where your CDM-structured data are housed
cdmDatabaseSchema <- "main"

# The schema where your achilles results are or will be housed
resultsDatabaseSchema <- "qc_result"

# The schema where your vocabulary tables are housed, typically the same as the cdmDatabaseSchema
vocabDatabaseSchema <- cdmDatabaseSchema

# A unique, identifiable name for your database
cdmSourceName <- "export"

# The version of the OMOP CDM you are currently on, v5.3 and v5.4 are supported.
cdmVersion <- "5.4"

# Whether the function should append existing Achilles tables or create new ones
appendAchilles <- FALSE

# The schema where any missing achilles analyses should be written. Only set if appendAchilles = FALSE
writeTo <- "qc_result"

# Whether to round to the 10s or 100s place. Valid inputs are 10 or 100, default is 10.
roundTo <- 10

# Vector of concepts to exclude from the output. Note: No patient-level data is pulled as part of the package or included as part of the output
excludedConcepts <- c()

# Whether the DQD should be run as part of the profile exercise
addDQD <- FALSE


################################################################################
## Run achilles 
result <- Achilles::achilles(
  connectionDetails,
  cdmDatabaseSchema = "main",
  resultsDatabaseSchema = "main",
#  sourceName = "export",
  cdmVersion = cdmVersion,
  createTable = TRUE
)

##
################################################################################


## Check

connection <- DatabaseConnector::connect(connectionDetails)
DatabaseConnector::getTableNames(connection, databaseSchema = "main")
DatabaseConnector::disconnect(connection)
cdmDatabaseSchema <- "main" # the fully qualified database schema name of the CDM
resultsDatabaseSchema <- "main" # the fully qualified database schema name of the results s

cdmSourceName <- "main" # a human readable name for your CDM source
cdmVersion <- "5.4" # the CDM version you are targetting. Currently supports 5.2, 5.3, and 5.4
# determine how many threads (concurrent SQL sessions) to use ----------------------------------------
numThreads <- 1 # on Redshift, 3 seems to work well
# specify if you want to execute the queries or inspect them ------------------------------------------
sqlOnly <- FALSE # set to TRUE if you just want to get the SQL scripts and not actually run the queries
sqlOnlyIncrementalInsert <- FALSE # set to TRUE if you want the generated SQL queries to calculate DQD r
sqlOnlyUnionCount <- 1 # in sqlOnlyIncrementalInsert mode, the number of check sqls to union in a singl
# NOTES specific to sqlOnly <- TRUE option ------------------------------------------------------------
# 1. You do not need a live database connection. Instead, connectionDetails only needs these parameters
# connectionDetails <- DatabaseConnector::createConnectionDetails(
# dbms = "", # specify your dbms
# )
# 2. Since these are fully functional queries, this can help with debugging.
# 3. In the results output by the sqlOnlyIncrementalInsert queries, placeholders are populated for execu
# 4. In order to use the generated SQL to insert metadata and check results into output table, you must
# where should the results and logs go? ----------------------------------------------------------------
# logging type -------------------------------------------------------------------------------------
verboseMode <- TRUE # set to FALSE if you don't want the logs to be printed to the console
# write results to table? ------------------------------------------------------------------------------
writeToTable <- TRUE # set to FALSE if you want to skip writing to a SQL table in the results schema
# specify the name of the results table (used when writeToTable = TRUE and when sqlOnlyIncrementalInsert
writeTableName <- "dqdashboard_results"
# write results to a csv file? -----------------------------------------------------------------------
writeToCsv <- FALSE # set to FALSE if you want to skip writing to csv file
csvFile <- "" # only needed if writeToCsv is set to TRUE
# if writing to table and using Redshift, bulk loading can be initialized ------------------------------
# Sys.setenv("AWS_ACCESS_KEY_ID" = "",
# "AWS_SECRET_ACCESS_KEY" = "",
# "AWS_DEFAULT_REGION" = "",
# "AWS_BUCKET_NAME" = "",
# "AWS_OBJECT_KEY" = "",
# "AWS_SSE_TYPE" = "AES256",
# "USE_MPP_BULK_LOAD" = TRUE)
# which DQ check levels to run -------------------------------------------------------------------
checkLevels <- c("TABLE", "FIELD", "CONCEPT")
2# which DQ checks to run? ------------------------------------
checkNames <- c() # Names can be found in inst/csv/OMOP_CDM_v5.3_Check_Descriptions.csv
# want to EXCLUDE a pre-specified list of checks? run the following code:
#
# checksToExclude <- c() # Names of check types to exclude from your DQD run
# allChecks <- DataQualityDashboard::listDqChecks()
# checkNames <- allChecks$checkDescriptions %>%
# subset(!(checkName %in% checksToExclude)) %>%
# select(checkName)
# which CDM tables to exclude? ------------------------------------
tablesToExclude <- c("CONCEPT", "VOCABULARY", "CONCEPT_ANCESTOR", "CONCEPT_RELATIONSHIP", "CONCEPT_CLASS", "DEATH", "LOCATION", "PAYER_PLAN_PERIOD", "NOTE", "NOTE_NLP", "SPECIMEM", "VISIT_DETAIL", "COST")
# run the job --------------------------------------------------------------------------------------
DataQualityDashboard::executeDqChecks(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  resultsDatabaseSchema = resultsDatabaseSchema,
  cdmSourceName = cdmSourceName,
  cdmVersion = cdmVersion,
  numThreads = numThreads,
  sqlOnly = sqlOnly,
  sqlOnlyUnionCount = sqlOnlyUnionCount,
  sqlOnlyIncrementalInsert = sqlOnlyIncrementalInsert,
  outputFolder = outputFolder,
  outputFile = outputFile,
  verboseMode = verboseMode,
  writeToTable = writeToTable,
  writeToCsv = writeToCsv,
  csvFile = csvFile,
  checkLevels = checkLevels,
  tablesToExclude = tablesToExclude,
  checkNames = checkNames
)
# inspect logs ----------------------------------------------------------------------------
# ParallelLogger::launchLogViewer(
#   logFileName = file.path(outputFolder,
#   sprintf("log_DqDashboard_%s.txt", cdmSourceName)))
#   # (OPTIONAL) if you want to write the JSON file to the results table separately ------------------------
#   jsonFilePath <- paste0(outputFolder, "/"+outputFile
# )

