
odata.opp.get<-function(user=keyring::key_list("odata.dev.shiny")[1,2], pass=keyring::key_get("odata.dev.shiny", "SHINYAPP_REQUEST"), url="https://my355441.crm.ondemand.com/sap/c4c/odata/v1/c4codataapi/OpportunityCollection?$filter=CreationDate%20ge%20datetime'2022-01-01T00:00:00'%20and%20CreationDate%20le%20datetime'2023-07-25T00:00:00'%20and%20SalesOrganisationID%20eq%20'NL01'", i=0){

  # Create a Progress object
  progress <- shiny::Progress$new()
  # Make sure it closes when we exit this reactive, even if there's an error
  on.exit(progress$close())
  j<-i
  if(j==0){
    progress$set(message = "Extracting Opportunity", value = j)
  } else {
    progress$inc(j, detail = paste("extracting 1000 more", j))
  }


  dt.get<-GET(url, authenticate(user, pass, type = "basic"))

  if(dt.get$status_code != 200){
    Sys.sleep(5)
  }
  dt.list<-content(dt.get, as="parsed")
  # uri_values <- lapply(dt.list$d$results, function(x) x$`__metadata`$uri)

  dat<-data.table::rbindlist(dt.list$d$results,use.names=TRUE, fill=TRUE, idcol="UID")
  dt.df<-unique(dat[, list(ID, ProcessingTypeCodeText, ProspectPartyID, Name, PrimaryContactPartyID,
                           PriorityCodeText, OriginTypeCodeText, LifeCycleStatusCodeText, ExternalUserStatusCodeText, SalesCycleCodeText, SalesCyclePhaseCodeText, ProbabilityPercent, ExpectedRevenueAmount, ExpectedRevenueAmountCurrencyCode,
                           SalesOrganisationID, SalesTerritoryID, MainEmployeeResponsiblePartyID,
                           PhaseProgressEvaluationStatusCodeText, MainEmployeeResponsiblePartyName,
                           SalesUnityPartyName, ProspectPartyName, SalesOrganisationName, SalesTerritoryName, CreationDate, Zoppstartdate_KUT, Zopptype_KUTText, Zproductpillar_KUT, Zinterproject_KUT, Zoppclosedate_KUT,
                           Zclassification_KUTText, Zprojectcountry_KUTText, Zprojectpostalcode_KUT, Zprojetcity_KUT, Zparentopportunity_KUT, SalesCyclePhaseCodeText, SalesCyclePhaseStartDate, HeaderRevenueSchedule, TotalExpectedNetAmount, TotalExpectedNetAmountAmountCurrencyCode, SalesRevenueForecastRelevanceIndicator, ExpectedProcessingStartDate, ExpectedProcessingEndDate, ExpectedRevenueStartDate, ExpectedRevenueEndDate, SalesForecastCategoryCodeText, ZExecutedengineeringhours_KUT, ZExpectedEngineeringHours_KUT, ZExpectedcalculationdate_KUT, ZReleasedForProduction_KUT, ZRevenueEndDate_KUT, ZRevenueStartDate_KUT, Zexpectedconstructiondate_KUT, Zexpecteddeliverydate_KUT, Zexpectedorderdate_KUT, ObjectID)])
  # dt.df[, uri := uri_values]
  # rm(uri_values)
  rm(dat)
  # print("writing file")
  filename<-paste0(tempdir(), "/", "OppData", ".csv")
  data.table::fwrite(dt.df,filename, append = TRUE, row.names = FALSE)
  # print("Done")
  # print(dt.list$d$`__next`)
  # print(names(dt.list$d))
  if(!c("__next") %in% names(dt.list$d)){

    progress$inc(j, detail = paste("cleaning", j))
    r.df<-read.csv(filename) |>
      mutate(CreationDate=as_datetime(as.numeric(gsub("\\D", "", CreationDate))/1000)) |>
      mutate(Zoppstartdate_KUT=as_datetime(as.numeric(gsub("\\D", "", Zoppstartdate_KUT))/1000)) |>
      mutate(SalesCyclePhaseStartDate=as_datetime(as.numeric(gsub("\\D", "", SalesCyclePhaseStartDate))/1000)) |>
      mutate(ExpectedProcessingStartDate=as_datetime(as.numeric(gsub("\\D", "", ExpectedProcessingStartDate))/1000)) |>
      mutate(ExpectedProcessingEndDate=as_datetime(as.numeric(gsub("\\D", "", ExpectedProcessingEndDate))/1000)) |>
      mutate(ExpectedRevenueStartDate=as_datetime(as.numeric(gsub("\\D", "", ExpectedRevenueStartDate))/1000)) |>
      mutate(ExpectedRevenueEndDate=as_datetime(as.numeric(gsub("\\D", "", ExpectedRevenueEndDate))/1000)) |>
      mutate(ZExpectedcalculationdate_KUT=as_datetime(as.numeric(gsub("\\D", "", ZExpectedcalculationdate_KUT))/1000)) |>
      mutate(ZReleasedForProduction_KUT=as_datetime(as.numeric(gsub("\\D", "", ZReleasedForProduction_KUT))/1000)) |>
      mutate(ZRevenueStartDate_KUT=as_datetime(as.numeric(gsub("\\D", "", ZRevenueStartDate_KUT))/1000)) |>
      mutate(ZRevenueEndDate_KUT=as_datetime(as.numeric(gsub("\\D", "", ZRevenueEndDate_KUT))/1000)) |>
      mutate(Zexpectedconstructiondate_KUT=as_datetime(as.numeric(gsub("\\D", "", Zexpectedconstructiondate_KUT))/1000)) |>
      mutate(Zexpecteddeliverydate_KUT=as_datetime(as.numeric(gsub("\\D", "", Zexpecteddeliverydate_KUT))/1000)) |>
      # mutate(uri=str_extract(uri, "'(.*?)'")) |>
      mutate(Zprojetcity_KUT=case_when(
        Zprojetcity_KUT=="'S HERTOGENBOSCH" ~  "'S-HERTOGENBOSCH",
        Zprojetcity_KUT=="HENGELO (O)" ~  "HENGELO",
        Zprojetcity_KUT=="'S GRAVENZANDE" ~  "'S-GRAVENZANDE",
        Zprojetcity_KUT=="DEN BOSCH" ~  "'S-HERTOGENBOSCH",
        Zprojetcity_KUT=="KRIMPEN A/D IJSSEL" ~  "KRIMPEN AAN DEN IJSSEL",
        Zprojetcity_KUT=="ALPHEN A/D RIJN" ~  "ALPHEN AAN DEN RIJN",
        Zprojetcity_KUT=="ALPHEN AAN DE RIJN" ~  "ALPHEN AAN DEN RIJN",
        Zprojetcity_KUT=="CAPELLE A/D IJSSEL" ~  "CAPELLE AAN DEN IJSSEL",
        Zprojetcity_KUT=="HENDRIK IDO AMBACHT" ~  "HENDRIK-IDO-AMBACHT",
        Zprojetcity_KUT=="SINT ANNALAND" ~  "SINT-ANNALAND",
        Zprojetcity_KUT=="NOORD SCHARWOUDE" ~  "NOORD-SCHARWOUDE",
        Zprojetcity_KUT=="RIJSWIJK BUITEN" ~  "RIJSWIJK-BUITEN",
        .default = Zprojetcity_KUT
      )
      )
    file.remove(filename)
    return(r.df)

  } else {
    j<-j+1
    return(
      odata.opp.get(url=dt.list$d$`__next`,user=keyring::key_list("odata.dev.shiny")[1,2], pass=keyring::key_get("odata.dev.shiny", "SHINYAPP_REQUEST"), i=j)
    )
  }

}
