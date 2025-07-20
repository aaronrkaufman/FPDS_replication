setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
library(tidyverse)
library(readr)
library(data.table)
library(mediocrethemes)
library(jsonlite)
library(arrow)
library(ggmap)

## https://r4ds.hadley.nz/arrow#sec-parquet
dat = arrow::open_dataset("./parquet")


### Pull cols so I can search through them
cols = colnames(dat)
cols[grepl("[Dd]olla", cols)]

dat %>% summarize(mean(is.na(`content.totalDollarValues.totalObligatedAmount`)),
                  mean(is.na(`content.dollarValues.obligatedAmount`))) %>%
  collect()



### What analyses do I want?

## Total Count
dat %>% summarize(count = n()) %>% collect() # 99,057,002

## Count by Year
## Dollars by Year
## Woman/Black owned by year
# NOTES:
## Some typo discrepancies between signedDate and effectiveDate
## signedDate seems better!
tmp = dat %>%
  mutate(dollars = as.numeric(content.totalDollarValues.totalObligatedAmount),
         year = year(as.Date(content.relevantContractDates.signedDate,
                             format = "%Y-%m-%d %H:%M:%S"))) %>%
  group_by(year) %>%
  summarize(count = n(),
            dollars = sum(dollars, na.rm=T),
            woman = sum(content.vendor.vendorSiteDetails.vendorSocioEconomicIndicators.isWomenOwned=="true",na.rm=T),
            black = sum(content.vendor.vendorSiteDetails.vendorSocioEconomicIndicators.minorityOwned.isBlackAmericanOwnedBusiness=="true",na.rm=T)) %>%
  collect()
tmp


## Plot volume over time
## Year on the x axis, dollars on the left y axis, count on the right y axis
tmp2 = tmp %>%filter(year >= 1979)
tmp2$logdollars = log(tmp2$dollars)
tmp2$logcount = log(tmp2$count)

p1 = ggplot(tmp2, aes(x = year, y = logdollars)) + 
  geom_line() + 
  xlab("Year") + 
  ylab("Total Dollars (log)") +
  mediocrethemes::theme_mediocre()
p2 = ggplot(tmp2, aes(x = year, y = logcount)) + 
  geom_line() + 
  xlab("Year") + 
  ylab("Contracts (log)") +
  mediocrethemes::theme_mediocre()
cowplot::plot_grid(p2,p1, nrow=2)
ggsave("E:/dropbox/apps/overleaf/fpds paper/figs/dollars_contracts.pdf",
       width=2400,height=2000, units="px")


### Next, geography
zips = dat %>%
  mutate(year = year(as.Date(content.relevantContractDates.signedDate,
                    format = "%Y-%m-%d %H:%M:%S")),
         zip = substr(start=1,stop=5,x=`content.vendor.vendorSiteDetails.vendorLocation.ZIPCode.#text`)) %>%
  select(zip, year) %>%
  group_by(zip, year) %>%
  summarize(count = n()) %>%
  collect()

zips2 = zips %>%
  filter(year>=2012 & year < 2022) %>%
  group_by(zip) %>%
  summarize(count = sum(count))

# https://www.census.gov/geo/maps-data/data/cbf/cbf_zcta.html
shp <- sf::st_read('tl_2020_us_zcta520.shp')

shp <- fortify(shp)
shp <- shp %>%
  left_join(zips2, by = c("ZCTA5CE20" = "zip"))
sum(is.na(shp$count))
shp <- shp %>% mutate(count = replace_na(count, 0))

shp <- shp %>% filter(!ZCTA5CE20 %in% as.character(c(99501:99950,96701:96898,
                                  96910:96932,96950:96952,
                                  96898,96799)))
shp <- shp %>% filter(!ZCTA5CE20 %in% as.character(c(99501:99950,96701:96898,
                                                     96910:96932,96950:96952,
                                                     96898,96799)))
shp <- shp %>% filter(substr(ZCTA5CE20,1,2)!="00" )

plot(shp[,"count"])



### Same, but with CDs
### Next, geography
### Contracts by state by year?
cds = dat %>%
  filter(!is.na(content.placeOfPerformance.placeOfPerformanceZIPCode),
         !is.na(`content.vendor.vendorSiteDetails.vendorLocation.state.#text`)) %>%
  mutate(dollars = as.numeric(content.totalDollarValues.totalObligatedAmount),
         year = year(as.Date(content.relevantContractDates.signedDate,
                             format = "%Y-%m-%d %H:%M:%S")),
         state = toupper(`content.vendor.vendorSiteDetails.vendorLocation.state.#text`),
         cd = content.vendor.vendorSiteDetails.vendorLocation.congressionalDistrictCode) %>%
  filter(year>=2012 & year < 2022,
         dollars > 0) %>%
  group_by(state,cd) %>%
  summarize(count = n(),
            dollars = sum(dollars, na.rm=T)) %>%
  collect()
cds

data(maps::state.fips)
state.fips$fips[nchar(state.fips$fips)==1] = paste0("0",state.fips$fips[nchar(state.fips$fips)==1])
cds <- left_join(cds, state.fips %>%
                    select(abb,fips) %>%
                    unique(), by = c("state" = "abb"))
cds$fips = as.character(cds$fips)
cds = cds %>% arrange(fips, cd)
## Get state fips for CDs


# https://www.census.gov/geo/maps-data/data/cbf/cbf_zcta.html
shp <- sf::st_read('tl_2014_us_cd114.shp')

shp <- fortify(shp)
shp <- shp %>%
  left_join(cds, by = c("STATEFP" = "fips", "CD114FP" = "cd"))

table(is.na(shp$count),shp$STATEFP) # fix the merge
sum(is.na(shp$count))
shp <- shp %>% mutate(count = log(count),
                      count = replace_na(count, 0.01))

shp2 <- shp %>% filter(!state %in% c("GU","PR", "MP","PW"),
                      STATEFP < 60,
                      STATEFP != "02",STATEFP != "15")

plot(shp2[,"count"],)

library(ggplot2)
ggplot(shp2 %>% mutate(`Contracts (log count)` = count),
       aes(fill=`Contracts (log count)`)) +
  geom_sf() + 
  theme_void()+
  scale_fill_gradient(low = "lightyellow", high = "darkred")+
  theme(legend.position = "bottom")
ggsave("E:/dropbox/apps/overleaf/fpds paper/figs/us_map.pdf",
       width=2400,height=2200, units="px")


### NAICS codes
naics = dat %>% pull(`content.productOrServiceInformation.principalNAICSCode.@description`)
naics2 = table(naics) %>% as.data.frame() %>% arrange(desc(Freq))

### Agency-level
agencies = dat %>%
  mutate(dollars = as.numeric(content.totalDollarValues.totalObligatedAmount),
         year = year(as.Date(content.relevantContractDates.signedDate,
                             format = "%Y-%m-%d %H:%M:%S")),
         state = toupper(`content.vendor.vendorSiteDetails.vendorLocation.state.#text`),
         cd = content.vendor.vendorSiteDetails.vendorLocation.congressionalDistrictCode,
         agency = `content.ID.ContractID.agencyID.@name`) %>%
  filter(!is.na(agency),
         year >= 1979) %>%
  group_by(agency,year) %>%
  summarize(count = n(),
            dollars = sum(dollars, na.rm=T)) %>%
  arrange(year, dollars) %>%
  collect()
agencies

## The dollar values are not very accurate for some of the early obs...
## Often it is just 0

top_agencies = agencies %>%
  group_by(agency) %>%
  summarize(count = sum(count)) %>%
  arrange(desc(count)) %>% 
  pull(agency)


agencies2 = agencies %>%
  filter(agency %in% top_agencies[c(1,3,4,5,7,8,10,12,15,16,21,22)]) %>%
  mutate(`Contracts (log count)` = log(count),
         agency = case_match(agency,
                             "DEPT OF DEFENSE" ~ "Dept of Defense",
                             "VETERANS AFFAIRS, DEPARTMENT OF" ~ "Veterans Affairs",
                             "PUBLIC BUILDINGS SERVICE" ~ "Public Buildings Service",
                             "STATE, DEPARTMENT OF" ~ "State Department",
                             "NATIONAL AERONAUTICS AND SPACE ADMINISTRATION" ~ "NASA",
                             "FEDERAL PRISON SYSTEM" ~ "Federal Prison System",
                             "FOREST SERVICE" ~ "Forest Service",
                             "NATIONAL INSTITUTES OF HEALTH" ~ "NIH",
                             "ENVIRONMENTAL PROTECTION AGENCY" ~ "EPA",
                             "DRUG ENFORCEMENT ADMINISTRATION" ~ "DEA",
                             "INTERNAL REVENUE SERVICE" ~ "IRS",
                             "NATIONAL OCEANIC AND ATMOSPHERIC ADMINISTRATION" ~ "NOAA"))

p3 = ggplot(agencies2,
            aes(x = year, y = `Contracts (log count)`)) +
  geom_line() + 
  facet_wrap(~agency) + 
  xlab("Year") +
  mediocrethemes::theme_mediocre()
p3
ggsave("E:/dropbox/apps/overleaf/fpds paper/figs/agencies.pdf",
       width=3400,height=1800, units="px")






## What columns do I want?
# Looking at 489th data set:
# 1,5,6,7,8,9,12,14,15,17,18,24,25,29,43,48,86,87,95,98,
# take 99:191 and pivot it to long
# 191:198,200,202,205,207,208,209,211,214,216,217,218,220,221,223,225,
# 240: protected


#na_cols = sapply(1:ncol(x), FUN= function(i) mean(is.na(x[,i])))



### Export a file
outdat = dat %>% 
  select(PIID = content.ID.ContractID.PIID,
         agency = `content.ID.ContractID.agencyID.@name`,
         naics = `content.productOrServiceInformation.principalNAICSCode.@description`,
         dollars = content.totalDollarValues.totalObligatedAmount,
         zip = content.placeOfPerformance.placeOfPerformanceZIPCode,
         cd = content.vendor.vendorSiteDetails.vendorLocation.congressionalDistrictCode,
         state = `content.vendor.vendorSiteDetails.vendorLocation.state.#text`,
         date = content.relevantContractDates.signedDate,
         smallbiz = content.vendor.vendorSiteDetails.vendorSocioEconomicIndicators.isSmallBusiness,
         woman = content.vendor.vendorSiteDetails.vendorSocioEconomicIndicators.isWomenOwned,
         minority = content.vendor.vendorSiteDetails.vendorSocioEconomicIndicators.minorityOwned.isMinorityOwned,
         url = `link.@href`,
         bids = content.competition.NumberOfOffersReceived,
         noncomp = `content.competition.typeOfSetAside.#text`
         ) %>%
    mutate(dollars = as.numeric(dollars),
         date = as.Date(date,
                             format = "%Y-%m-%d %H:%M:%S"),
         year = year(date)) %>%
  collect()

#object.size(outdat)

save(outdat, file="fpds_25nov2024.RData")
write.csv(outdat, file="fpds_25nov2024.csv")


#load("fpds_25nov2024.RData")

#### Technical validation section

lewis_rep = outdat %>% 
  mutate(year = year(date)) %>%
  filter(year %in% 2003:2015) %>%
  group_by(PIID) %>%
  summarize(dollars = sum(dollars)) %>%
  filter(dollars > 150000)

## Merge these PIIDs wtih the raw data set with the Dahlstrom covars

outdat2 = outdat %>% filter(PIID %in% lewis_rep$PIID) %>%
  distinct(PIID,.keep_all = T)

mean(outdat2$bids==1,na.rm=T)

lewis_orig = readstata13::read.dta13(("E:/Dropbox/fpds data/uspp_largec.dta"))


mean(lewis_orig$dollarsobligated_sum %in% outdat2$dollars)

test_merge = lewis_orig %>% left_join(outdat2, by = c("ca_sdate" = "date",
                                                      "dollarsobligated_sum" = "dollars"))

#### Potter
#codes = unique(tmp$`content.productOrServiceInformation.productOrServiceCode.@description`)
#codes[grepl("STUD", codes)]
#studies = "OTHER SPECIAL STUDIES AND ANALYSES"

#potter_orig = readstata13::read("E:/Dropbox/fpds data/research_contracts.dta")



sum(tmp$`content.productOrServiceInformation.productOrServiceCode.@description`==studies, na.rm=T)

potter = dat %>% 
  mutate(date = content.relevantContractDates.signedDate,
         date = as.Date(date,
                        format = "%Y-%m-%d %H:%M:%S"),
         year = year(date)) %>%
  filter(`content.productOrServiceInformation.productOrServiceCode.@description`==studies,
         content.dollarValues.baseAndAllOptionsValue >= 1000,
         year %in% 2001:2019) %>%
  collect()

## Find the two singled-out cases: 
# 2013 the Department of Education funded a $3 million longitudinal study on high school
# 2013 Department of Health and Human Services funded a $685,000 survey to monitor influenza vaccination