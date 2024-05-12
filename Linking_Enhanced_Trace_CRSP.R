####
####
#  R Code to create linking table from Enhanced TRACE to CRSP
#   
# Authors: Diego Bonelli & Katsiaryna Falkovich
# Date: 5/12/2022
#  
#               
# Notes: 
#     1) 
#
#
####
####


# clear all
rm(list = ls())

# set wd and load functions
# Needed CRSP and Mergent files in sas format
# Required CRSP in Data/CRSP
# Required Mergent files in Data/Mergent/
# Required Enhanced Trace in Data/Trace/
# temp files are saved in Data/temp
# final dataset is saved in Data/Dataset
setwd(" ")
# Load Libraries
suppressWarnings(suppressMessages({
  require(lubridate, quietly = T) # Useful package for dates
  require(zoo, quietly = T) # Useful package for dates (yearmon format)
  require(dplyr, quietly = T) # usefull for ntile function
  require(data.table, quietly = T) # data.table
  require(haven, quietly = T) # read sas files
  require(bizdays, quietly = T) # package for business days
}))

load_rmetrics_calendars(1970:2030)

#
## load data ----
#

# load data from trace
trace=fread("Data/Trace/trace_enhanced.csv",nThread=setDTthreads(0L),
            select=c("trd_exctn_dt","cusip_id","company_symbol"))

# aggregate to daily
trace=trace[,.(company_symbol=unique(company_symbol))
      ,by=c("cusip_id","trd_exctn_dt")]
fwrite(trace,file="Data/temp/trace_daily_to_merge.csv",nThread=setDTthreads(0L))


trace=fread(file="Data/temp/trace_daily_to_merge.csv",nThread=setDTthreads(0L))

# set dates
trace[,trd_exctn_dt:=as.Date(trd_exctn_dt)
      ][,datem:=as.yearmon(trd_exctn_dt,"%Y%m%d")]

# load bond issue information
fisd_issue <- read_sas("Data/Mergent/fisd_issue.sas7bdat",NULL)
fisd_issue=as.data.table(fisd_issue)
names(fisd_issue)=tolower(names(fisd_issue))
fisd_issue=unique(fisd_issue[,list(issuer_id,issuer_cusip,offering_date,bond_type,maturity,complete_cusip)])

# merge with trace
trace=merge(trace,fisd_issue,by.x="cusip_id",by.y="complete_cusip")

trace[,min_trace_date:=min(trd_exctn_dt,na.rm=T),by=list(cusip_id,company_symbol)
][,max_trace_date:=max(trd_exctn_dt,na.rm=T),by=list(cusip_id,company_symbol)]

# get from with month to which month cusip_id-company symbol are the same
trace1=copy(trace)[,list(cusip_id,trd_exctn_dt,datem,company_symbol,maturity)
][datem>="Jul 2002"
][!is.na(trd_exctn_dt)]

# issuer cusip for each issuer id
fisd_issue=unique(fisd_issue[,list(issuer_id,issuer_cusip)])

# add parent to issue
# get issuer info
fisd_issuer <- read_sas("Data/Mergent/fisd_issuer.sas7bdat",NULL)
fisd_issuer=as.data.table(fisd_issuer)
names(fisd_issuer)=tolower(names(fisd_issuer))
# get parent info
fisd_issuer=fisd_issuer[agent_id==parent_id][,list(issuer_id,parent_id,cusip_name)]
# add parent info to issue
fisd=fisd_issuer[fisd_issue,on='issuer_id']
fisd=na.omit(fisd)
fisd$issuer_id=NULL
# parent cusip is cusip(s) of issuers that have the agent_id of the parent
# (same agent_id can have several issuers)
setnames(fisd,'issuer_cusip','parent_cusip')
setnames(fisd,'cusip_name','parent_name')

# add parent cusip to issuer info
fisd_issuer <- read_sas("Data/Mergent/fisd_issuer.sas7bdat",NULL)
fisd_issuer=as.data.table(fisd_issuer)
names(fisd_issuer)=tolower(names(fisd_issuer))
fisd_issuer=fisd_issuer[,list(issuer_id,parent_id,cusip_name)]
fisd=merge(fisd_issuer,fisd,by='parent_id',allow.cartesian = T,all.x=T)

# merge with issue data
fisd=merge(trace,fisd,by='issuer_id',allow.cartesian = T)

# load crsp names and adjust
CRSP_names = read_sas("Data/CRSP/msenames.sas7bdat", NULL )
names(CRSP_names)=tolower(names(CRSP_names))
CRSP_names=as.data.table(CRSP_names)
CRSP_names=CRSP_names[,list(permno, permco,namedt,nameendt,ncusip,tsymbol,comnam)]

CRSP_names$ncusip[which(CRSP_names$ncusip=="")]=NA
CRSP_names$tsymbol[which(CRSP_names$tsymbol=="")]=NA

# old firms (before 1968) are missing ncusips -- fill them
CRSP_names[order(namedt),ncusip :=na.locf(ncusip ,na.rm=F),by='permco'
][order(-namedt),ncusip :=na.locf(ncusip ,na.rm=F),by='permco']

# sometimes there are gaps in symbols (usually around cusip change)
# fill the gaps with the symbol from before/after for the same permco-firm ncusip
# firm 6 digit cusip
CRSP_names=CRSP_names[,issuer_cusip:=substr(ncusip,0,6)][,-c("ncusip")
][,enddate_permco:=max(nameendt,na.rm=T),by="permco"]
# fill symbol
CRSP_names[order(namedt),tsymbol:=na.locf(tsymbol,na.rm=F),by=c('permco','issuer_cusip')
][order(-namedt),tsymbol:=na.locf(tsymbol,na.rm=F),by=c('permco','issuer_cusip')]


# read delisting file
CRSP_delist = read_sas("Data/CRSP/msedelist.sas7bdat", NULL)
names(CRSP_delist)=tolower(names(CRSP_delist))
CRSP_delist=as.data.table(CRSP_delist)

# remove available code 
CRSP_delist=CRSP_delist[!dlstcd%in%c(100)][nwcomp!="0"]

# create linking table following permco through time 
# add crsp names to delisting
CRSP_delist=unique(CRSP_delist[,list(permno,permco,dlstdt,nwcomp)])
CRSP_delist1=merge(CRSP_delist,CRSP_names,by=c("permno","permco"))
# 
CRSP_delist1=unique(CRSP_delist1[dlstdt==nameendt][,list(dlstdt,permco,nwcomp)])
names(CRSP_delist1)[2]=c("old_permco")
names(CRSP_delist1)[3]=c("permco")

CRSP_delist1=merge(CRSP_delist1,CRSP_names[,list(permco,namedt,nameendt,tsymbol,comnam)],by=c("permco"),allow.cartesian = T)

CRSP_delist1=unique(CRSP_delist1)
CRSP_delist1=CRSP_delist1[nameendt>=dlstdt
][,namedt:=as.Date(ifelse(namedt<=dlstdt,dlstdt+1,namedt))
][,dlstdt:=NULL]

CRSP_names1=CRSP_names[,list(permco,tsymbol,comnam,namedt,nameendt)]
names(CRSP_names1)[1]="old_permco"
names(CRSP_names1)[4:5]=c("namedt_old","nameendt_old")

table_permco=rbind(CRSP_names1,CRSP_delist1,fill=T)

table_permco[,last_link_pre:=min(namedt),by=list(permco,old_permco)]

table_permco[,":="(permco=fcoalesce(permco,old_permco),
                   namedt=fcoalesce(namedt,namedt_old),
                   nameendt=fcoalesce(nameendt,nameendt_old))
][,namedt_old:=NULL][,nameendt_old:=NULL]


#
## issuer cusip ----
#

# merge with bond data, follow the bond through time
# merge on permco
fisd1=unique(fisd[,list(issuer_id,issuer_cusip,cusip_id,company_symbol,max_trace_date,min_trace_date,offering_date)])
fisd1=merge(fisd1,CRSP_names,by="issuer_cusip",allow.cartesian = T)

# get min and max dates for each issuer cusip and filter offering date in between
fisd1=fisd1[,":="(namedt=min(namedt,na.rm=T),
                  nameendt=max(nameendt,na.rm=T)),
            by=list(cusip_id,issuer_cusip)
][offering_date>=namedt&
    nameendt>offering_date]
# unique obs
fisd1=unique(fisd1)

# merge with mergers table and get series of permco-cusip links
fisd1=unique(fisd1[,list(cusip_id,permco,offering_date,issuer_cusip,company_symbol)])
fisd1=merge(table_permco,fisd1,by.y="permco",by.x="old_permco",allow.cartesian = T, all.x=T)
uniqueN(fisd1$cusip_id)

# remove splits when bond is issued after the split (should stay with old_permco)
# if old_permco==permco (before mergers and splits), don't check last_link_pre
fisd1=fisd1[nameendt>=offering_date|is.na(offering_date)]
fisd1=fisd1[(last_link_pre>=offering_date|old_permco==permco)|is.na(offering_date)]

table_permco_orig=copy(table_permco)
table_permco=fisd1

x=0

while(x!=dim(table_permco)[1]){
  x=dim(table_permco)[1]
  
  table_permco1=unique(copy(table_permco)[,permco_new:=ifelse(permco!=old_permco,permco,NA)
  ][,mindate:=min(nameendt),by=c("old_permco","permco")
  ][,list(old_permco,permco_new,mindate,cusip_id,offering_date,company_symbol)
  ][!is.na(permco_new)])
  
  table_permco1=merge(table_permco1,
                      table_permco[,list(old_permco,tsymbol,comnam,permco,namedt,nameendt,last_link_pre)],
                      by.x=c("permco_new"),
                      by.y=c("old_permco"),allow.cartesian = T)
  
  table_permco1=table_permco1[mindate<=namedt
  ][,-c("mindate","permco_new")][!is.na(last_link_pre)]
  
  table_permco=rbind(table_permco,table_permco1,fill=T)
  table_permco=unique(table_permco)
}

fisd1=table_permco[!is.na(cusip_id)]

rm(fisd_issue,fisd_issuer,CRSP_names1,CRSP_delist,CRSP_delist1)


## check if multiple permco per cusip

# create dates 
date.eom.bd <- unique(preceding(seq(as.Date(min(fisd1$offering_date,na.rm=T)),  as.Date(today()), by="1 day") - 1, "Rmetrics/NYSE"))

complete_cusip <- data.table(expand.grid(date=date.eom.bd,cusip_id=unique(fisd1$cusip_id)))

complete_cusip[,date1:=date]

# remove the case when namedt>nameendt -- happens when there is a merger and name change of the acquiring company the same day;
# there is an observation next day
fisd1=fisd1[nameendt>=namedt]

# remove if after today
fisd1=fisd1[namedt< as.Date(today())]

fisd1=complete_cusip[fisd1,on=.(cusip_id,
                                date1>=namedt,
                                date1<=nameendt)]

rm(complete_cusip)

# a few cases when date is na because the short period of merger and name change of the acquiring company falls on non working day
# remove these
fisd1=fisd1[!is.na(date)]
gc()

# number of rows for cusip-date
fisd1[,flag:=.N,by=c("cusip_id","date")]
table(fisd1$flag)

# check whether any of the permco had at certain point the same trading symbol
fisd1=unique(fisd1)

#number of permnos for those with several observations during the date
fisd1[flag>1,number_permco:=length(unique(permco)),by=c("cusip_id","date")]

# for those that have several permcos, mark if can match by symbol
fisd1[number_permco>1,matched_by_symbol:=any(tsymbol==company_symbol),by=c("cusip_id","permco","date")]

# keep if one observation for the date, or if one unique permco, or if matched by symbol
fisd1[flag==1|number_permco==1|matched_by_symbol==T,link_type:="C"]

# flag if company symbol matched at certain point
fisd1[,any_symbol_match:=any(tsymbol==company_symbol),by=c("cusip_id","permco")]

# in case cusip_id date has another match link_type is non_link else is secondary
fisd1[number_permco>1,other_match:=all(is.na(link_type)),by=c("cusip_id","date")
][is.na(link_type)&other_match==T&any_symbol_match==T,link_type:="P"
][is.na(link_type)&other_match==T,link_type:="S"
][,link_type:=fcoalesce(link_type,"N")]

# get trace dates
trace2=unique(copy(trace)[,min_trace_date:=min(min_trace_date,na.rm=T),by=list(cusip_id)
][,max_trace_date:=max(max_trace_date,na.rm=T),by=list(cusip_id)
][,list(min_trace_date,max_trace_date,cusip_id,maturity)])
fisd1=merge(fisd1,trace2,by="cusip_id")

# filter dates
fisd1=fisd1[date>=offering_date&date<=maturity]


# set order dates
setorder(fisd1,cusip_id,permco,date)

# create a counter id based on cusip_id, permco and number_permco
fisd1$counter <- with(fisd1, rleid(cusip_id, permco,link_type))
# unify dates
fisd1[,":="(date1=min(date1,na.rm=T),
            date1.1=max(date1.1,na.rm=T)),by=list(cusip_id,permco,counter)]
# fisd1[,":="(min_date=min(date,na.rm=T),
#             max_date=max(date,na.rm=T)),by=list(cusip_id,permco,counter)]
# get unique obs
fisd1=unique(fisd1[,list(date1,date1.1,cusip_id,permco,link_type)])
names(fisd1)[1:2]=c("date_st","date_end")
fisd1=fisd1[!link_type%in%c("N")]

save(fisd1,file="Data/temp/lt_issuer.rdata")
#
## parent cusip ----
#

# use parent cusip as issuer cusip as before (only for unmatched cusips matched to permnos without mergers or )
fisd2=unique(fisd[!cusip_id%in%unique(fisd1$cusip_id)][!is.na(parent_cusip)
][,list(issuer_id,issuer_cusip,cusip_id,company_symbol,
        max_trace_date,min_trace_date,offering_date,parent_cusip,bond_type)])
# merge with parent cusips
fisd2=merge(fisd2,CRSP_names,by.x="parent_cusip",by.y="issuer_cusip",allow.cartesian = T)
fisd2[,parent_permco:=permco]

# trace back permco's of parents
fisd2=unique(fisd2[,list(cusip_id,permco,offering_date,max_trace_date,min_trace_date,issuer_cusip,company_symbol,bond_type,parent_permco)])
# use table_permco_orig to get permco's before the last merger
t=merge(table_permco_orig,fisd2,by="permco",allow.cartesian = T, all.x = T)

# move backwards to get all old_permcos
t=unique(t[,list(permco,old_permco,cusip_id)])

x=0
while(x!=dim(t)[1]){
  x=dim(t)[1]
  
  t1=unique(copy(t)[,permco_old_new:=ifelse(permco!=old_permco,old_permco,NA)])
  t1=unique(t1[,list(permco,permco_old_new,cusip_id)])[!is.na(permco_old_new)]
  
  t1=merge(t1,
           table_permco_orig[,list(old_permco,permco)],
           by.x=c("permco_old_new"),
           by.y=c("permco"),
           allow.cartesian = T)
  t=rbind(t,t1,fill=T)
  t=unique(t[!is.na(cusip_id)])
}

#in the end, for each cusip, we have all starting points to reach parent permco(s)
t=t[,list(old_permco,cusip_id)]
t=unique(t)

# add bond information
t=merge(t,unique(fisd2[,list(cusip_id,offering_date)]) #without symbol for now
        ,by="cusip_id",allow.cartesian = T)

# track the bond forward from old_permco (as in the issuer matching)
t1=merge(table_permco_orig,t,by="old_permco",allow.cartesian = T, all.x=T)

# filter on offering date
t1[,":="(min_dt=min(namedt,na.rm=T),
         max_dt=max(nameendt,na.rm=T)),
   by=list(cusip_id,old_permco,permco)]
t1=t1[(offering_date>=min_dt&max_dt>offering_date)|old_permco!=permco
][nameendt>offering_date
][,-c("min_dt","max_dt")]

# remove old_permcos if after the merger
t1=t1[last_link_pre>offering_date|is.na(last_link_pre)]

# track forward
x=0
while(x!=dim(t1)[1]){
  x=dim(t1)[1]
  
  table_permco1=unique(copy(t1)[,permco_new:=ifelse(permco!=old_permco,permco,NA)
  ][,mindate:=min(nameendt),by=c("old_permco","permco")
  ][,list(old_permco,permco_new,mindate)
  ][!is.na(permco_new)])
  
  table_permco1=unique(table_permco1[,list(old_permco,permco_new,mindate)])
  
  table_permco1=merge(table_permco1,
                      t1[,list(cusip_id,old_permco,tsymbol,comnam,permco,namedt,nameendt,last_link_pre,offering_date)],
                      by.x=c("permco_new"),
                      by.y=c("old_permco"),allow.cartesian = T)
  
  table_permco1=table_permco1[mindate<=namedt
  ][,-c("mindate","permco_new")][!is.na(last_link_pre)]
  
  t1=rbind(t1,table_permco1,fill=T)
  t1=unique(t1)
}

# add bond info
fisd2=merge(t1, fisd2[,list(cusip_id,max_trace_date,min_trace_date,company_symbol,bond_type, parent_permco)], by="cusip_id",allow.cartesian = T)
fisd2=unique(fisd2)

# Branches should eventually lead to the parent, remove if they don't
fisd2[,parent_branch:=any(permco==parent_permco),by=c("cusip_id","old_permco")]
fisd2=fisd2[parent_branch==T][,-c("parent_branch","parent_permco")]
fisd2=unique(fisd2)

# There are several permcos that lead to the same parent. Identify correct permcos by symbol
# check that the symbol is valid at the time, check that the symbol matches
fisd2[,symbol_valid:=(max_trace_date>=namedt&nameendt>=min_trace_date)
][,symbol_checked:=(tsymbol==company_symbol)]
# identify permcos by symbol that is valid at the time
fisd2[,checked_in_time:=symbol_valid==T&symbol_checked==T]

# to identify the correct branch (when there are several), first number chronological permco appearances
# (first symbol match means that the correct branch should reach this point first)
fisd2=fisd2[order(min_trace_date)][checked_in_time==T,permco_order:=.GRP,by=c("cusip_id","permco")]

# get old_permco that starts the symbol match
fisd2[,checked_start_indiv:=old_permco==permco&checked_in_time==T
][,checked_start:=checked_start_indiv==T&permco_order==min(permco_order,na.rm=T),by="cusip_id"
][,checked_start:=fcoalesce(checked_start,F)]
#mark the whole branch as checked (move forward)
fisd2[,checked_full:=any(checked_start==T),by=list(cusip_id,old_permco)]
## Matched permcos start only from the period when the bond is in trace

# Mark other branches as remove if one branch is checked_full
fisd2[,remove:=cusip_id%in%unique(fisd2[checked_full==T]$cusip_id)&checked_full==F]
uniqueN(fisd2[checked_full==T]$cusip_id) #100108


# Split cases, run the same code as in issuer matching, only for checked_full==T (have symbol match)
## check if multiple permco per cusip

# Do in batches of 10000 cusips
k=fisd2[remove==F&checked_full==T]
f=NULL
cusips=unique(k$cusip_id)
Ncusips=uniqueN(k$cusip_id)
for (i in 1:ceiling(Ncusips/10000)) {
  n=i*10000
  i=i*10000-10000+1
  print(i)
  print(n)
  
  k=fisd2[remove==F&checked_full==T]
  # create dates 
  date.eom.bd <- unique(preceding(seq(as.Date(min(k[cusip_id%in%cusips[i:n]]$offering_date,na.rm=T)),  as.Date(today()), by="1 day") - 1, "Rmetrics/NYSE"))
  
  complete_cusip <- data.table(expand.grid(date=date.eom.bd,cusip_id=unique(k[cusip_id%in%cusips[i:n]]$cusip_id)))
  complete_cusip[,date1:=date]
  
  
  # remove the case when namedt>nameendt -- happens when there is a merger and name change of the acquiring company the same day;
  # there is an observation next day
  k=k[nameendt>=namedt]
  
  # remove if after today
  k=k[namedt< as.Date(today())]
  
  k=complete_cusip[k,on=.(cusip_id,
                          date1>=namedt,
                          date1<=nameendt)]
  
  # a few cases when date is na because the short period of merger and name change of the acquiring company falls on non working day
  # remove these
  k=k[!is.na(date)]
  rm(complete_cusip)
  
  # number of rows for cusip-date
  k[,flag:=.N,by=c("cusip_id","date")]
  #table(k$flag)
  
  # check whether any of the permco had at certain point the same trading symbol
  k=unique(k)
  
  #number of permnos for those with several observations during the date
  k[flag>1,number_permco:=length(unique(permco)),by=c("cusip_id","date")]
  
  # for those that have several permcos, mark if can match by symbol
  k[number_permco>1,matched_by_symbol:=any(tsymbol==company_symbol),by=c("cusip_id","permco","date")]
  
  # keep if one observation for the date, or if one unique permco, or if matched by symbol
  k[flag==1|number_permco==1|matched_by_symbol==T,link_type:="C"]
  
  # flag if company symbol matched at certain point
  k[,any_symbol_match:=any(tsymbol==company_symbol),by=c("cusip_id","permco")]
  
  # in case cusip_id date has another match link_type is non_link else is secondary
  k[number_permco>1,other_match:=all(is.na(link_type)),by=c("cusip_id","date")
  ][is.na(link_type)&other_match==T&any_symbol_match==T,link_type:="P"
  ][is.na(link_type)&other_match==T,link_type:="S"
  ][,link_type:=fcoalesce(link_type,"N")]
  
  k=k[,-c("max_trace_date","min_trace_date")]
  # get trace dates
  trace2=unique(copy(trace)[,min_trace_date:=min(min_trace_date,na.rm=T),by=list(cusip_id)
  ][,max_trace_date:=max(max_trace_date,na.rm=T),by=list(cusip_id)
  ][,list(min_trace_date,max_trace_date,cusip_id,maturity)])
  
  k=merge(k,trace2,by="cusip_id")
  
  # filter dates
  k=k[date>=offering_date&date<=maturity]
  
  
  # set order dates
  setorder(k,cusip_id,permco,date)
  
  # create a counter id based on cusip_id, permco and number_permco
  k$counter <- with(k, rleid(cusip_id, permco,link_type))
  # unify dates
  k[,":="(date1=min(date1,na.rm=T),
          date1.1=max(date1.1,na.rm=T)),by=list(cusip_id,permco,counter)]
  # get unique obs
  k=unique(k[,list(date1,date1.1,cusip_id,permco,link_type)])
  names(k)[1:2]=c("date_st","date_end")
  
  f=rbind(f,k)
}

# observations that we used in the check above
k=fisd2[remove==F&checked_full==T]
# all other
f2=fisd2[remove!=F|checked_full!=T]

# bonds that have no symbol match
f2[,no_match:=!cusip_id%in%unique(fisd2[checked_full==T]$cusip_id)]
table(f2$no_match)
uniqueN(f2[no_match==T]$cusip_id) #9610

rm(CRSP_delist1, CRSP_names1, fisd_issue, fisd_issuer, t, t1, table_permco, table_permco1)


# Table
# remove the case when namedt>nameendt -- happens when there is a merger and name change of the acquiring company the same day;
# there is an observation next day
f2=f2[nameendt>=namedt]

# remove if after today
f2=f2[namedt< as.Date(today())]

# table k has the same types as matching by issuer tables

# checked_full -- the full branch, starting permco matched by symbol within valid dates
# remove -- other branches of the cusip if there is one checked_full branch
# no_match -- no valid symbol match to identify correct branch

# remove
f2=f2[remove!=T]
# mark if no match
f2[no_match==T,link_type:="NM"]

f2=f2[,-c("min_trace_date","max_trace_date")]

f2=merge(f2,trace2,by="cusip_id")

# filter dates
f2=unique(f2[namedt<=maturity][,list(cusip_id,permco,namedt,nameendt,link_type)])

names(f2)[3:4]=c("date_st","date_end")

#bind together
fisd2=rbind(f,f2)

# filter
fisd2=fisd2[!link_type%in%c("NM","N")]

save(fisd2,file="Data/temp/lt_parent.rdata")



#
## match by issuer_cusip and symbol regardless of the time ----
#

fisd3=fisd[!cusip_id%in%unique(fisd2$cusip_id)][!cusip_id%in%unique(fisd1$cusip_id)]
uniqueN(fisd3$cusip_id)

# merge on cusip and check the symbol
fisd3=unique(fisd3[,list(issuer_id,issuer_cusip,cusip_id,company_symbol,max_trace_date,min_trace_date,offering_date)])
fisd3=merge(fisd3,CRSP_names,by="issuer_cusip")

# get min and max dates for each parent cusip and filter offering date in between
fisd3=fisd3[,":="(namedt=min(namedt,na.rm=T),
                  nameendt=max(nameendt,na.rm=T)),
            by=list(cusip_id,issuer_cusip)
][max_trace_date>=namedt&
    nameendt>=min_trace_date]
# unique obs
fisd3=unique(fisd3)

# merge with mergers table and get series of permco-cusip links
fisd3=unique(fisd3[,list(cusip_id,permco,offering_date,issuer_cusip,company_symbol)])
fisd3=merge(table_permco_orig,fisd3,by.y="permco",by.x="old_permco",allow.cartesian = T, all.x=T)

# remove splits when bond is issued after the split (should stay with old_permco)
# if old_permco==permco (before mergers and splits), don't check last_link_pre
fisd3=fisd3[nameendt>=offering_date|is.na(offering_date)]
fisd3=fisd3[(last_link_pre>=offering_date|old_permco==permco)|is.na(offering_date)]

table_permco=fisd3
x=0

while(x!=dim(table_permco)[1]){
  x=dim(table_permco)[1]
  
  table_permco1=unique(copy(table_permco)[,permco_new:=ifelse(permco!=old_permco,permco,NA)
  ][,mindate:=min(nameendt),by=c("old_permco","permco")
  ][,list(old_permco,permco_new,mindate,cusip_id,offering_date,company_symbol)
  ][!is.na(permco_new)])
  
  table_permco1=merge(table_permco1,
                      table_permco[,list(old_permco,tsymbol,comnam,permco,namedt,nameendt,last_link_pre)],
                      by.x=c("permco_new"),
                      by.y=c("old_permco"),allow.cartesian = T)
  
  table_permco1=table_permco1[mindate<=namedt
  ][,-c("mindate","permco_new")][!is.na(last_link_pre)]
  
  table_permco=rbind(table_permco,table_permco1,fill=T)
  table_permco=unique(table_permco)
}

fisd3=table_permco[!is.na(cusip_id)]

# check if multiple permco per cusip
# create dates 
date.eom.bd <- unique(preceding(seq(as.Date(min(fisd3$offering_date,na.rm=T)),  as.Date(today()), by="1 day") - 1, "Rmetrics/NYSE"))
complete_cusip <- data.table(expand.grid(date=date.eom.bd,cusip_id=unique(fisd3$cusip_id)))
complete_cusip[,date1:=date]

# remove the case when namedt>nameendt -- happens when there is a merger and name change of the acquiring company the same day;
# there is an observation next day
fisd3=fisd3[nameendt>=namedt]

# remove if after today
fisd3=fisd3[namedt< as.Date(today())]

fisd3=complete_cusip[fisd3,on=.(cusip_id,
                                date1>=namedt,
                                date1<=nameendt)]

# a few cases when date is na because the short period of merger and name change of the acquiring company falls on non working day
# remove these
fisd3=fisd3[!is.na(date)]
rm(complete_cusip)

# number of rows for cusip-date
fisd3[,flag:=.N,by=c("cusip_id","date")]
table(fisd3$flag)

# check whether any of the permco had at certain point the same trading symbol
fisd3=unique(fisd3)

#number of permnos for those with several observations during the date
fisd3[flag>1,number_permco:=length(unique(permco)),by=c("cusip_id","date")]
table(fisd3$number_permco)

# for those that have several permcos, mark if can match by symbol
fisd3[number_permco>1,matched_by_symbol:=any(tsymbol==company_symbol),by=c("cusip_id","permco","date")]
table(fisd3$matched_by_symbol)

# keep if one observation for the date, or if one unique permco, or if matched by symbol
fisd3[flag==1|number_permco==1|matched_by_symbol==T,link_type:="C"]

# flag if company symbol matched at certain point
fisd3[,any_symbol_match:=any(tsymbol==company_symbol),by=c("cusip_id","permco")]

# in case cusip_id date has another match link_type is non_link else is secondary
fisd3[number_permco>1,other_match:=all(is.na(link_type)),by=c("cusip_id","date")
][is.na(link_type)&other_match==T&any_symbol_match==T,link_type:="P"]

# in this case S and N are wrong matches hence we remove them
fisd3=fisd3[link_type%in%c("C","P")]

# get trace dates
trace2=unique(copy(trace)[,min_trace_date:=min(min_trace_date,na.rm=T),by=list(cusip_id)
][,max_trace_date:=max(max_trace_date,na.rm=T),by=list(cusip_id)
][,list(min_trace_date,max_trace_date,cusip_id,maturity)])

fisd3=merge(fisd3,trace2,by="cusip_id")

# filter dates
fisd3=fisd3[date>=offering_date&date<=maturity]

# set order dates
setorder(fisd3,cusip_id,permco,date)

# create a counter id based on cusip_id, permco and number_permco
fisd3$counter <- with(fisd3, rleid(cusip_id, permco,link_type))
# unify dates
fisd3[,":="(date1=min(date1,na.rm=T),
            date1.1=max(date1.1,na.rm=T)),by=list(cusip_id,permco,counter)]
# get unique obs
fisd3=unique(fisd3[,list(date1,date1.1,cusip_id,permco,link_type)])
names(fisd3)[1:2]=c("date_st","date_end")
save(fisd3,file="Data/temp/lt_symbol.rdata")


#
## match by symbol and time ----
#

fisd4=fisd[!cusip_id%in%unique(fisd2$cusip_id)][!cusip_id%in%unique(fisd1$cusip_id)][!cusip_id%in%unique(fisd3$cusip_id)]
uniqueN(fisd4$cusip_id)

# merge on symbols
fisd4=unique(fisd4[,list(issuer_id,issuer_cusip,cusip_id,company_symbol,
                         max_trace_date,min_trace_date,offering_date)][!is.na(company_symbol)])
fisd4=merge(fisd4,CRSP_names,by.x="company_symbol",by.y="tsymbol",allow.cartesian = T)

# get min and max dates for each permco and filter on valid name in between
fisd4=fisd4[,":="(namedt=min(namedt,na.rm=T),
                  nameendt=max(nameendt,na.rm=T)),
            by=list(cusip_id,permco)
][max_trace_date>=namedt&
    nameendt>min_trace_date]

# get only min overlapping dates between trace and crsp
fisd4[,min_trace_date:=pmax(namedt,min_trace_date)][,max_trace_date:=pmin(nameendt,max_trace_date)]

# unique obs
fisd4=unique(fisd4)

# merge with mergers table and get series of permco-cusip links
fisd4=unique(fisd4[,list(cusip_id,permco,offering_date,company_symbol,max_trace_date,min_trace_date)])

# store data to attach dates back to check company symbol tree
table_symbol=copy(fisd4)
fisd4=merge(table_permco_orig,fisd4,by.y="permco",by.x="old_permco",allow.cartesian = T, all.x=T)

# remove splits when bond is issued after the split (should stay with old_permco)
# if old_permco==permco (before mergers and splits), don't check last_link_pre
fisd4=fisd4[nameendt>=offering_date|is.na(offering_date)]
fisd4=fisd4[(last_link_pre>=offering_date|old_permco==permco)|is.na(offering_date)]

# fisd4=fisd4[permco!=old_permco|(max_trace_date>=namedt&nameendt>min_trace_date)]


table_permco=fisd4

x=0

while(x!=dim(table_permco)[1]){
  x=dim(table_permco)[1]
  
  table_permco1=unique(copy(table_permco)[,permco_new:=ifelse(permco!=old_permco,permco,NA)
  ][,mindate:=min(nameendt),by=c("old_permco","permco")
  ][,list(old_permco,permco_new,mindate,cusip_id)
  ][!is.na(permco_new)])
  
  table_permco1=merge(table_permco1,
                      table_permco[,list(old_permco,tsymbol,comnam,permco,namedt,nameendt,last_link_pre)],
                      by.x=c("permco_new"),
                      by.y=c("old_permco"),allow.cartesian = T)
  
  table_permco1=table_permco1[mindate<=namedt
  ][,-c("mindate","permco_new")][!is.na(last_link_pre)]
  
  table_permco=rbind(table_permco,table_permco1,fill=T)
  table_permco=unique(table_permco)
}

fisd4=table_permco
fisd4=fisd4[!is.na(cusip_id)]

# merge back on with symbols
fisd4=fisd4[,-c("offering_date","company_symbol","max_trace_date","min_trace_date")]
fisd4=merge(fisd4,table_symbol,by=c("cusip_id","permco"),allow.cartesian=T)

# unique obs
fisd4=unique(fisd4)

# check the symbol time
fisd4=fisd4[,symbol_time:=(max_trace_date>=namedt&nameendt>min_trace_date)]
fisd4[,matched_by_symbol:=tsymbol==company_symbol]
# Create time difference
fisd4[,trace_time_diff:=max_trace_date-min_trace_date]
# get sum of the dates
fisd4[symbol_time!=T|matched_by_symbol!=T,trace_time_diff:=0][,
                                                              sum_trace_time_diff:=sum(trace_time_diff,na.rm=T),by=c("old_permco","cusip_id")]
# get filter dummy
fisd4[,max_trace_time_diff:=sum_trace_time_diff==max(sum_trace_time_diff,na.rm=T),by="cusip_id"]
fisd4=fisd4[max_trace_time_diff==T]

# 
# # check if multiple permco per cusip
# # create dates 
# fisd4=fisd4[,":="(date_st=min(namedt),
#             date_end=max(nameendt)),by=c("cusip_id","permco")][,list(cusip_id,permco,date_st,date_end)]
# fisd4=unique(fisd4)
# 


## check if multiple permco per cusip

# create dates 
date.eom.bd <- unique(preceding(seq(as.Date(min(fisd4$offering_date,na.rm=T)),  as.Date(today()), by="1 day") - 1, "Rmetrics/NYSE"))
complete_cusip <- data.table(expand.grid(date=date.eom.bd,cusip_id=unique(fisd4$cusip_id)))

complete_cusip[,date1:=date]

# remove the case when namedt>nameendt -- happens when there is a merger and name change of the acquiring company the same day;
# there is an observation next day
fisd4=fisd4[nameendt>=namedt]

# remove if after today
fisd4=fisd4[namedt< as.Date(today())]

fisd4=complete_cusip[fisd4,on=.(cusip_id,
                                date1>=namedt,
                                date1<=nameendt)]

rm(complete_cusip)


# number of rows for cusip-date
fisd4[,flag:=.N,by=c("cusip_id","date")]
table(fisd4$flag)


# check whether any of the permco had at certain point the same trading symbol
fisd4=unique(fisd4)

#number of permnos for those with several observations during the date
fisd4[flag>1,number_permco:=length(unique(permco)),by=c("cusip_id","date")]
table(fisd4$number_permco)

# remove ones with multiple matches (6 cases)
fisd4=fisd4[number_permco==1|is.na(number_permco)]

# keep if one observation for the date, or if one unique permco, or if matched by symbol
fisd4[flag==1|number_permco==1,link_type:="C"]

# in this case S and N are wrong matches hence we remove them
fisd4=fisd4[link_type%in%c("C","P")]

fisd4=fisd4[,-c("min_trace_date","max_trace_date")]

# get trace dates
trace2=unique(copy(trace)[,min_trace_date:=min(min_trace_date,na.rm=T),by=list(cusip_id)
][,max_trace_date:=max(max_trace_date,na.rm=T),by=list(cusip_id)
][,list(min_trace_date,max_trace_date,cusip_id,maturity)])

fisd4=merge(fisd4,trace2,by="cusip_id")

# filter dates
fisd4=fisd4[date>=offering_date&date<=maturity]

# set order dates
setorder(fisd4,cusip_id,permco,date)

# create a counter id based on cusip_id, permco and number_permco
fisd4$counter <- with(fisd4, rleid(cusip_id, permco,link_type))
# unify dates
fisd4[,":="(date1=min(date1,na.rm=T),
            date1.1=max(date1.1,na.rm=T)),by=list(cusip_id,permco,counter)]
# get unique obs
fisd4=unique(fisd4[,list(date1,date1.1,cusip_id,permco,link_type)])
names(fisd4)[1:2]=c("date_st","date_end")
save(fisd4,file="Data/temp/lt_symbol2.rdata")

#
## match by issuer name ----
#

fisd5=fisd[!cusip_id%in%unique(fisd2$cusip_id)][!cusip_id%in%unique(fisd1$cusip_id)
][!cusip_id%in%unique(fisd3$cusip_id)][!cusip_id%in%unique(fisd4$cusip_id)]
uniqueN(fisd5$cusip_id)
# merge on cusip and check the symbol
fisd5=unique(fisd5[,list(cusip_name,cusip_id,company_symbol,max_trace_date,min_trace_date,offering_date)])

require(stringdist)
fisd5$cusip_name=gsub(" ","",fisd5$cusip_name)
CRSP_names$comnam=gsub(" ","",CRSP_names$comnam)

y=unique(CRSP_names$comnam)
x=unique(fisd5$cusip_name)
dist=stringdistmatrix(x,y, method = "jaccard",q=4)
rownames(dist)=x
colnames(dist)=y

# transform matrix in list of couples
dist=data.table(
  cusip_name = rep(rownames(dist), ncol(dist)),
  comnam =  rep(colnames(dist), each = nrow(dist)),
  dist = c(dist))
# retain only if measure <0.2
dist=dist[dist<=0.2]

fisd5=merge(fisd5,dist,by="cusip_name")
fisd5=merge(fisd5,CRSP_names,by="comnam")

uniqueN(fisd5$cusip_id)

# get min and max dates for each permco and filter on valid name in between
fisd5=fisd5[,":="(namedt=min(namedt,na.rm=T),
                  nameendt=max(nameendt,na.rm=T)),
            by=list(cusip_id,permco)
][offering_date>=namedt&
    nameendt>offering_date]

# get trace dates
trace2=unique(copy(trace)[,min_trace_date:=min(min_trace_date,na.rm=T),by=list(cusip_id)
][,max_trace_date:=max(max_trace_date,na.rm=T),by=list(cusip_id)
][,list(min_trace_date,max_trace_date,cusip_id,maturity)])
fisd5=merge(fisd5,trace2,by="cusip_id")

# filter dates
fisd5=unique(fisd5[namedt<=maturity][,list(cusip_id,permco,namedt,nameendt)])
names(fisd5)[3:4]=c("date_st","date_end")
# add link type
fisd5[,link_type:="C"]

save(fisd5,file="Data/temp/lt_name.rdata")

#
## match by parent name ----
#

fisd6=fisd[!cusip_id%in%unique(fisd2$cusip_id)][!cusip_id%in%unique(fisd1$cusip_id)][!cusip_id%in%unique(fisd3$cusip_id)][!cusip_id%in%unique(fisd4$cusip_id)
][!cusip_id%in%unique(fisd5$cusip_id)]
fisd6=fisd6[,min_trace_date:=as.Date(as.character(min_trace_date))][!is.na(min_trace_date)]
uniqueN(fisd6$cusip_id)
# merge on cusip and check the symbol
fisd6=unique(fisd6[,list(parent_name,issuer_cusip,cusip_id,company_symbol,max_trace_date,min_trace_date,offering_date)])


require(stringdist)
fisd6$parent_name=gsub(" ","",fisd6$parent_name)
CRSP_names$comnam=gsub(" ","",CRSP_names$comnam)

y=unique(CRSP_names$comnam)
x=unique(fisd6$parent_name)
dist=stringdistmatrix(x,y, method = "jaccard",q=4)
rownames(dist)=x
colnames(dist)=y

# transform matrix in list of couples
dist=data.table(
  parent_name = rep(rownames(dist), ncol(dist)),
  comnam =  rep(colnames(dist), each = nrow(dist)),
  dist = c(dist))
# retain only if measure <0.2
dist=dist[dist<=0.2]

fisd6=merge(fisd6,dist,by="parent_name")
fisd6=merge(fisd6,CRSP_names,by="comnam")

uniqueN(fisd6$cusip_id)


# get min and max dates for each permco and filter on valid name in between
fisd6=fisd6[,":="(namedt=min(namedt,na.rm=T),
                  nameendt=max(nameendt,na.rm=T)),
            by=list(cusip_id,permco)
][offering_date>=namedt&
    nameendt>offering_date]


# get trace dates
trace2=unique(copy(trace)[,min_trace_date:=min(min_trace_date,na.rm=T),by=list(cusip_id)
][,max_trace_date:=max(max_trace_date,na.rm=T),by=list(cusip_id)
][,list(min_trace_date,max_trace_date,cusip_id,maturity)])

fisd6=merge(fisd6,trace2,by="cusip_id")

# filter dates
fisd6=unique(fisd6[namedt<=maturity][,list(cusip_id,permco,namedt,nameendt)])
names(fisd6)[3:4]=c("date_st","date_end")
# add link type
fisd6[,link_type:="C"]
save(fisd6,file="Data/temp/lt_parent_name.rdata")




#
## create final table ----
#

# create matching id to identify match type:
# I: match by issuer cusip;
# P: match by parent cusip
# ID: match by issuer cusip and symbol;
# SD: match on symbol and date
# IN: match by issuer name
# PN: match by parent name

fisd1$match_type="I"
fisd2$match_type="P"
fisd3$match_type="ID"
fisd4$match_type="SD"
fisd5$match_type="IN"
fisd6$match_type="PN"

linking_table=rbind(fisd1,fisd2,fisd3,fisd4,fisd5,fisd6,fill=T)

save(linking_table,file="Data/Datasets/linking_table.rdata")
table(linking_table$match_type)
