setwd('D:/Users/cnguyen14/Documents/projects/adhoc/covid19')


###### John Hopkins data deprecated starting 2020-03-23 due to new data file format ingested into etl process

# data_covid <- RCurl::getURL("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv") %>% read.csv(text=.)
# data_covid <- read.csv("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv")

data_covid <- read.csv("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv")
data_covid_nyt <- read.csv("https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv")

# data_covid %>% write.csv("./data/JHU_COVID_20200324.csv")



#### old schema
# list_provinces_states <- c("Texas", "California", "New York", "Washington", "Virginia", "Guangdong", "Hong Kong", "France", "United Kingdom", "Hubei" )  #
# list_Country <- c("Spain", "Iran", "Germany", "Japan", "Italy"   )  #


### new schema 3.24.20
list_provinces_states <- c("Texas", "California", "New York", "Washington", "Virginia", 'Illinois', 'Florida', "France", "United Kingdom", "Colorado" )  #, "Guangdong", "Hong Kong", "Hubei", "Quebec"
list_Country <- c("Spain", "Iran", "Germany", "Italy", 'US', 'United Kingdom', 'China', "Canada")  #, 'Russia'   , "Japan", 'Brazil'

list_Country2 <- c('China', "Canada")  #, 'Russia'   , "Japan", 'Brazil'

mydata2 <- data_covid_nyt %>% 
            mutate( Province.State = state
                    , Country.Region = 'US'
                    , date = date %>%  as.Date ) %>% 
            subset(Province.State %in% list_provinces_states) %>%
            group_by(date, Province.State, Country.Region) %>% 
            summarize(cases = sum(cases))



# data_covid %>% subset(Province.State %>% grepl("New York",.)) 
# data_covid %>% subset(Country.Region %>% grepl("Germany",.)) %>% select( c(1,2,10, 40, 60) )
# data_covid %>% subset(Country.Region %>% grepl("US",.) & Province.State == NULL ) %>% select( c(1,2,10, 40, 60) )

{
  
  mydata <- data_covid %>% subset(Province.State %in% list_provinces_states |
                                    (Country.Region %in% list_Country & Province.State %in% '' )
                                  )
  {     #### country aggregate for country city segments from JHU
        mydata3 <- data_covid %>% subset(Country.Region %in% list_Country2) %>% mutate(Province.State='' , n=1) %>% arrange(Province.State, Country.Region)
        
                      mydata3_etl <- data.frame( 
                        mydata3 %>% select(c(Province.State, Country.Region)) %>% unique %>% arrange(Province.State, Country.Region)
                        , mydata3 %>% select(-c(Province.State, Country.Region)) %>% 
                          by(., mydata3 %>% select(c(Province.State, Country.Region)), function(x){apply(x,2,sum) %>% data.frame}) %>% do.call(rbind,.)
                      )
                      
                      names(mydata3_etl) <- names(mydata3)
                      
                      mydata3_etl <- mydata3_etl %>% mutate(Lat = Lat / n, Long = Long / n) %>% select(-n)
  }
  

        mydata <- mydata %>% union_all(mydata3_etl)
  
        date_first <- names(mydata)[5] %>% as.Date(format="X%m.%d.%y")
        date_last  <- names(mydata)[ncol(mydata)] %>% as.Date(format="X%m.%d.%y")
        
        names(mydata)[5:ncol(mydata)] <- (date_first:date_last)
        # names(mydata)
        
        mydata <- mydata %>% #head(1) %>% 
                        reshape2::melt(id.vars=c(1,2,3,4), variable.name='date', value.name='cases') %>% 
          
                        mutate(date = date %>% as.character %>% as.numeric %>% as.Date(origin='1970-01-01') ) %>% 
          
                        union_all(mydata2) %>% 
          
          
                        mutate( id = paste(Country.Region, Province.State ) ) %>%           
          
                        arrange(id, desc(date) ) %>% 
          
                        mutate(
                                  date_lag1 = lead(date, 1)
                                  , cases_lag1 = lead(cases, 1)
                                  , cases_new = abs( cases - cases_lag1)
                                  , cases_ratio_inc = cases/cases_lag1
                                
                              ) 
                        

          mydata <- mydata %>% 
                        left_join(mydata %>% subset(cases==0) %>% group_by(id) %>% summarize( date_zero = max(date))) %>%
                        left_join(mydata %>% group_by(id) %>% summarize( date_min = min(date))) %>% 
                        mutate(date_zero = coalesce(date_zero, date_min-1) ) %>% 
                        mutate(day = as.numeric(date - date_zero)  ) %>% 
                        arrange(id, date) 
            
          ### join future day values
          # mydata <- mydata %>% right_join(
          #                         data.frame(mydata %>% select(id) %>% unique, key = 1 ) %>%  
          #                               full_join( data.frame(day=0:(mydata$day %>% max), key =1) ) %>% 
          #                               select(-key)
          #                       )
        
}



  
# mydata %>% 
#         subset(0 <= day) %>%
#         # subset(day < 30) %>% 
#               ggplot() +
#               geom_point(aes(day, cases, color=id ) ) +
#               facet_wrap(~id, scales = "fixed" ) +
#               scale_y_continuous(labels = scales::comma) +
#               ggtitle('Time Series of Total Cases')
# 
# 
# mydata %>% 
#       subset(0 <= day) %>%
#       # subset(day < 30) %>% 
#               ggplot() +
#               geom_point(aes(date, cases, color=id ) ) +
#               facet_wrap(~id, scales = "free_y" ) +
#               scale_y_continuous(labels = scales::comma) +
#               ggtitle('Time Series of Total Cases')
# 
# 
# mydata %>% 
#       subset(100 <= cases) %>% 
#       subset(0 <= day) %>% 
#       # subset(day < 30) %>%
#       subset(date > date_lag1) %>% 
#       ggplot() +
#       geom_point(aes(day, cases_new, color=id ) ) +
#       facet_wrap(~id, scales = "free_y" ) +
#       scale_y_continuous(labels = scales::comma)  +
#       ggtitle('Time Series of Daily New Cases')
# 
# 
# mydata %>% 
#       subset(100 <= cases) %>% 
#       # subset(10 <= day) %>% 
#       # subset(day < 30) %>%
#       subset(date > date_lag1) %>% 
#       ggplot() +
#       geom_point(aes(date, cases_ratio_inc, color=id ) ) +
#       facet_wrap(~id, scales = "fixed" ) +
#       scale_y_continuous(labels = scales::comma)  +
#       ggtitle('Time Series of Daily New Cases')



mydata %>% 
      subset(0 <= day) %>% 
      # subset(day < 30) %>%
      # subset(100 <= cases) %>% 
      subset(date > date_lag1) %>% 
            ggplot() +
            geom_point(aes(day, cases, color=id ) ) +
            geom_bar(aes(x=day, y=cases_new, fill=id ) , stat="identity") +
            # geom_smooth(aes(day, cases_new, color=id ) ) +
            # geom_point(aes(day, cases_new, color=id ) ) +
  
            # facet_wrap(~id, scales = "fixed" ) +
            facet_wrap(~paste(id, '- first case:', date_zero), scales = "free_y" )  +
            # geom_line(aes(day, cases), data=mydata ) +
  
            scale_y_continuous(labels = scales::comma)  +
            # xlab('day') +
            ggtitle( paste0('Total Cases and Daily New Cases per Region  (aggregated data from John Hopkins and New York Times) - Snapshot ', date_last ))


