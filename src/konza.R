#dia### Set up ####
library(tidyverse)
library(glue)
library(lubridate)
library(errors)
library(data.table)
library(feather)

ms_vars <- read_csv('data/variables.csv')
site_data <- read_csv('data/site_data.csv')
source('src/helpers.R')

#### Knonza Stream Chemistry ####


network <- 'lter'
domain <- 'konza'
prodname_ms <- 'stream_chemistry__50'
site_code <- 'sitename_NA'
component <- 'NWC011'

is_spatial <- FALSE


rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                n = network,
                d = domain,
                p = prodname_ms,
                s = site_code,
                c = component)

d <- read.csv(rawfile1,
              colClasses = 'character',
              quote = '')

d <- d %>%
    as_tibble() %>%
    mutate(RecTime = ifelse(RecTime == '.', 1200, RecTime)) %>%
    mutate(num_t = nchar(RecTime)) %>%
    mutate(num_d = nchar(RecDay)) %>%
    mutate(time = case_when(num_t == 1 ~ paste0('010', RecTime),
                            num_t == 2 ~ paste0('01', RecTime),
                            num_t == 3 ~ paste0('0', RecTime),
                            num_t == 4 ~ as.character(RecTime),
                            is.na(num_t) ~ '1200')) %>%
    mutate(day = ifelse(num_d == 1, paste0('0', as.character(RecDay)), as.character(RecDay))) %>%
    select(-num_t, -RecTime, -num_d, -RecDay)

# trawler for flagging good/bad
TP_codes <- c('TP below Det limit', 'TP<Det limit', 'tp< det limit', 'TP below Det limit',
              'no3 and tp < det limit', 'tp < det limit')
NO3_codes <- c('No3 Below det limit', 'NO3 < det limit', 'NO3<det limit', 'no3 < det limit',
               'no3 below det limit', 'no3 and tp < det limit')
NH4_codes <- c('NH4 <det limit', 'nh4<det limit')

d_comments <- d$COMMENTS

tp <- grepl(paste(TP_codes,collapse="|"), d_comments)

no3 <-  grepl(paste(NO3_codes,collapse="|"), d_comments)

nh4 <-  grepl(paste(NH4_codes,collapse="|"), d_comments)

d <- d %>%
    mutate(TP_code = tp,
           NO3_code = no3,
           NH4_code = nh4,
           SRP_code = FALSE,
           TN_code = FALSE,
           DOC_code = FALSE,
           check = 1
    ) %>%
    filter(WATERSHED %in% c('n04d', 'n02b', 'n20b', 'n01b', 'nfkc', 'hokn',
                            'sfkc', 'tube', 'kzfl', 'shan', 'hikx'))

d <- ms_read_raw_csv(preprocessed_tibble = d,
                     datetime_cols = list('RecYear' = '%Y',
                                          'RecMonth' = '%m',
                                          'day' = '%d',
                                          'time' = '%H%M'),
                     datetime_tz = 'US/Central',
                     site_code_col = 'WATERSHED',
                     alt_site_code = list('N04D' = 'n04d',
                                          'N02B' = 'n02b',
                                          'N20B' = 'n20b',
                                          'N01B' = 'n01b',
                                          'NFKC' = 'nfkc',
                                          'HOKN' = 'hokn',
                                          'SFKC' = 'sfkc',
                                          'TUBE' = 'tube',
                                          'KZFL' = 'kzfl',
                                          'SHAN' = 'shan',
                                          'HIKX' = 'hikx'),
                     data_cols =  c('NO3', 'NH4'='NH4_N', 'TN', 'SRP',
                                    'TP', 'DOC'),
                     data_col_pattern = '#V#',
                     var_flagcol_pattern = '#V#_code',
                     summary_flagcols = 'check',
                     set_to_NA = '.',
                     is_sensor = FALSE)

d <- ms_cast_and_reflag(d,
                        variable_flags_clean = 'FALSE',
                        variable_flags_dirty = 'TRUE',
                        summary_flags_to_drop = list('check' = '2'),
                        summary_flags_clean = list('check' = '1'))
# accepts equivalents, moles, and grams
d <- ms_conversions(d,
                    # convert_molecules = c('NO3', 'SO4', 'PO4', 'SiO2',
                    #                       'NH4', 'NH3'),
                    convert_units_from = c(NO3 = 'ug/l', NH4_N = 'ug/l',
                                           TN = 'ug/l', SRP = 'ug/l',
                                           TP = 'ug/l'),
                    convert_units_to = c(NO3 = 'mg/l', NH4_N = 'mg/l',
                                         TN = 'mg/l', SRP = 'mg/l',
                                         TP = 'mg/l'))
# ^^ cart-before-the-horse conversion- sig figs first, math later?
d <- carry_uncertainty(d,
                       network = network,
                       domain = domain,
                       prodname_ms = prodname_ms)

# wont interpolate grabs > 2wks apart
d <- synchronize_timestep(d)

# applies carry_uncertainty findings to dataframe
d <- apply_detection_limit_t(d, network, domain, prodname_ms)

sites <- unique(d$site_code)
for(i in 1:length(sites)){
    
    filt_site <- sites[i]
    
    out_comp_filt <- filter(d, site_code == !!filt_site)
    
    write_ms_file(d = out_comp_filt,
                  network = network,
                  domain = domain,
                  prodname_ms = prodname_ms,
                  site_code = filt_site,
                  level = 'munged',
                  shapefile = is_spatial,
                  link_to_portal = FALSE)
}




#### Konza Discharge ####
# Konza discharge is divided by site. Every site had an indivdual file as opposed 
# all sites being in the same file like for many stream chemistry products 

#### discharge__7 ####
network <- 'lter'
domain <- 'konza'
prodname_ms <- 'discharge__7'
site_code <- 'sitename_NA'
component <- 'ASD022'

is_spatial <- FALSE

rawfile1 <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                 n = network,
                 d = domain,
                 p = prodname_ms,
                 s = site_code,
                 c = component)

d <- read.csv(rawfile1, colClasses = "character")

d <- d %>%
    mutate(num_d = nchar(RECDAY)) %>%
    mutate(num_m = nchar(RECMONTH)) %>%
    mutate(day = ifelse(num_d == 1, paste0('0', as.character(RECDAY)), as.character(RECDAY))) %>%
    mutate(month = ifelse(num_m == 1, paste0('0', as.character(RECMONTH)), as.character(RECMONTH)))

d <- ms_read_raw_csv(preprocessed_tibble = d,
                     datetime_cols = list('RECYEAR' = '%Y',
                                          'month' = '%m',
                                          'day' = '%d'),
                     datetime_tz = 'US/Central',
                     site_code_col = 'WATERSHED',
                     alt_site_code = list('N04D' = 'n04d'),
                     data_cols =  c('MEANDISCHARGE' = 'discharge'),
                     data_col_pattern = '#V#',
                     summary_flagcols = c('QUAL_FLAG', 'MAINTENANCE_FLAG',
                                          'INCOMPLETE_FLAG'),
                     is_sensor = TRUE,
                     set_to_NA = '.')

d <- ms_cast_and_reflag(d,
                        varflag_col_pattern = NA,
                        summary_flags_to_drop = list('QUAL_FLAG' = '1',
                                                     'MAINTENANCE_FLAG' = '1',
                                                     'INCOMPLETE_FLAG' = '1'),
                        summary_flags_clean = list('QUAL_FLAG' = c('0', NA),
                                                   'MAINTENANCE_FLAG' = c('0', NA),
                                                   'INCOMPLETE_FLAG' = c('0', NA)))

# Convert from cm/s to liters/s
d <- d %>%
    mutate(val = val*1000)

d <- carry_uncertainty(d,
                       network = network,
                       domain = domain,
                       prodname_ms = prodname_ms)

d <- synchronize_timestep(d)

d <- apply_detection_limit_t(d, network, domain, prodname_ms)

site_code_from_file <- unique(d$site_code)

write_ms_file(d = d,
              network = network,
              domain = domain,
              prodname_ms = prodname_ms,
              site_code = site_code_from_file,
              level = 'munged',
              shapefile = is_spatial,
              link_to_portal = FALSE)

#### discharge__8 ####
network <- 'lter'
domain <- 'konza'
prodname_ms <- 'discharge__8'
site_code <- 'sitename_NA'
component <- 'ASD042'


rawfile1 <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                 n = network,
                 d = domain,
                 p = prodname_ms,
                 s = site_code,
                 c = component)

d <- read.csv(rawfile1, colClasses = "character")

d <- d %>%
    mutate(num_d = nchar(RECDAY)) %>%
    mutate(num_m = nchar(RECMONTH)) %>%
    mutate(day = ifelse(num_d == 1, paste0('0', as.character(RECDAY)), as.character(RECDAY))) %>%
    mutate(month = ifelse(num_m == 1, paste0('0', as.character(RECMONTH)), as.character(RECMONTH)))

d <- ms_read_raw_csv(preprocessed_tibble = d,
                     datetime_cols = list('RECYEAR' = '%Y',
                                          'month' = '%m',
                                          'day' = '%d'),
                     datetime_tz = 'US/Central',
                     site_code_col = 'WATERSHED',
                     alt_site_code = list('N20B' = 'n20b'),
                     data_cols =  c('MEANDISCHARGE' = 'discharge'),
                     data_col_pattern = '#V#',
                     summary_flagcols = c('QUAL_FLAG', 'MAINTENANCE_FLAG',
                                          'INCOMPLETE_FLAG'),
                     is_sensor = TRUE,
                     set_to_NA = '.')

d <- ms_cast_and_reflag(d,
                        varflag_col_pattern = NA,
                        summary_flags_to_drop = list('QUAL_FLAG' = '1',
                                                     'MAINTENANCE_FLAG' = '1',
                                                     'INCOMPLETE_FLAG' = '1'),
                        summary_flags_clean = list('QUAL_FLAG' = c('0', NA),
                                                   'MAINTENANCE_FLAG' = c('0', NA),
                                                   'INCOMPLETE_FLAG' = c('0', NA)))

# Convert from cm/s to liters/s
d <- d %>%
    mutate(val = val*1000)

d <- carry_uncertainty(d,
                       network = network,
                       domain = domain,
                       prodname_ms = prodname_ms)

d <- synchronize_timestep(d)

d <- apply_detection_limit_t(d, network, domain, prodname_ms)

site_code_from_file <- unique(d$site_code)

write_ms_file(d = d,
              network = network,
              domain = domain,
              prodname_ms = prodname_ms,
              site_code = site_code_from_file,
              level = 'munged',
              shapefile = is_spatial,
              link_to_portal = FALSE)

#### discharge__9 ####
network <- 'lter'
domain <- 'konza'
prodname_ms <- 'discharge__9'
site_code <- 'sitename_NA'
component <- 'ASD052'


rawfile1 <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                 n = network,
                 d = domain,
                 p = prodname_ms,
                 s = site_code,
                 c = component)

d <- read.csv(rawfile1, colClasses = "character")

d <- d %>%
    mutate(num_d = nchar(RECDAY)) %>%
    mutate(num_m = nchar(RECMONTH)) %>%
    mutate(day = ifelse(num_d == 1, paste0('0', as.character(RECDAY)), as.character(RECDAY))) %>%
    mutate(month = ifelse(num_m == 1, paste0('0', as.character(RECMONTH)), as.character(RECMONTH)))

d <- ms_read_raw_csv(preprocessed_tibble = d,
                     datetime_cols = list('RECYEAR' = '%Y',
                                          'month' = '%m',
                                          'day' = '%d'),
                     datetime_tz = 'US/Central',
                     site_code_col = 'WATERSHED',
                     alt_site_code = list('N20B' = 'n20b'),
                     data_cols =  c('MEANDISCHARGE' = 'discharge'),
                     data_col_pattern = '#V#',
                     summary_flagcols = c('QUAL_FLAG', 'MAINTENANCE_FLAG',
                                          'INCOMPLETE_FLAG'),
                     is_sensor = TRUE,
                     set_to_NA = '.')

d <- ms_cast_and_reflag(d,
                        varflag_col_pattern = NA,
                        summary_flags_to_drop = list('QUAL_FLAG' = '1',
                                                     'MAINTENANCE_FLAG' = '1',
                                                     'INCOMPLETE_FLAG' = '1'),
                        summary_flags_clean = list('QUAL_FLAG' = c('0', NA),
                                                   'MAINTENANCE_FLAG' = c('0', NA),
                                                   'INCOMPLETE_FLAG' = c('0', NA)))

# Convert from cm/s to liters/s
d <- d %>%
    mutate(val = val*1000)

d <- carry_uncertainty(d,
                       network = network,
                       domain = domain,
                       prodname_ms = prodname_ms)

d <- synchronize_timestep(d)

d <- apply_detection_limit_t(d, network, domain, prodname_ms)

site_code_from_file <- unique(d$site_code)

write_ms_file(d = d,
              network = network,
              domain = domain,
              prodname_ms = prodname_ms,
              site_code = site_code_from_file,
              level = 'munged',
              shapefile = is_spatial,
              link_to_portal = FALSE)

#### discharge__10 ####
network <- 'lter'
domain <- 'konza'
prodname_ms <- 'discharge__10'
site_code <- 'sitename_NA'
component <- 'ASD062'


rawfile1 <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                 n = network,
                 d = domain,
                 p = prodname_ms,
                 s = site_code,
                 c = component)

d <- read.csv(rawfile1, colClasses = "character")

d <- d %>%
    mutate(num_d = nchar(RECDAY)) %>%
    mutate(num_m = nchar(RECMONTH)) %>%
    mutate(day = ifelse(num_d == 1, paste0('0', as.character(RECDAY)), as.character(RECDAY))) %>%
    mutate(month = ifelse(num_m == 1, paste0('0', as.character(RECMONTH)), as.character(RECMONTH)))

d <- ms_read_raw_csv(preprocessed_tibble = d,
                     datetime_cols = list('RECYEAR' = '%Y',
                                          'month' = '%m',
                                          'day' = '%d'),
                     datetime_tz = 'US/Central',
                     site_code_col = 'WATERSHED',
                     alt_site_code = list('N20B' = 'n20b'),
                     data_cols =  c('MEANDISCHARGE' = 'discharge'),
                     data_col_pattern = '#V#',
                     summary_flagcols = c('QUAL_FLAG', 'MAINTENANCE_FLAG',
                                          'INCOMPLETE_FLAG'),
                     is_sensor = TRUE,
                     set_to_NA = '.')

d <- ms_cast_and_reflag(d,
                        varflag_col_pattern = NA,
                        summary_flags_to_drop = list('QUAL_FLAG' = '1',
                                                     'MAINTENANCE_FLAG' = '1',
                                                     'INCOMPLETE_FLAG' = '1'),
                        summary_flags_clean = list('QUAL_FLAG' = c('0', NA),
                                                   'MAINTENANCE_FLAG' = c('0', NA),
                                                   'INCOMPLETE_FLAG' = c('0', NA)))

# Convert from cm/s to liters/s
d <- d %>%
    mutate(val = val*1000)

d <- carry_uncertainty(d,
                       network = network,
                       domain = domain,
                       prodname_ms = prodname_ms)

d <- synchronize_timestep(d)

d <- apply_detection_limit_t(d, network, domain, prodname_ms)

site_code_from_file <- unique(d$site_code)

write_ms_file(d = d,
              network = network,
              domain = domain,
              prodname_ms = prodname_ms,
              site_code = site_code_from_file,
              level = 'munged',
              shapefile = is_spatial,
              link_to_portal = FALSE)

#### Combine discharge products ####

network <- 'lter'
domain <- 'konza'
prodname_ms <- 'discharge__ms001'

sw(combine_products(network = network,
                 domain = domain,
                 prodname_ms = prodname_ms,
                 input_prodname_ms = c('discharge__7',
                                       'discharge__9',
                                       'discharge__8',
                                       'discharge__10')))
