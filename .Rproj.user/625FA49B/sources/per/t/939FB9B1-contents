#### Set up ####
library(tidyverse)
library(glue)
library(lubridate)
library(errors)
library(data.table)
library(feather)

ms_vars <- read_csv('data/variables.csv')
site_data <- read_csv('data/site_data.csv')
source('src/helpers.R')

#### Hubbard Brook Stream Chemistry


network <- 'lter'
domain <- 'hbef'
prodname_ms <- 'stream_chemistry__208'
site_code <- 'sitename_NA'
component <- 'HubbardBrook_weekly_stream_chemistry_1963-2020'

is_spatial <- FALSE

rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                n = network,
                d = domain,
                p = prodname_ms,
                s = site_code,
                c = component)

d <- ms_read_raw_csv(filepath = rawfile,
                     datetime_cols = c(date = '%Y-%m-%d',
                                       timeEST = '%H:%M'),
                     datetime_tz = 'US/Eastern',
                     site_code_col = 'site',
                     alt_site_code = list('w1' = c('1', 'W1'),
                                          'w2' = c('2', 'W2'),
                                          'w3' = c('3', 'W3'),
                                          'w4' = c('4', 'W4'),
                                          'w5' = c('5', 'W5'),
                                          'w6' = c('6', 'W6'),
                                          'w7' = c('7', 'W7'),
                                          'w8' = c('8', 'W8'),
                                          'w9' = c('9', 'W9')),
                     data_cols = c('pH', 'DIC', 'spCond', 'temp',
                                   'ANC960', 'ANCMet', 'Ca', 'Mg', 'K',
                                   'Na', 'TMAl', 'OMAl', 'Al_ICP', 'NH4',
                                   'SO4', 'NO3', 'Cl',
                                   'PO4', 'DOC', 'TDN', 'DON',
                                   'SiO2', 'Mn', 'Fe', 'F',
                                   'cationCharge', 'anionCharge',
                                   'theoryCond', 'ionError', 'ionBalance',
                                   'pHmetrohm'),
                     data_col_pattern = '#V#',
                     is_sensor = FALSE,
                     summary_flagcols = 'fieldCode')

d <- ms_cast_and_reflag(d,
                        summary_flags_clean = list(fieldCode = NA),
                        summary_flags_to_drop = list(fieldCode = '#*#'),
                        varflag_col_pattern = NA)

d <- ms_conversions(d,
                    convert_units_from = c(DIC = 'umol/l'),
                    convert_units_to = c(DIC = 'mg/l'))


d <- carry_uncertainty(d,
                       network = network,
                       domain = domain,
                       prodname_ms = prodname_ms)

d <- synchronize_timestep(d)

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


