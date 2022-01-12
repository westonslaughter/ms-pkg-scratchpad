# macrosheds csv input formatter function

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

#### Konza Stream Chemistry Testing ####

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

# should we incorporate this permanently into the f(x)?
d <- read.csv(rawfile1,
              colClasses = 'character',
              quote = '')

item_replace <- function(df,
                         rpl_cols,
                         replace_dict,   
                         ignore_na = TRUE,
                         na_replacer = FALSE
                         ) {
    # loop through every time value
    for (col in rpl_cols) {
        for(i in 1:nrow(df[col])) {
            # if it is NA
            if (is.na(df[col][i, ])) {
                # pass if ignore_na is TRUE
                if (ignore_na) {
                    next()
                    # replace with default time if FALSE
                } else {
                    df[col][i, ] <- na_replacer
                }
                # if it is equal to replace_value
            } else if (df[col][i, ] %in% names(replace_dict)) {
                # replace with the default
                df[col][i, ] <- replace_dict[df[col][i, ]]
                # if the default time is midnight
            }
        }
    }
    return(df)
}

# dt_dict: dictionary of form: c('datetime_column_name' = 'datetime_type')  'datetime_type' options: 'time' 'day' 'month' 'year'    
# ignore_dt-na: allows you to skip NAs, # FALSE reassigns to default_dt
numbers_only <- function(x) !grepl("\\D", x)

datetime_clean <- function(df,
                           dt_dict, 
                           ignore_dt_na = TRUE, 
                           dt_defaults_dict = c('time' = 1200, 'day' = 00, 'month' = 00, 'year' = 2000)
                           ) {
    # loop through every provided datetime_type
    for (time_col in names(dt_dict)) {
        # loop through every time value
        for(i in 1:nrow(df[time_col])) {
            if (dt_dict[time_col] == 'time') {
                # skip/change NAs
                if(is.na(df[time_col][i, ])) {
                    if(ignore_dt_na) {
                        invisible()
                    } else {
                        df[time_col][i, ] <- dt_defaults_dict['time']                        
                    }
                # only perform time ops on numerics
                } else if(numbers_only(df[time_col][i, ])){
                    # make sure that all entries have 4 digits
                    if(nchar(df[time_col][i, ]) == 1) {
                        df[time_col][i, ] <- paste0(000, df[time_col][i, ])
                    } else if(nchar(df[time_col][i, ]) == 2) {
                        df[time_col][i, ] <- paste0(00, df[time_col][i, ])
                    } else if(nchar(df[time_col][i, ]) == 3) {
                        df[time_col][i, ] <- paste0(0, df[time_col][i, ])
                    } else if(nchar(df[time_col][i, ]) == 4) {
                        df[time_col][i, ] <- as.character(df[time_col][i, ])
                    } else if(nchar(df[time_col][i, ]) > 4) {
                        print("ERROR: greater than 4 character entry in the time column")
                    }
                } else {
                    invisible()
                }
            } else if (dt_dict[time_col] == 'day'| dt_dict[time_col] == 'month') {
                # skip/change NAs
                if(is.na(df[time_col][i, ])) {
                    if(ignore_dt_na) {
                        invisible()
                    } else {
                        if(dt_dict[time_col] == 'day'){
                            df[time_col][i, ] <- dt_defaults_dict['day']  
                        } else {
                            df[time_col][i, ] <- dt_defaults_dict['month']  
                        }
                    }
                    # only perform time ops on numerics
                } else if(numbers_only(df[time_col][i, ])){
                    if(nchar(df[time_col][i, ]) == 1) {
                        df[time_col][i, ] <- paste0(0, df[time_col][i, ])
                    } else if(nchar(df[time_col][i, ]) == 2) {
                        df[time_col][i, ] <- as.character(df[time_col][i, ])
                    } else if(nchar(df[time_col][i, ]) > 2) {
                        print("ERROR: greater than 2 character entry in the month column")
                    }
                } else {
                    invisible()
                }
            } else if (dt_dict[time_col] == 'year' ) {
                # skip/change NAs
                if(is.na(df[time_col][i, ])) {
                    if(ignore_dt_na) {
                        invisible()
                    } else {
                        df[time_col][i, ] <- dt_defaults_dict['day']  
                    }
                    # only perform time ops on numerics
                } else if(numbers_only(df[time_col][i, ])){
                    if(nchar(df[time_col][i, ]) == 1) {
                        df[time_col][i, ] <- paste0(200, df[time_col][i, ])
                    } else if(nchar(df[time_col][i, ]) == 2) {
                        df[time_col][i, ] <- paste0(20, df[time_col][i, ])
                    } else if(nchar(df[time_col][i, ]) == 3) {
                        df[time_col][i, ] <- paste0(2, df[time_col][i, ])
                    } else if(nchar(df[time_col][i, ]) == 4) {
                        df[time_col][i, ] <- as.character(df[time_col][i, ])
                    } else if(nchar(df[time_col][i, ]) > 4) {
                        print("ERROR: greater than 4 character entry in the year column")
                    }
                } else {
                    invisible()
                }
            }
        }   
    }
    return(df)
}

here <- item_replace(d, 'RecTime', c("." = NA))
here <- datetime_clean(here, c('RecTime' = 'time'), ignore_dt_na = FALSE)
# here <- datetime_clean(d, 'RecDay', dt_type = 'day')

# 
# d <- d %>%
#     as_tibble() %>%
#     mutate(RecTime = ifelse(RecTime == '.', 1200, RecTime)) %>%
#     mutate(num_t = nchar(RecTime)) %>%
#     mutate(num_d = nchar(RecDay)) %>%
#     mutate(time = case_when(num_t == 1 ~ paste0('010', RecTime),
#                             num_t == 2 ~ paste0('01', RecTime),
#                             num_t == 3 ~ paste0('0', RecTime),
#                             num_t == 4 ~ as.character(RecTime),
#                             is.na(num_t) ~ '1200')) %>%
#     mutate(day = ifelse(num_d == 1, paste0('0', as.character(RecDay)), as.character(RecDay))) %>%
#     select(-num_t, -RecTime, -num_d, -RecDay)

# d_test_datetime_cols <- list('RecYear' = '%Y',
#                            'RecMonth' = '%m',
#                            'day' = '%d',
#                            'time' = '%H%M')
# nchar_tester(d, d_test_datetime_cols)

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


ms_read_raw_csv <- function(filepath,
                            preprocessed_tibble,
                            datetime_cols,
                            datetime_tz,
                            optionalize_nontoken_characters = ':',
                            site_code_col,
                            alt_site_code,
                            data_cols,
                            data_col_pattern,
                            alt_datacol_pattern,
                            is_sensor,
                            set_to_NA,
                            replacer,
                            var_flagcol_pattern,
                            alt_varflagcol_pattern,
                            summary_flagcols,
                            # a vector of column names in which
                            # to look for replacement values
                            # as dictated by replacement_dicts
                            rpl_cols = c(),
                            # a dictionary of key-value pairs
                            # of type, 'old.str':'new.str'
                            # used in conjunction with rpl_cols
                            # to allow user to clean out unwanted values
                            replace_dict = c(),
                            # dictate whether replacer function
                            # glides over NAs, or, user can elect to
                            # change to FALSE, and
                            ignore_na = TRUE,
                            # provide a na_replacer value- and the function
                            # will replace all NAs with this value
                            na_replacer = FALSE,
                            sampling_type = NULL){
    
    #checks
    filepath_supplied <-  ! missing(filepath) && ! is.null(filepath)
    tibble_supplied <-  ! missing(preprocessed_tibble) && ! is.null(preprocessed_tibble)
    
    if(filepath_supplied && tibble_supplied){
        stop(glue('Only one of filepath and preprocessed_tibble can be supplied. ',
                  'preprocessed_tibble is for rare circumstances only.'))
    }
    
    
    if(! datetime_tz %in% OlsonNames()){
        stop('datetime_tz must be included in OlsonNames()')
    }
    
    if(length(data_cols) == 1 &&
       ! (missing(var_flagcol_pattern) || is.null(var_flagcol_pattern))){
        stop(paste0('Only one data column. Use summary_flagcols instead ',
                    'of var_flagcol_pattern.'))
    }
    
    if(any(! is.logical(is_sensor))){
        stop('all values in is_sensor must be logical.')
    }
    
    svh_names <- names(is_sensor)
    if(
        length(is_sensor) != 1 &&
        (is.null(svh_names) || any(is.na(svh_names)))
    ){
        stop('if is_sensor is not length 1, all elements must be named.')
    }
    
    if(! all(data_cols %in% ms_vars$variable_code)) {
        
        for(i in 1:length(data_cols)) {
            if(!data_cols[i] %in% ms_vars$variable_code) {
                logerror(msg = paste(unname(data_cols[i]), 'is not in varibles.csv; add'),
                         logger = logger_module)
            }
        }
    }
    
    if(! is.null(sampling_type)){
        if(! length(sampling_type) == 1){
            stop('sampling_type must be a length of 1')
        }
        if(! sampling_type %in% c('G', 'I')){
            stop('sampling_type must be either I or G')
        }
    }
       
    #parse args; deal with missing args
    datetime_colnames <- names(datetime_cols)
    datetime_formats <- unname(datetime_cols)
    
    alt_datacols <- var_flagcols <- alt_varflagcols <- NA
    alt_datacol_names <- var_flagcol_names <- alt_varflagcol_names <- NA
    if(missing(summary_flagcols)){
        summary_flagcols <- NULL
    }
    
    if(missing(set_to_NA)) {
        set_to_NA <- NULL
    }
    
    if(missing(alt_site_code)) {
        alt_site_code <- NULL
    }
    
    #fill in missing names in data_cols (for columns that are already
    #   canonically named)
    datacol_names0 <- names(data_cols)
    if(is.null(datacol_names0)) datacol_names0 <- rep('', length(data_cols))
    datacol_names0[datacol_names0 == ''] <-
        unname(data_cols[datacol_names0 == ''])
    
    #expand data columnname wildcards and rename data_cols
    datacol_names <- gsub_v(pattern = '#V#',
                            replacement_vec = datacol_names0,
                            x = data_col_pattern)
    names(data_cols) <- datacol_names
    
    #expand alternative data columnname wildcards and populate alt_datacols
    if(! missing(alt_datacol_pattern) && ! is.null(alt_datacol_pattern)){
        alt_datacols <- data_cols
        alt_datacol_names <- gsub_v(pattern = '#V#',
                                    replacement_vec = datacol_names0,
                                    x = alt_datacol_pattern)
        names(alt_datacols) <- alt_datacol_names
    }
    
    #expand varflag columnname wildcards and populate var_flagcols
    if(! missing(var_flagcol_pattern) && ! is.null(var_flagcol_pattern)){
        var_flagcols <- data_cols
        var_flagcol_names <- gsub_v(pattern = '#V#',
                                    replacement_vec = datacol_names0,
                                    x = var_flagcol_pattern)
        names(var_flagcols) <- var_flagcol_names
    }
    
    #expand alt varflag columnname wildcards and populate alt_varflagcols
    if(! missing(alt_varflagcol_pattern) && ! is.null(alt_varflagcol_pattern)){
        alt_varflagcols <- data_cols
        alt_varflagcol_names <- gsub_v(pattern = '#V#',
                                       replacement_vec = datacol_names0,
                                       x = alt_varflagcol_pattern)
        names(alt_varflagcols) <- alt_varflagcol_names
    }
    
    #combine all available column name mappings; assemble new name vector
    colnames_all <- c(data_cols, alt_datacols, var_flagcols, alt_varflagcols)
    na_inds <- is.na(colnames_all)
    colnames_all <- colnames_all[! na_inds]
    
    suffixes <- rep(c('__|dat', '__|dat', '__|flg', '__|flg'),
                    times = c(length(data_cols),
                              length(alt_datacols),
                              length(var_flagcols),
                              length(alt_varflagcols)))
    
    suffixes <- suffixes[! na_inds]
    colnames_new <- paste0(colnames_all, suffixes)
    
    colnames_all <- c(datetime_colnames, colnames_all)
    names(colnames_all)[1:length(datetime_cols)] <- datetime_colnames
    colnames_new <- c(datetime_colnames, colnames_new)
    
    if(! missing(site_code_col) && ! is.null(site_code_col)){
        colnames_all <- c('site_code', colnames_all)
        names(colnames_all)[1] <- site_code_col
        colnames_new <- c('site_code', colnames_new)
    }
    
    if(! is.null(summary_flagcols)){
        nsumcol <- length(summary_flagcols)
        summary_flagcols_named <- summary_flagcols
        names(summary_flagcols_named) <- summary_flagcols
        colnames_all <- c(colnames_all, summary_flagcols_named)
        colnames_new <- c(colnames_new, summary_flagcols)
    }
    
    #assemble colClasses argument to read.csv
    classes_d1 <- rep('numeric', length(data_cols))
    names(classes_d1) <- datacol_names
    
    classes_d2 <- rep('numeric', length(alt_datacols))
    names(classes_d2) <- alt_datacol_names
    
    classes_f1 <- rep('character', length(var_flagcols))
    names(classes_f1) <- var_flagcol_names
    
    classes_f2 <- rep('character', length(alt_varflagcols))
    names(classes_f2) <- alt_varflagcol_names
    
    classes_f3 <- rep('character', length(summary_flagcols))
    names(classes_f3) <- summary_flagcols
    
    class_dt <- rep('character', length(datetime_cols))
    names(class_dt) <- datetime_colnames
    
    if(! missing(site_code_col) && ! is.null(site_code_col)){
        class_sn <- 'character'
        names(class_sn) <- site_code_col
    }
    
    classes_all <- c(class_dt, class_sn, classes_d1, classes_d2, classes_f1,
                     classes_f2, classes_f3)
    classes_all <- classes_all[! is.na(names(classes_all))]
    
    if(filepath_supplied){
        d <- read.csv(filepath,
                      stringsAsFactors = FALSE,
                      colClasses = "character")
    } else {
        d <- mutate(preprocessed_tibble,
                    across(everything(), as.character))
    }
    
    d <- item_replace(d, rpl_cols, replace_dict, ignore_na, na_replacer)
    
    d <- d %>%
        as_tibble() %>%
        select(one_of(c(names(colnames_all), 'NA.'))) #for NA as in sodium
    if('NA.' %in% colnames(d)) class(d$NA.) = 'character'
    
    # Remove any variable flags created by pattern but do not exist in data
    # colnames_all <- colnames_all[names(colnames_all) %in% names(d)]
    # classes_all <- classes_all[names(classes_all) %in% names(d)]


    # Set values to NA if used as a flag or missing data indication
    # Not sure why %in% does not work, seem to only operate on one row
    if(! is.null(set_to_NA)){
        for(i in 1:length(set_to_NA)){
            d[d == set_to_NA[i]] <- NA
        }
    }
    
    #Set correct class for each column
    colnames_d <- colnames(d)
    
    for(i in 1:ncol(d)){
        
        if(colnames_d[i] == 'NA.'){
            class(d[[i]]) <- 'numeric'
            next
        }
        
        class(d[[i]]) <- classes_all[names(classes_all) == colnames_d[i]]
    }
    # d[] <- sw(Map(`class<-`, d, classes_all)) #sometimes classes_all is too long, which makes this fail
    
    #rename cols to canonical names
    for(i in 1:ncol(d)){
        
        if(colnames_d[i] == 'NA.'){
            colnames_d[i] <- 'Na__|dat'
            next
        }
        
        canonical_name_ind <- names(colnames_all) == colnames_d[i]
        if(any(canonical_name_ind)){
            colnames_d[i] <- colnames_new[canonical_name_ind]
        }
    }
    
    colnames(d) <- colnames_d
    
    #resolve datetime structure into POSIXct
    d  <- resolve_datetime(d = d,
                           datetime_colnames = datetime_colnames,
                           datetime_formats = datetime_formats,
                           datetime_tz = datetime_tz,
                           optional = optionalize_nontoken_characters)
    
    # datetime character enforcement 
    
    #remove rows with NA in datetime or site_code
    d <- filter(d,
                across(any_of(c('datetime', 'site_code')),
                       ~ ! is.na(.x)))
    
    #remove all-NA data columns and rows with NA in all data columns.
    #also remove flag columns for all-NA data columns.
    all_na_cols_bool <- apply(select(d, ends_with('__|dat')),
                              MARGIN = 2,
                              function(x) all(is.na(x)))
    all_na_cols <- names(all_na_cols_bool[all_na_cols_bool])
    all_na_cols <- c(all_na_cols,
                     sub(pattern = '__\\|dat',
                         replacement = '__|flg',
                         all_na_cols))
    
    d <- d %>%
        select(-one_of(all_na_cols)) %>%
        filter_at(vars(ends_with('__|dat')),
                  any_vars(! is.na(.)))
    
    #for duplicated datetime-site_code pairs, keep the row with the fewest NA
    #   values. We could instead do something more sophisticated.
    d <- d %>%
        rowwise(one_of(c('datetime', 'site_code'))) %>%
        mutate(NAsum = sum(is.na(c_across(ends_with('__|dat'))))) %>%
        ungroup() %>%
        arrange(datetime, site_code, NAsum) %>%
        select(-NAsum) %>%
        distinct(datetime, site_code, .keep_all = TRUE) %>%
        arrange(site_code, datetime)
    
    #convert NaNs to NAs, just in case.
    d[is.na(d)] <- NA
    
    #either assemble or reorder is_sensor to match names in data_cols
    if(length(is_sensor) == 1){
        
        is_sensor <- rep(is_sensor,
                         length(data_cols))
        names(is_sensor) <- unname(data_cols)
        
    } else {
        
        data_col_order <- match(names(is_sensor),
                                names(data_cols))
        is_sensor <- is_sensor[data_col_order]
    }
    
    #fix sites names if multiple names refer to the same site
    if(! is.null(alt_site_code)){
        
        for(z in 1:length(alt_site_code)){
            
            d <- mutate(d,
                        site_code = ifelse(site_code %in% !!alt_site_code[[z]],
                                           !!names(alt_site_code)[z],
                                           site_code))
        }
    }
    
    #prepend two-letter code to each variable representing sample regimen and
    #record sample regimen metadata
    d <- sm(identify_sampling(df = d,
                              is_sensor = is_sensor,
                              domain = domain,
                              network = network,
                              prodname_ms = prodname_ms,
                              sampling_type = sampling_type))
    
    #Check if all sites are in site file
    unq_sites <- unique(d$site_code)
    if(! all(unq_sites %in% site_data$site_code)){
        
        for(i in seq_along(unq_sites)) {
            if(! unq_sites[i] %in% site_data$site_code){
                logwarn(msg = paste(unname(unq_sites[i]),
                                    'is not in site_data file; add?'),
                        logger = logger_module)
            }
        }
    }
    
    return(d)
}

ms_d <- ms_read_raw_csv(preprocessed_tibble = d,
                     datetime_cols = list('RecYear' = '%Y',
                                          'RecMonth' = '%m',
                                          'RecDay' = '%d',
                                          'RecTime' = '%H%M'),
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
                     rpl_cols = c('RecTime'),
                     replace_dict = c("." = 1200),
                     is_sensor = FALSE)

# comment for git test
# HUbbard MSREAD
d <- ms_read_csv(filepath = rawfile,
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

# flags are ONLY a binary for good/bad , miscellaneous notes are not kept
# must inform user that this is the case, THEY need to parse notes
# for good/bad flagging
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




# 
# col_item_replacer <- function(x, replacer) {
#     # x: dataframe
#     # replacer: vector of, c(colname, string, replacement_string)
#     for (i in 1:nrow(x[replacer[1]])) {
#         # pass over NAs
#         if (is.na(x[replacer[1]][i,])) {
#             invisible()
#         } else {
#             # on finding a match to the string, 
#             # replace item with replacement string
#             if((x[replacer[1]][i,] == replacer[2])) {
#                 x[replacer[1]][i,] <- replacer[3]
#             }
#         }
#     } 
#     # return changed DF
#     return(x)
# }
# 
# nchar_enforcer <- function(x, col_name, numchar, new_string, paste_front = TRUE) {
#     for (i in 1:nrow(x[col_name])) {
#         # pass over NAs
#         if (is.na(x[col_name][i,])) {
#             invisible()
#         } else {
#             if (paste_front == TRUE) {
#                 # past new string to front of target
#                 if(nchar(x[col_name][i,]) == numchar) {
#                     print("character pasted to correct nchar")
#                     x[col_name][i,] <- paste0(new_string, x[col_name][i,])
#                 } 
#             } else if (paste_front == FALSE) {
#                 # paste new string to end of target
#                 if(nchar(x[col_name][i,]) == numchar) {
#                     print("character pasted to correct nchar")
#                     x[col_name][i,] <- paste0(x[col_name][i,], new_string)
#                 } 
#             }
#         }
#     }
#     return(x)
# }
# 
# nchar_enforcer <- function(x, col_name, numchar, new_string, paste_front = TRUE) {
#     for (i in 1:nrow(x[col_name])) {
#         # pass over NAs
#         if (is.na(x[col_name][i,])) {
#             invisible()
#         } else {
#             if (paste_front == TRUE) {
#                 # past new string to front of target
#                 if(nchar(x[col_name][i,]) == numchar) {
#                     x[col_name][i,] <- paste0(new_string, x[col_name][i,])
#                 } 
#             } else if (paste_front == FALSE) {
#                 # paste new string to end of target
#                 if(nchar(x[col_name][i,]) == numchar) {
#                     x[col_name][i,] <- paste0(x[col_name][i,], new_string)
#                 } 
#             }
#         }
#     }
#     return(x)
# }
# 
# 
# nchar_tester <- function(x, col_posix) {
#     col_names <- names(col_posix)
#     
#     for (column in col_names) {
#         for (i in 2:nrow(x[column])) {
#             # pass over NAs
#             if (is.na(x[column][i,]) | is.na(x[column][i-1,])) {
#                 invisible()
#             } else if (x[column][i,] == "" | x[column][i-1,] == ""){
#                 invisible()
#             } else {
#                 if(nchar(x[column][i,]) != nchar(x[column][i-1,])) {
#                     cat("WARNING: number of character not uniform in datetime column. column name: ", 
#                         paste0("        ",column),   
#                         "        user is advised to ensure correct formatting of all datetime columns to avoid errors ", 
#                         "", sep="\n")
#                     
#                     # printing specifics
#                     # print(x[column][i,])
#                     # print(x[column][i-1,])
#                     # 
#                     # print(nchar(x[column][i,]))
#                     # print(nchar(x[column][i-1,]))
#                     break
#                 }
#             }
#         }
#     }
# }