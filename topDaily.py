import pandas as pd
import os


def clean_listen(listen):
    """Drop the rows that do not have the expected format.
    """

    ## One list of True/False indices per column
    ind1 = listen.sng_id.astype(str).str.match('^[0-9]+$', na='xxx')
    ind2 = listen.user_id.astype(str).str.match('^[0-9]+$',na='xxx')
    ind3 = listen.country.astype(str).str.match('^[A-Z]{2}$', na='xxx')

    ## Combine the three lists (ind1 AND ind2 AND ind3)
    ## and select the rows that respect the format (i.e. where ind == True)
    ind = ind1.mul(ind2).mul(ind3).to_list()
    listenClean = listen[ind].reset_index(drop=True)

    return listenClean


def make_interm_dir(intDir, YYYY, MM, DD):
    """Make directories to partition the intermediary data
    Creation of one directory per day is created and
    creation of two subdirectories, for the countries and
    for the users.
    """

    intDirDay          = f'{intDir}listen-{YYYY}{MM}{DD}/'
    intDirDayCountries = f'{intDirDay}countries/'
    intDirDayUsers     = f'{intDirDay}users/'
    if not os.path.isdir(intDirDay):
        os.mkdir(intDirDay)
    if not os.path.isdir(intDirDayCountries):
        os.mkdir(intDirDayCountries)
    if not os.path.isdir(intDirDayUsers):
        os.mkdir(intDirDayUsers)

    return intDirDayCountries, intDirDayUsers


def compute_and_write_top(listen, key, intDirDayKeys, filename):
    """Compute top for each country/user from listening data listen
    and write the result to intermediate file.
    key is either 'country' or 'user_id'
    """

    ## Group by key and songs and compute how many times the song
    ## was played in/by the key in the chunk of data
    listenByKey = listen.groupby([key,'sng_id']).size()

    ## List the keys present in the chunk of data
    ## and iterate on the keys
    keyList = list(listenByKey.index.get_level_values(0).unique())
    for key in keyList:

        ## Make dir for each key in the intermediary directory
        intDirDayKeysKey = f'{intDirDayKeys}{key}/'
        if not os.path.isdir(intDirDayKeysKey):
            os.mkdir(intDirDayKeysKey)

        ## Make a top (unsorted) for the key with the chunk of
        ## data (the top contains all the songs) and
        ## write to intermediary file in the intermediary key dir
        topAll = listenByKey[key]
        topAll.to_csv(f'{intDirDayKeysKey}{filename}',
                      header=False, sep='|')


if __name__ == '__main__':

    ## Directory containing the listen-YYYYMMDD.log files
    directory = os.getcwd()+'/dataset/'

    ## Ouput directory that will contain intermediary files
    ## that will be used to compute the final top50 files.
    intDir = directory+'interm/'
    if not os.path.isdir(intDir):
        os.mkdir(intDir)

    ## Parameters used by pandas CSV reader
    ## The listen-YYYYMMDD.log files are read by
    ## chunks of {chunksize} rows
    colnames  = ['sng_id', 'user_id', 'country']
    sep       = '|'
    chunksize = 5000000

    ## Iterate on the 8 days of data, build a top by
    ## country or user and by chunk
    for i in range(1,9):

        ## Construct filename of the day
        DD=f'{i:02}'
        MM = '09'
        YYYY = '2012'
        filename = f'listen-{YYYY}{MM}{DD}.log'

        ## Make directories to partition the intermediary data
        intDirDayCountries, intDirDayUsers = make_interm_dir(intDir, YYYY, MM, DD)

        ## Make an iterator to read the file by chunks
        ## of daframe containing {chunksize} rows.
        ## The option keep_default_na=False prevents from
        ## interpreting NA (Namibie) as NaN
        listens = pd.read_csv(directory+filename,
                              header    = None,
                              sep       = sep,
                              names     = colnames,
                              chunksize = chunksize,
                              keep_default_na=False)

        ## Iterate on the chunks of the file listen-YYYYMMDD.log
        nChunk=0
        for listen in listens:

            print(f'Making top of day {DD}, chunk {nChunk}')

            ## Drop rows that do not respect the expected format
            listen = clean_listen(listen)

            ## Write the tops by country or user and by chunk of data  
            for key in ['country']:
            ##for key in ['country', 'user_id']:
                topFilename = f'top-{YYYY}{MM}{DD}-{key}-{nChunk}.csv'
                intDirDayKeys = intDirDayCountries if key == 'country' else intDirDayUsers
                compute_and_write_top(listen, key, intDirDayKeys, topFilename)

            nChunk += 1
