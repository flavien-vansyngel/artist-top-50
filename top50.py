import pandas as pd
import os


def make_top(keyList, topType, topNum, DDi):
    """Construct top 50 from the intermediary tops
    """

    print(f'Making top {topNum} of day {i:02} for {topType}')

    ## Parameters used by pandas CSV reader
    colnames = ['sng_id',     'count']
    dtypes   = {'sng_id':int, 'count':int}
    sep      = '|'

    ## Initialize the top50.
    ## Each element will be a row of the (country|user)_top50_YYYYMMDD.txt file
    top50 = []

    ## Make top 50 for each country/user:
    ## Iterate on all the tops constructed on the last seven days
    ## to construct a global top50
    for key in keyList:

        ## Iterate on the last seven days
        first = True
        for j in range(i-6,i+1):

            YYYY = '2012'
            MM   = '09'
            DDj  = f'{j:02}'

            ## Intermediary directory for the specific key
            ## containing {nChunk} tops
            subDir = 'countries' if topType == 'country' else 'users'
            intDirDayKeys = f'{intDir}listen-{YYYY}{MM}{DDj}/{subDir}/'
            directory = f'{intDirDayKeys}{key}/'

            ## directory does not exist if no stream is made by 
            ## the user/country that day
            if os.path.isdir(directory):
                tmp1, tmp2, allFiles = next(os.walk(directory))

                ## Iterate on the chunk tops
                for filename in allFiles:

                    ## Read chunk top
                    top = pd.read_csv(directory+filename,
                                      header = None,
                                      sep    = sep,
                                      names  = colnames,
                                      dtype  = dtypes)

                    ## If first top to be read
                    ## then it is the current top
                    ## else build the new current top from topCurrent and top
                    if first:
                        topCurrent = top.copy()
                        first = False
                    else:
                        ## Outer join between the topCurrent and top,
                        ## NaN values in the joined dataframe are replaced by value 0,
                        ## sum the counts from topCurrent and top
                        topCurrent = topCurrent.merge(top,
                                                      on='sng_id',
                                                      how='outer',
                                                      suffixes=('_l', '_r')).fillna(0)
                        topCurrent['count'] = topCurrent['count_l'] + topCurrent['count_r']
                        topCurrent = topCurrent[['sng_id','count']]

        ## At this point, topCurrent contains the top for the key over the last 7 days
        ## topCurrent does not exist if no stream was made by user/country
        ## If topCurrent exists, append to the top
        if 'topCurrent' in locals():
            ## Sort the top
            topCurrent.sort_values('count', ascending=False, inplace=True)
            topCurrent.reset_index(drop=True, inplace=True)

            ## Make the key row of the (country|user)_top50_YYYYMMDD.txt file and
            tuplesSngCount = list(zip(topCurrent.loc[0:topNum-1, 'sng_id'].to_list(),
                                      topCurrent.loc[0:topNum-1,  'count'].to_list()))
            listSngCount = [ f'{str(sng)}:{str(int(count))}' for (sng, count) in tuplesSngCount]
            keyRow   = f'{key}|' + ','.join(listSngCount)
            top50.append(keyRow)

    ## Write top50 by key to file
    topFilenamePrefix = 'country' if topType == 'country' else 'user'
    with open(f'{outputDir}{topFilenamePrefix}_top50_{YYYY}{MM}{DDi}.txt', 'w') as openedFile:
        openedFile.write('\n'.join(top50))


if __name__ == '__main__':

    topNum = 50

    ## Directory containing the tree structure of the
    ## intermediary files (i.e. top by country and chunk)
    intDir = os.getcwd()+'/dataset/interm/'

    ## Output directory that will will contain the final country_top50_YYYYMMDD.txt
    outputDir = os.getcwd()+'/'
    if not os.path.isdir(outputDir):
        os.mkdir(outputDir)

    ## Read a list of ISO codes for countries
    isoFile="isoCodes.csv"
    countries = pd.read_csv(isoFile, keep_default_na=False)
    countryList = countries['Code'].to_list()

    ## Make a top50 over the last seven days for days 07 and 08
    ## for country
    for i in range(7,9):
        DDi = f'{i:02}'
        make_top(countryList, 'country', topNum, DDi)

##    ## Make a list of users
##    nUsers   = 14000000
##    userList = list(range(nUsers))
##
##    ## Make a top50 over the last seven days for days 07 and 08
##    ## for user_id
##    for i in range(7,9):
##        DDi = f'{i:02}'
##        make_top(userList,    'user_id', topNum, DDi)

