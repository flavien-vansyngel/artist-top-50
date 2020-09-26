import pandas as pd
import numpy as np
import os


def create_countries_df(isoFile="isoCodes.csv"):
    """Create a dataframe containing countries information.
    Returns a pandas dataframe with columns Name,Code,weight.
    Column Name contains the name of the country
    Column Code contains the ISO code of the country
    Column weight contains the fraction of the streaming service users
    that are in the country
    """

    ## Read Name and ISO Codes
    ## The option keep_default_na=False prevents from
    ## interpreting NA (Namibie) as NaN
    countries = pd.read_csv(isoFile, keep_default_na=False)

    ## Add user information.
    ## One third of the users are in France,
    ## the rest of the users are equally distributed over all other countries.
    pFR = 1./3
    nCountries = len(countries.index)
    countries['weight']=0
    countries.loc[countries.Code=='FR','weight'] = pFR
    countries.loc[countries.Code!='FR','weight'] = (1 - pFR) / (nCountries - 1)

    return countries


def create_users_df(countries, nUsers=100, nUsersListening=70, lmbd=.03):
    """Create a dataframe containing users information.
    Returns a pandas dataframe with columns user_id,weight,country.
    Column user_id contains the id of the user.
    Column weight contains the fraction of streams associated to the user
    Column country contains the country of the user.
    """

    ## Generate user IDs
    usersId = np.random.permutation(nUsers)

    ## Exponential distribution of streams among users
    usersWeight = np.exp( -lmbd * np.arange(nUsersListening) )
    usersWeight = usersWeight / usersWeight.sum()
    usersWeight = np.concatenate((usersWeight, np.zeros(nUsers-nUsersListening)))

    ## Pick a random country for each user
    usersCountry = np.random.choice(countries.Code, nUsers, p=countries.weight)

    ## Gather information and return.
    users = pd.DataFrame({'user_id': usersId,
                          'weight':  usersWeight,
                          'country': usersCountry})
    return users


def create_songs_df(nSongs=500, nSongsListened=400, lmbd=.1):
    """Create a dataframe containing songs information.
    Returns a pandas dataframe with columns sng_id,weight.
    Column sng_id contains the id of the song.
    Column weight contains the fraction of streams associated to the song.
    """

    ## Generate song IDs
    songsId = np.random.permutation(nSongs)

    ## Exponential distribution of songs among streams
    songsWeight = np.exp( -lmbd * np.arange(nSongsListened) )
    songsWeight = songsWeight / songsWeight.sum()
    songsWeight = np.concatenate((songsWeight, np.zeros(nSongs-nSongsListened)))

    ## Gather information and return.
    songs = pd.DataFrame({'sng_id': songsId,
                          'weight': songsWeight})

    return songs


def write_listen_one_day(users, songs, nStream=1000, outputDir='./', DD='01', MM='01', YYYY='2012'):
    """Write the stream of the listen-YYYYMMDD.log file.
    """

    ## Generate a possible steram set
    ## First pick users according to their weights in the streams,
    ## then pick songs according to their weights in the streams.
    ## (users and songs are uncorrelated)
    listen = users.filter(['user_id','country']) \
                  .sample(nStream, weights=users.weight, replace=True) \
                  .reset_index(drop=True)
    listen['sng_id'] = np.random.choice(songs.sng_id, nStream, p=songs.weight)

    ## Write the file containing the streams of the day.
    filename = f'listen-{YYYY}{MM}{DD}.log'
    listen.to_csv(outputDir+filename, sep='|', header=False, index=False, columns=['sng_id','user_id','country'])


if __name__ == '__main__':

    ## User information.
    ## nUsers: number of users
    ## fractionListening: fraction of users listening to the streaming service
    ## nUsersListening: number of users actually listening
    ## lmbdUsers: lambda parameter for exponential distribution of streams among users
    nUsers = 14000000
    fractionListening = .8
    nUsersListening = int( nUsers * fractionListening )
    lmbdUsers = 6. / nUsersListening

    ## Songs information.
    ## nSongs: number of songs in catalog
    ## fractionListening: fraction of songs listening to the streaming service
    ## nSongsListened: number of songs actually listened to
    ## lmbdSongs: lambda parameter for exponential distribution of songs among streams
    nSongs = 30000000
    fractionListened = .8
    nSongsListened = int( nSongs * fractionListened )
    lmbdSongs = 14. / nSongs

    ## Number of songs listened to
    nStream = 30000000

    ## Read country information
    countries = create_countries_df()

    ## Generate users
    users = create_users_df(countries, nUsers=nUsers, nUsersListening=nUsersListening, lmbd=lmbdUsers)

    ## Generate songs
    songs = create_songs_df(nSongs=nSongs, nSongsListened=nSongsListened, lmbd=lmbdSongs)

    ## Output directory that will contain the dataset
    datasetDir = os.getcwd()+'/dataset/'
    if not os.path.isdir(datasetDir):
        os.mkdir(datasetDir)

    ## Write the listen-YYYYMMDD.log file for eight days
    for i in range(1,9):
        DD=f'{i:02}'
        MM = '09'
        YYYY = '2012'
        write_listen_one_day(users, songs, nStream=nStream, outputDir=datasetDir, DD=DD, MM=MM, YYYY=YYYY)
