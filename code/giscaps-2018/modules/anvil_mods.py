# anvil_mods.py

import pandas as pd
import numpy as np 
import shapely
import geopandas as gpd
import quandl
from fred import Fred

# demo api key
quandl.ApiConfig.api_key = "HZeuPhaYzsNZGSvqnS76"


def formatIndicatorLikeQuandl(indicator, **kwargs):
    """
    Uses the FRED module to access data not included 
    in QUANDL's dataset. Limits the returned FRED data
    to only date and value fields supplied. 
    
    Accepts a FRED-formatted string for the desired
    economic index (indicator).
    
    Returns the formatted indicator as a pandas
    DataFrame for downstream processing, or 
    an error message.
    """
    try:
        # set fred instance: demo API key
        fr = Fred(api_key='0242102d8f6da35b30c2c9c28d305046',response_type='df')

        # get the index and limit to start_date=start_date, end_date=end_date
        indicator = fr.series.observations(indicator).loc[:, ('date', 'value')]
        # drop nans
        indicator.dropna(inplace=True)
        # convert str date to datetime
        indicator['date'] = pd.to_datetime(indicator['date'])
        # check if start and end dates are present
        if kwargs:
            # create date mask for only dates within period of dataset
            date_mask = (indicator['date'] >= kwargs['start_date']) & (indicator['date'] <= kwargs['end_date'])
            # filter
            indicator = indicator[date_mask]
        # set the index to the date for index processing downstream
        indicator.set_index('date', drop=True, inplace=True)
        # rename the year col for consistency
        indicator.rename({'value':'Value'}, axis=1, inplace=True)

    except Exception as e:
        return e
    
    # return the index
    return indicator

def convertGeoJsonGeometry(data):
    """
    Convert JSON features into shapely 
    geometry and then convert entire json data
    object into geopandas dataframe. 
    
    Accepts a JSON data object.
    
    Returns a geopandas geodataframe or an 
    error.
    """
    try:
        # convert features to shapes so it can be converted to GDF
        for d in data['features']:
            d['geometry'] = shapely.geometry.shape(d['geometry'])
        # covnvert to geopandas    
        geoframe = gpd.GeoDataFrame(pd.io.json.json_normalize(data['features'])) # comes as a geojson feature collection 
        # replace prefix in column names
        geoframe.columns = geoframe.columns.str.replace('properties.', '')
    
    except Exception as e:
        return e
    
    return geoframe

def convertSpecGeoJsonGeometry(data, cols):
    """
    Convert JSON features into shapely 
    geometry and then convert entire json data
    object into geopandas dataframe. 
    
    Accepts a JSON data object as well as a 
    list of columns to create for the dataframe 
    from properties listed in the JSON object.
    
    Returns a geopandas geodataframe or an 
    error.
    """
    try:        
        # extract all data and put into single list
        all_parcels = []
        
        # for each feature in the data
        for feature in data['features']:
            # dict container
            parcel = {}
            # get the keys for the feature set
            keys = feature.keys()
            # loop through the keys
            for key in keys:  
                if key == 'geometry': 
                    # convert features to shapes so it can be converted to GDF
                    parcel[key] = shapely.geometry.shape(feature[key])
                elif key == 'properties': 
                    # for each desired column in the property set
                    for col in cols:
                        # get property name and append to parcel
                        parcel[col] = feature[key][col]
                else: # skip any other keys
                    pass
            # append entire parcel to all_parcels
            all_parcels.append(parcel)
        
        # covnvert to geopandas    
        geoframe = gpd.GeoDataFrame(all_parcels)
    
    except Exception as e:
        return e
    
    return geoframe

def getPeriodicIndexMovement(indicator):
    """
    Get the movement of the index (a nx1 DF) for each 
    year desired.
    
    Accepts a pandas DataFrame, which is an index
    of economic indicators. 
    
    Note that the column values 'Year' and 'Value' are 
    baked into QUANDL data. Will need to check for changes
    in future. A tripwire assert is added in case the change
    occurs.
    
    Returns either a numpy float val or an error message.
    
    """
    try:
        # trip wire in case col values change in QUANDL
        assert 'Value' in indicator.columns, 'getIndexMovement() Value column value has changed. Edit function definition and try again..'
        # set the year of the non res const for grouping
        indicator['Year'] = indicator.index.year
        # group the years and get the sum of the differences for each year
        indicator_mvt = indicator.groupby(['Year'])['Value'].apply(lambda x: x.diff().sum())

    except Exception as e:
        return e
        
    return indicator_mvt

def getAnnualIndexMovement(indicator):
    """
    Get the movement of the index (a nx1 DF) for each year desired
    
    Accepts a pd.DataFrame, which is an index
    of economic indicators. 
    
    Note that the column values 'Year' and 'Value' are 
    baked into QUANDL data. Will need to check for changes
    in future. A tripwire fault is added in case the change
    occurs.
    
    Returns either a tuple of pd.DataFrames or an error message.
    
    """
    try:
        # trip wire in case col values change in QUANDL
        assert 'Value' in indicator.columns, 'getIndexMovement() Value column value has changed. Edit function definition and try again..'
        # group the years and get the sum of the differences for each year
        indicator_mvt = indicator.diff(-1)
        # convert index to only year for .get() lookup
        indicator_mvt.index = indicator_mvt.index.year        

    except Exception as e:
        return e
    
    # return a series 
    return indicator_mvt.squeeze()

def spatialJoinFeatures(parcels, features):
    """
    Spatially join each parcel with the feature dataset
    by intersecting based on geometry.
    
    Parcels is a geopandas dataframe. The columns in this 
    frame should only be [['buff_dist', 'parcel']].
    
    Features is a geopandas dataframe. Contains only 
    geometry and feature names columns.
    
    Returns the spaital join of the two input 
    geopandas dataframes. Resulting frame has 
    4 columns: geometry, feature name, parcel
    name, and index_right. 
    """
    try:
        
        assert isinstance(parcels, gpd.GeoDataFrame), 'spatialJoinAmmenities first argument must be a geodataframe. You passed an %r' % type(parcels)
        assert isinstance(features, gpd.GeoDataFrame), 'spatialJoinAmmenities second argument must be a geodataframe. You passed an %r' % type(features)
        
        # make a container
        parcels_w_features = gpd.GeoDataFrame()
        # chunk the data to make memory usage more efficient
        for chunk in np.array_split(parcels, np.round(parcels.index.size/100)):
            
            increment = 500
            iter1 = 0
            iter2 = increment
            size = chunk.index.size
            
            # convert chunk back to GeoDataFrame for sjoin operation
            chunk = gpd.GeoDataFrame(chunk)
            if 'buff_dist' in chunk.columns: # set the right geometry in case of buffer distance
                chunk = chunk.set_geometry('buff_dist')
            
            # iterate through each chunk
            while iter1 < size:
                # do remaining rows 
                if iter2 > size:
                    temp_df = gpd.tools.sjoin(chunk.iloc[iter1:], features)
                # iterate through sequence iter1:iter2 to use memory more efficiently
                else:
                    temp_df = gpd.tools.sjoin(chunk.iloc[iter1:iter2], features)
                # save memory if empty
                if temp_df.empty:
                    del(temp_df)
                else: # combine parcels_w_features and temp_df
                    parcels_w_features = pd.concat([parcels_w_features, temp_df])
                    # free up memory
                    del(temp_df)

                # increment iterators
                iter1=iter2
                iter2+=increment
                # break loop when finished
                if iter1 > size:
                    break
    
    except Exception as e: 
        return e
    # return the result w/o the index_right column added with concat
    return parcels_w_features.drop('index_right', axis=1)

def getCountForSpatialJoin(search_parcels, record_parcels, search_col1, search_col2):
    """
    Computes the number of times each parcel 
    appears in the results of a spatial join.
    
    Accepts:
        search_parcels, a pd.Series
        record_parcels is a gpd.GeoDataFrame
        search_col1 is the name to match
        search_col2 is the address to match to ensure
        duplicates are removed. 
        
    Returns a pandas Series of parcel counts indexed
    by the parcel.
    """
    
    try: 
        assert isinstance(search_col1, str), 'Param search_col1 should be type str. Got %r instead.' % type(search_col1)
        assert isinstance(search_col2, str), 'Param search_col2 should be type str. Got %r instead.' % type(search_col2)
        
        # temp container
        counts = {}
        # for each parcel
        for parcel in search_parcels.unique():
            # get unique values as pd.Series for feature names and count how many
            # this will bring up non-unique parcels, so you must filter for unique again.
            items = record_parcels[record_parcels['parcel'] == parcel].loc[:,(search_col1, search_col2)].drop_duplicates(search_col2)[search_col1]    
            # count the number of roads in each unique record
            count = 0
            for item in items:
                # count how many items in this parcel in case of semicolon delimiter
                splits = len(item.split(';'))
                # if more than 1, increment count of roads by that number
                if splits > 1:
                    count += splits
                # if 1, increment count of roads by 1
                else:
                    count += 1
            # set this parcel's count entry
            counts[parcel] = count
             
    except Exception as e:
        return e
    # apply count to features nearby col in parcels_w_hist and return series
    return pd.Series(counts)

def getDateIntvlByParcel(parcels):
    """
    Calculate days between sales of each parcel. 
    
    parcels is a pd.DataFrame with two cols: parcels and 
    sale date. 
    
    Returns a pd.Series of pd.timedeltas to be assigned 
    to a new col in DataFrame.
    """
    try:
        
        assert isinstance(parcels, pd.DataFrame), 'getDateIntvlByParcel argument takes a pd.DataFrame. Got %r instead' % type(parcels)
        assert all([i in parcels.columns for i in ['parcel', 'Sale Date']]), 'getDateIntvlByParcel parcels argument must contain parcel and Sale Date columns.'
        
        # make container Series to hold timeintvls. Initialize with NaTs
        timeintvls = pd.Series([pd.NaT for i in parcels.index])
        
        # iterate through each unique parcel number
        for parcel in parcels['parcel'].unique():
            # get all the parcels in parcels_w_hist and sort by date Descending
            search_df = parcels[parcels['parcel'] == parcel]['Sale Date'].sort_values(ascending=False)
            # make sure the parcel has more than one sale to compare
            if search_df.index.size > 1:
                # calculate the time intervals between sales of each parcel
                intervals = search_df - search_df.shift(-1)
                # iterate through each interval and assign the timedelta to parcels_w_hist using index
                for idx, interval in intervals.iteritems():
                    timeintvls.iloc[idx] = interval
                    
    except Exception as e:
        return e
    # return the time intervals as a pd.Series
    return timeintvls

def getRecentIdxMovement(product, source):
    """
    Computes the most recent year's movement in 
    the economic index data provided. 
    
    Accepts two string arguments where product 
    is the unique ID of the data product and 
    source is one of two main data sources, 
    Quandl or FRED. 
    
    Returns a numpy float64 rounded value of the 
    index's change for the current year. Note for 
    early year months (Jan-Mar), this function does 
    not yet account for early months or lack of 
    current year data.
    """
    try: 
        # make sure a string was passed
        assert isinstance(product, str), 'getLatestIdxStats takes a str argument. Got type %r instead.' % type(index)
        assert isinstance(source, str), 'getLatestIdxStats takes a str argument. Got type %r instead.' % type(index)
        
        # set the search date range
        today = pd.to_datetime('today')
        past = today - pd.Timedelta('730 days')
   
        # check the source
        if source == 'quandl':
            # special products require more formatting
            if product == 'FMAC/FIX30YR': 
                index = quandl.get(product, start_date=past, end_date=today).loc[:,'US Interest Rate']
            # special products require more formatting
            elif product == 'FMAC/HPI': 
                index = quandl.get(product, start_date=past, end_date=today).loc[:,'United States seasonaly adjusted']
            # get the raw data 
            else:
                index = quandl.get(product, start_date=past, end_date=today)
        elif source == 'fred':
            index = formatIndicatorLikeQuandl(product, **{'start_date':past, 'end_date':today})
        else:
            # if incorrect source, send a msg back to caller
            raise ValueError('Got an unrecgonized source argument: %r. Should be either quandl or fred.' % source)
        
        # check for empty frame...index no longer tracked or no longer available
        if index.empty:
            latest = 0.0
        # format the raw data, sort descending, and select the first element
        else:
            # special products require more formatting
            if product == 'FMAC/FIX30YR':
                index = pd.DataFrame(index).rename({'US Interest Rate':'Value'}, axis=1)
                latest = getPeriodicIndexMovement(index).sort_index(ascending=False).iloc[0]
            elif product == 'FMAC/HPI': 
                index = pd.DataFrame(index).rename({'United States seasonaly adjusted':'Value'}, axis=1)
                latest = getPeriodicIndexMovement(index).sort_index(ascending=False).iloc[0]
            else: 
                latest = getPeriodicIndexMovement(index).sort_index(ascending=False).iloc[0]
        
    except Exception as e:
        print('An error occurred. Did you pass the correct product and source info?')
        return e
    
    # return a rounded figure
    return np.round(latest, 3)
