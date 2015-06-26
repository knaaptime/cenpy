import pandas as pd
import requests as r
import numpy as np
import explorer as exp
import math
from itertools import izip_longest as longzip


class Connection():
    def __init__(self, api_name = None):
        """
        Constructor for a Connection object

        Parameters
        ============
        api_name : shortcode identifying which api to connect to

        Returns
        ========

        a Cenpy Connection object
        """
        if 'eits' not in api_name and api_name != None:
            curr = exp.APIs[api_name]
            self.title = curr['title']
            self.identifier = curr['identifier']
            self.description = curr['description']
            self.contact = curr['mbox']
            self.cxn = unicode(curr['webService'] + u'?')
            self.last_query = ''

            self.__urls__ = {k.strip('c_')[:-4]:v for k,v in curr.iteritems() if k.endswith('Link')}

            if 'documentation' in self.__urls__.keys():
                self.doclink = self.__urls__['documentation']
            if 'variables' in self.__urls__.keys():
                v = pd.DataFrame()
                self.variables = v.from_dict(r.get(self.__urls__['variables']).json().values()[0]).T
            if 'geography' in self.__urls__.keys():
                res = r.get(self.__urls__['geography']).json()
                self.geographies = {k:pd.DataFrame().from_dict(v) for k,v \
                                                        in res.iteritems()}
            if 'tags' in self.__urls__.keys():
                self.tags = r.get(self.__urls__['tags']).json().values()[0]

            if 'examples' in self.__urls__.keys():
                self.example_entries = r.get(self.__urls__['examples']).json()

        else:
            raise ValueError('Pick dataset identifier using the census_pandas.explorer.available() function')

    def __repr__(self):
        return str('Connection to ' + self.title + ' (ID: ' + self.identifier + ')')

    def query(self, cols = [], geo_unit = 'us:00', geo_filter = {}, apikey = None, **kwargs):
        """
        Conduct a query over the USCB api connection

        Parameters
        ===========
        cols : census field identifiers to pull
        geo_unit : dict or string identifying what the basic spatial
                    unit of the query should be
        geo_filter : dict of required geometries above the specified
                      geo_unit needed to complete the query
        apikey : USCB-issued key for your query.
        **kwargs : additional search predicates can be passed here

        Returns
        ========
        pandas dataframe of results 

        Example
        ========
        To grab the total population of all of the census blocks in a part of Arizona:
        
            >>> cxn.query('P0010001', geo_unit = 'block:*', geo_filter = {'state':'04','county':'019','tract':'001802'})

        Notes
        ======

        If your list of columns exceeds the maximum query length of 50,
        the query will be broken up and concatenates back together at 
        the end. Sometimes, the USCB might frown on large-column queries,
        so be careful with this. Cenpy is not liable for your key getting
        banned if you query tens of thousands of columns at once. 

        If your query is rather large, or you're getting errors about your
        search hierarchy not being found or having many matches, specify a
        geoLevelId in the keyword args.
        """

        self.last_query = self.cxn

        geo_unit = geo_unit.replace(' ', '+')
        geo_filter = {k.replace(' ', '+'):v for k,v in geo_filter.iteritems()}
            
        self.last_query += 'get=' + ','.join(col for col in cols)
        
        if isinstance(geo_unit, dict):
            geo_unit = geo_unit.keys()[0].replace(' ', '+') + ':' + str(geo_unit.values()[0])
        else:
            geo_unit = geo_unit.replace(' ', '+')
            
        self.last_query += '&for=' + geo_unit
        
        if len(cols) >= 50:
            return self._bigcolq(cols, geo_unit, geo_filter, apikey, **kwargs)


        if geo_filter != {}:
            self.last_query += '&in='
            for key,value in geo_filter.iteritems():
                self.last_query += key + ':' + value + '+'
            self.last_query = self.last_query[:-1]
        
        for key,val in kwargs.iteritems():
            self.last_query += '&' + key + '=' + val
        
        if apikey is not None:
            self.last_query += '&key=' + apikey
        res = r.get(self.last_query)
        if res.status_code == 204:
            raise r.HTTPError(str(res.status_code) + ' error: no records matched your query')
        try:
            res = res.json()
            return pd.DataFrame().from_records(res[1:], columns=res[0])
        except ValueError:
            if res.status_code == 400:
                raise r.HTTPError(str(res.status_code) + ' ' + [l for l in res.iter_lines()][0])
            else:
                res.raise_for_status()

    def _bigcolq(self, cols=[], geo_unit='us:00', geo_filter={}, apikey=None, **kwargs):
        """
        Helper function to manage large queries

        Parameters
        ===========
        cols : large list of columns to be grabbed in a query
        """
        if len(cols) < 50:
            print 'tiny query!'
            return self.query(cols, geo_unit, geo_filter, apikey, **kwargs)
        else:
            result = pd.DataFrame()
            chunks = np.array_split(cols, math.ceil(len(cols) / 49.))
            for chunk in chunks:
                tdf = self.query(chunk, geo_unit, geo_filter, apikey, **kwargs)
                noreps = [x for x in tdf.columns if x not in result.columns]
                result = pd.concat([result, tdf[noreps]], axis=1)
            return result

    def _biggeomq(self, id_type='fips', verbose='', geoLevelId=None,
                  geo_unit='us:00', geo_filter={}, apikey=None, **kwargs):
        if geoLevelId is None:
            geo_unit_base = geo_unit.split(':')[0]
            if len(verbose) > 2:
                print 'Starting query identification'
            targetfilt = [geo_unit_base == x for x in self.geographies[id_type]['name']]
            lenfilt = [len(geo_filter.keys()) == len(x) for x in self.geographies[id_type]['requires'].fillna([])]
            idfilt = [sum([1 for x, y in geo_filter.keys(), reqs if x == y]) == len(reqs) \
                             for reqs in self.geographies[id_type]['requires'].fillna([])]
            filt = [targ and leng and idx for targ, leng, idx in zip(targetfilt, lenfilt, idfilt)]
            reqs = self.geographies[id_type][filt]
        else:
            reqs = self.geographies[id_type][self.geographies[id_type]['geoLevelId'] == geoLevelId]
        
        if reqs.shape[0] < 1:
            raise KeyError('No geographic hierarchy found matching unit and filter provided')
        elif reqs.shape[0] > 1:
            raise KeyError('Many geographic hierarchies found matching unit and filter provided')
            
        wilds = []
         
        wilds.extend([k for k in reqs['requires'] if '*' == geo_filter[k]])

        if '*' in geo_unit:
        	wilds.append(geo_unit)
        
        sdict= {k:v for k,v in geo_filter.iteritems() if k not in wilds}
        topqueue = self.query(cols=['NAME'], geo_unit=':'.join(top, '*'), geo_filter=sdict)[topqueue].tolist()
        
        while queue:
            curr = queue.pop(0)
            level = self.query(cols=['NAME'], geo_unit=':'.join(top, '*'), geo_filter=sdict)[curr].tolist()
            



        

