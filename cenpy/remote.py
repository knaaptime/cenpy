import pandas as pd
import requests as r
import numpy as np
import explorer as exp
import math
import copy
from six import iteritems as ditems


class APIConnection():
    def __init__(self, api_name=None):
        """
        Constructor for a Connection object

        Parameters
        ============
        api_name : shortcode identifying which api to connect to

        Returns
        ========

        a Cenpy Connection object
        """
        if 'eits' not in api_name and api_name is not None:
            curr = exp.APIs[api_name]
            self.title = curr['title']
            self.identifier = curr['identifier']
            self.description = curr['description']
            self.cxn = unicode(curr['distribution'][0]['accessURL'] + '?')
            self.last_query = ''

            self.__urls__ = {k.strip('c_')[:-4]: v
                             for k, v in ditems(curr) if k.endswith('Link')}

            if 'documentation' in self.__urls__.keys():
                self.doclink = self.__urls__['documentation']
            if 'variables' in self.__urls__.keys():
                v = pd.DataFrame()
                self.variables = v.from_dict(r.get(self.__urls__['variables']).json()['variables']).T
            if 'geography' in self.__urls__.keys():
                res = r.get(self.__urls__['geography']).json()
                self.geographies = {k: pd.DataFrame().from_dict(v)
                                    for k, v in ditems(res)}
            if 'tags' in self.__urls__.keys():
                self.tags = r.get(self.__urls__['tags']).json().values()[0]

            if 'examples' in self.__urls__.keys():
                self.example_entries = r.get(self.__urls__['examples']).json()

        else:
            raise ValueError('Pick dataset identifier using the census_pandas.explorer.available() function')

    def __repr__(self):
        return str('Connection to ' + self.title + ' (ID: ' + self.identifier + ')')

    def query(self, cols=[], geo_unit='us:00', geo_filter={}, apikey='', **kwargs):
        """
        Conduct a query over the USCB api connection

        Parameters
        ===========
        cols        : census field identifiers to pull
        geo_unit    : dict or string identifying what the basic spatial
                    unit of the query should be
        geo_filter  : dict of required geometries above the specified
                      geo_unit needed to complete the query
        apikey      : USCB-issued key for your query.
        **kwargs    : additional search predicates can be passed here:

        **kwargs
        --------
        infer       : bool denoting whether or not to infer datatypes in the query
      
        idtype      : string denoting what ID system (i.e. FIPS or GNIS) to default to (useful in large geometry queries)
        geoLevelID  : string denoting the USCB's summary level to work over (useful in large geometry queries)

        Returns
        ========
        pandas dataframe of results 

        Example
        ========
        To grab the total population of all of the census blocks in 
        a part of Arizona:
            >>> cxn.query('P0010001', geo_unit = 'block:*', geo_filter = {'state':'04','county':'019','tract':'001802'})

        Notes
        ======

        If your list of columns exceeds the maximum query length of 50,
        the query will be broken up and concatenates back together at 
        the end. Sometimes, the USCB might frown on large-column queries,
        so be careful with this. Cenpy is not liable for your key getting
        banned if you query tens of thousands of columns at once. 
        """
        infer = kwargs.pop('infer', True)
        idtype = kwargs.pop('idtype', 'fips')
        geoLevelId = kwargs.pop('geoLevelID', '')  # format of name provided by USCB
        hierarchy = kwargs.pop('hierarchy', None) #for big geometry queries
        position = kwargs.pop('position', 0) #for big geometry queries

        if isinstance(geo_unit, dict):
            geo_unit = geo_unit.keys()[0].replace(' ', '+') + ':' + str(geo_unit.values()[0])
        else:
            geo_unit = geo_unit.replace(' ', '+')

        if '*' in geo_filter.values():
            return self._biggeomq(cols, geo_unit, geo_filter, idtype, geoLevelId, hierarchy, position)

        self.last_query = self.cxn

        geo_filter = {k.replace(' ', '+'): v for k, v in ditems(geo_filter)}

        self.last_query += 'get=' + ','.join(col for col in cols)

        if isinstance(geo_unit, dict):
            geo_unit = geo_unit.keys()[0].replace(' ', '+') + ':' + str(geo_unit.values()[0])
        else:
            geo_unit = geo_unit.replace(' ', '+')

        self.last_query += '&for=' + geo_unit

        if len(cols) >= 50:
            results = self._bigcolq(cols, geo_unit, geo_filter, apikey, **kwargs)

        if geo_filter != {}:
            self.last_query += '&in='
            self.last_query += '+'.join(['{k}:{v}'.format(k=k, v=v)
                                        for k, v in ditems(geo_filter)])
        if kwargs != {}:
            self.last_query += ''.join(['&{k}={v}'.format(k=k, v=v)
                                        for k, v in ditems(kwargs)])
        if apikey != '':
            self.last_query += '&key=' + apikey
        res = r.get(self.last_query)

        if res.status_code == 204:
            raise r.HTTPError(str(res.status_code) + ' error: no records matched your query')
        try:
            res = res.json()
            results = pd.DataFrame().from_records(res[1:], columns=res[0])
        except ValueError:
            if res.status_code == 400:
                raise r.HTTPError(str(res.status_code) + ' ' + [l for l in res.iter_lines()][0])
            else:
                res.raise_for_status()
        
        if infer:
            results[cols] = results[cols].convert_objects(convert_numeric=True)
        return results

    def _bigcolq(self, cols=[], geo_unit='us:00', geo_filter={},
                 apikey=None, **kwargs):
        """
        Helper function to manage large queries

        Parameters
        ===========
        cols : large list of columns to be grabbed in a query
        """
        if len(cols) < 50:
            print('tiny query!')
            return self.query(cols, geo_unit, geo_filter, apikey, **kwargs)
        else:
            result = pd.DataFrame()
            chunks = np.array_split(cols, math.ceil(len(cols) / 49.))
            for chunk in chunks:
                tdf = self.query(chunk, geo_unit, geo_filter, apikey, **kwargs)
                noreps = [x for x in tdf.columns if x not in result.columns]
                result = pd.concat([result, tdf[noreps]], axis=1)
            return result

    def _biggeomq(self, cols, geo_unit, geo_filter, idtype='fips', geoLevelId='', hierarchy=None, position=0):
        
        if hierarchy is None:
            print('finding hierarchy')
            if geoLevelId == '':
                unitmatch = self.geographies[idtype]['name'] == geo_unit.split(':')[0]
                
                filtset = set(level.split(':')[0] for level in geo_filter.keys())
                filtmatch = []
                for req in self.geographies[idtype]['requires']:
                    if not isinstance(req, list):
                        if geo_filter == {}:
                            filtmatch.append(True)
                        else:
                            filtmatch.append(False)
                    else:
                        filtmatch.append(set(req) == set(filtset))

                match = self.geographies[idtype][[u and f for u, f in zip(unitmatch, filtmatch)]]
            else:
                match = self.geographies[idtype][self.geographies['geoLevelId'] == geoLevelId]
            
            if match.empty:
                raise Exception('No geoLevelId matched geo_filter provided.')
            if match.shape[0] > 1:
                raise Exception('No unique geoLevelId found. Please provide geoLevelId.')
            hierarchy = match['requires'].tolist()[0] #since we know it'll have only one element
            p = hierarchy[position]
        else:
            print('inherited hierarchy {}'.format(hierarchy))
            p = hierarchy[position]
        
        #return p, hierarchy, position

        #need to pass a dictionary to correctly filter the layer below. Then, recursively call this function to move
        # further down the tree
        print(p, hierarchy, position)
        above_filter = {p:None}
        queue = self.query(cols=['NAME'], geo_unit=p+':*', infer=False)[p].tolist()
        result = pd.DataFrame()
        for element in self.query(cols=['NAME']):
            above_filter[p] = element
            print(element, queue[0:5])
            print('querying: {u}, {f}'.format(u=geo_unit, f=geo_filter))
            tdf = self.query(cols=cols, geo_unit=geo_unit, geo_filter=geo_filter,
                             hierarchy=hierarchy, position=position+1)
            result.append(tdf)
        return result
