import requests as r

raw_cAPIs = r.get('http://api.census.gov/data.json').json()

cAPIs = {entry['identifier']: {key: value for key, value in entry.iteritems() if key != entry['identifier']} for entry in raw_cAPIs}

def _qjson(st):
    return r.get(st+'?f=json')

tiger_url = 'http://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb'
tigers = [x.split('/')[-1] for x in [y['name'] for y in _qjson(tiger_url).json()['services']]]

def available(tiger = True, verbose=False):
    """
    Returns available identifiers for Census Data APIs. 
    NOTE: we do not support the Economic Indicators Time Series API yet.

    Arguments
    ==========
    tiger   : boolean governing whether to provide tiger spatial data api
    verbose : boolean governing whether to provide ID and title
              or just ID
    

    Returns
    ========

    identifiers (if verbose: and dataset names)

    """
    av_apis = [api for api in cAPIs.keys() if 'eits' not in api]
    
    if tiger:
        av_apis.extend(x for x in tigers)
    
    if verbose:
        rdict = dict()
        for name in av_apis:
            if name in cAPIs.keys():
                rdict.update({name:cAPIs[name]['title']})
            else:
                rdict.update({name:'TIGERweb MapServer'})
        return rdict
    else:
        return av_apis

def explain(identifier=None, verbose=False):
    """
    Explains datasets currently available via the census API

    Arguments
    ==========
    identifier : string identifying which dataset in the API to use
    verbose : boolean governing whether to provide full API record
              or just title and description.

    Returns
    ========

    title and description (if verbose: and full API information)
    """
    if identifier is None:
        raise ValueError('No identifier provided. Use available() to discover identifiers')
    elif not verbose:
        if identifier not in cAPIs.keys():
            desc = _qjson(tiger_url + '/' + identifier + '/' + 'MapServer').json()['description']
        else:
            desc = cAPIs[identifier]['description']
        return {identifier: desc}
    else:
        cAPIs[identifier]

