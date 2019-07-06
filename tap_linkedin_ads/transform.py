import re
from datetime import datetime, timedelta
import singer

LOGGER = singer.get_logger()

# Convert camelCase to snake_case
def convert(name):
    regsub = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', regsub).lower()


# Convert keys in json array
def convert_array(arr):
    new_arr = []
    for i in arr:
        if isinstance(i, list):
            new_arr.append(convert_array(i))
        elif isinstance(i, dict):
            new_arr.append(convert_json(i))
        else:
            new_arr.append(i)
    return new_arr


# Convert keys in json
def convert_json(this_json):
    out = {}
    for key in this_json:
        try:
            new_key = convert(key)
        except TypeError:
            LOGGER.error('Error key = {}'.format(key))
        if isinstance(this_json[key], dict):
            out[new_key] = convert_json(this_json[key])
        elif isinstance(this_json[key], list):
            out[new_key] = convert_array(this_json[key])
        else:
            out[new_key] = this_json[key]
    return out


def transform_analytics(data_dict):
    # create pivot id and urn fields from pivot and pivot_value
    if 'pivot' in data_dict and 'pivot_value' in data_dict:
        key = data_dict['pivot'].lower()
        val = data_dict['pivot_value']
        search = re.search('^urn:li:(.*):(.*)$', val)
        if search:
            data_dict['{}'.format(key)] = val
    # Create start_at and end_at fields from nested date_range
    if 'date_range' in data_dict:
        if 'start' in data_dict['date_range']:
            if 'day' in data_dict['date_range']['start'] \
            and 'month' in data_dict['date_range']['start'] \
            and 'year' in data_dict['date_range']['start']:
                year = data_dict['date_range']['start']['year']
                month = data_dict['date_range']['start']['month']
                day = data_dict['date_range']['start']['day']
                start_at = datetime(year=year, month=month, day=day)
                data_dict['start_at'] = start_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        if 'end' in data_dict['date_range']:
            if 'day' in data_dict['date_range']['end'] \
            and 'month' in data_dict['date_range']['end'] \
            and 'year' in data_dict['date_range']['end']:
                year = data_dict['date_range']['end']['year']
                month = data_dict['date_range']['end']['month']
                day = data_dict['date_range']['end']['day']
                end_at = datetime(year=year, month=month, day=day) + timedelta(days=1)
                data_dict['end_at'] = end_at.strftime('%Y-%m-%dT%H:%M:%SZ')
    return data_dict


def transform_campaigns(data_dict):
    if 'targeting' not in data_dict or 'targeting_criteria' not in data_dict:
        return data_dict

    new_dict = data_dict
    # Abstract targeting excludes
    or_dict = data_dict.get('targeting', {}).get('excluded_targeting_facets', {})
    if 'excluded_targeting_facets' in new_dict['targeting']:
        del new_dict['targeting']['excluded_targeting_facets']
    new_dict['targeting']['excluded_targeting_facets'] = []
    ky_cnt = 0
    num = len(or_dict) - 1
    while ky_cnt <= num:
        key = list(or_dict)[ky_cnt]
        append_dict = {'type': key,
                       'values': or_dict[key]}
        new_dict['targeting']['excluded_targeting_facets'].append(append_dict)
        ky_cnt = ky_cnt + 1

    # Abstract targeting includes
    or_dict = data_dict.get('targeting', {}).get('included_targeting_facets', {})
    if 'excluded_targeting_facets' in new_dict['targeting']:
        del new_dict['targeting']['included_targeting_facets']
    new_dict['targeting']['included_targeting_facets'] = []
    ky_cnt = 0
    num = len(or_dict) - 1
    while ky_cnt <= num:
        key = list(or_dict)[ky_cnt]
        append_dict = {'type': key,
                       'values': or_dict[key]}
        new_dict['targeting']['included_targeting_facets'].append(append_dict)
        ky_cnt = ky_cnt + 1

    # Abstract targeting_criteria excludes
    or_dict = data_dict.get('targeting_criteria', {}).get('exclude', {}).get('or', {})
    if 'exclude' in new_dict['targeting_criteria']:
        del new_dict['targeting_criteria']['exclude']
    new_dict['targeting_criteria']['exclude'] = []
    ky_cnt = 0
    num = len(or_dict) - 1
    while ky_cnt <= num:
        key = list(or_dict)[ky_cnt]
        append_dict = {'type': key,
                       'values': or_dict[key]}
        new_dict['targeting_criteria']['exclude'].append(append_dict)
        ky_cnt = ky_cnt + 1

    # Abstract targeting_criteria includes
    and_list = data_dict.get('targeting_criteria', {}).get('include', {}).get('and', {})
    add_cnt = 0
    for and_criteria in and_list:
        or_dict = and_criteria.get('or', {})
        if 'or' in new_dict['targeting_criteria']['include']['and'][add_cnt]:
            del new_dict['targeting_criteria']['include']['and'][add_cnt]['or']
        ky_cnt = 0
        num = len(or_dict) - 1
        while ky_cnt <= num:
            key = list(or_dict)[ky_cnt]
            new_dict['targeting_criteria']['include']['and'][add_cnt]['type'] = key
            new_dict['targeting_criteria']['include']['and'][add_cnt]['values'] = or_dict[key]
            ky_cnt = ky_cnt + 1
        add_cnt = add_cnt + 1

    return new_dict

# Abstract variables to type with key/value pairs
def transform_creatives(data_dict):
    if 'variables' not in data_dict:
        return data_dict

    new_dict = data_dict
    variables = new_dict.get('variables', {}).get('data', {})
    ky_cnt = 0
    num = len(variables) - 1
    while ky_cnt <= num:
        key = list(variables)[ky_cnt]
        params = new_dict.get('variables', {}).get('data', {}).get(key, {})
        new_dict['variables']['type'] = key
        new_dict['variables']['values'] = []

        pk_cnt = 0
        pnum = len(params) - 1
        while pk_cnt <= pnum:
            param_key = list(params)[pk_cnt]
            param_value = new_dict.get('variables', {}).get('data', {}).get(key, {})\
                .get(param_key, '')
            val = {'key': param_key,
                   'value': '{}'.format(param_value)}
            new_dict['variables']['values'].append(val)
            pk_cnt = pk_cnt + 1

        if 'data' in new_dict['variables']:
            del new_dict['variables']['data']
        ky_cnt = ky_cnt + 1

    return new_dict


# Copy audit fields to root level
def transform_audit_fields(data_dict):
    if 'change_audit_stamps' in data_dict:
        if 'last_modified' in data_dict['change_audit_stamps']:
            if 'time' in data_dict['change_audit_stamps']['last_modified']:
                data_dict['last_modified_time'] = data_dict['change_audit_stamps']\
                    ['last_modified']['time']
        if 'created' in data_dict['change_audit_stamps']:
            if 'time' in data_dict['change_audit_stamps']['created']:
                data_dict['created_time'] = data_dict['change_audit_stamps']['created']['time']
    return data_dict


# Create ID field for each URN
def transform_urn(data_dict):
    data_dict_copy = data_dict.copy()
    for key, val in data_dict_copy.items():
        # Create ID fields from URNs
        if isinstance(val, str):
            search = re.search('^urn:li:(.*):(.*)$', val)
            if search and not key == 'value':
                id_type = convert(search.group(1).replace('sponsored', ''))
                if id_type == key:
                    new_key = '{}_id'.format(id_type)
                else:
                    new_key = '{}_{}_id'.format(key, id_type)
                if not id_type == 'unknown':
                    try:
                        id_val = int(search.group(2))
                    except ValueError:
                        id_val = search.group(2)
                    data_dict[new_key] = id_val
    return data_dict


def transform_data(data_dict, stream_name):
    if isinstance(data_dict, list):
        return [transform_data(x, stream_name) for x in data_dict]
    elif isinstance(data_dict, dict):
        # Transform dictionaries
        data_dict = transform_urn(data_dict)
        if stream_name[0:15] == 'ad_analytics_by_':
            data_dict = transform_analytics(data_dict)
        elif stream_name == 'campaigns':
            data_dict = transform_campaigns(data_dict)
        elif stream_name == 'creatives':
            data_dict = transform_creatives(data_dict)
        data_dict = transform_audit_fields(data_dict)

        # Transform unix epoch integers to datetimes
        for key, val in data_dict.items():
            if key[-4:] == 'time' or key[-3:] == '_at' or key == 'start' or key == 'end':
                if isinstance(val, int) and not isinstance(val, bool):
                    if val >= 1000000000000 and val <= 2000000000000: # valid unix epoch times
                        timestamp, msecs = divmod(val, 1000)
                        dttm = datetime.fromtimestamp(timestamp) + timedelta(milliseconds=msecs)
                        formatted_time = dttm.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                        data_dict[key] = formatted_time
        return {transform_data(key, stream_name): transform_data(val, stream_name)\
            for key, val in data_dict.items()}
    else:
        return data_dict


def transform_json(this_json, stream_name):
    LOGGER.info('Transforming stream: {}'.format(stream_name))
    # TESTING: LOGGER.info(str(this_json))
    transformed_json = transform_data(convert_json(this_json), stream_name)
    return transformed_json
