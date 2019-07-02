import re

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
        new_key = convert(key)
        if isinstance(this_json[key], dict):
            out[new_key] = convert_json(this_json[key])
        elif isinstance(this_json[key], list):
            out[new_key] = convert_array(this_json[key])
        else:
            out[new_key] = this_json[key]
    return out


# Remove all _links and _embedded nodes from json
def remove_embedded_links(this_json):
    if not isinstance(this_json, (dict, list)):
        return this_json
    if isinstance(this_json, list):
        return [remove_embedded_links(vv) for vv in this_json]
    return {kk: remove_embedded_links(vv) for kk, vv in this_json.items()
            if kk not in {'_embedded', '_links'}}


# Copy path/_embedded sub-nodes up to path
def denest_embedded_nodes(this_json, path=None):
    if path is None:
        return this_json
    i = 0
    nodes = ['attachments', "address", "chats", "emails", "phones", "social_profiles", "websites"]
    for record in this_json[path]:
        if "_embedded" in record:
            for node in nodes:
                if node in record['_embedded']:
                    this_json[path][i][node] = this_json[path][i]['_embedded'][node]
        i = i + 1
    return this_json


# Run all transforms: denests _embedded, removes _embedded/_links, and
#  converst camelCase to snake_case for fieldname keys.
def transform_json(this_json, path):
    transformed_json = convert_json(remove_embedded_links(\
                    denest_embedded_nodes(this_json, path)))
    return transformed_json
