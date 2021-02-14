from mongo_connection import MongoDBConnector


class Filter:

    def __init__(self, **kwargs):
        self.db_connector = kwargs.get('db_connector') or MongoDBConnector()
        self._required_collections = ['filter_sites', 'filter_settings']
        self._required_fields = {'filter_sites': ['site'], 'filter_settings': ['name', '_1c', 'cv'],
                                 'filter_compliance': ['name', '_1c', 'cv']}
        self.error = ''

    def _check_collection(self, collection_name, collection=None):

        checked = False
        is_compliance = False
        self.error = ''

        if collection_name in self._required_collections:
            checked = True
        else:
            name_list = collection_name.split('_')
            if len(name_list) == 3 and name_list[0] == 'filter' and name_list[2] == 'compliance':
                filter_name = name_list[1]

                filter_settings_collection = self.get_filter_collection('filter_settings')

                for filter_settings_line in filter_settings_collection:
                    if filter_settings_line.get('name') == filter_name:
                        checked = True
                        is_compliance = True
                        break

        if not checked:
            self.error = 'Wrong collection name ""{}""'.format(collection_name)

        if checked and collection:

            if is_compliance:
                required_fields = self._required_fields.get('filter_compliance')
            else:
                required_fields = self._required_fields.get(collection_name)

            if collection_name != 'filter_sites':
                sites = self._get_sites()
                for site in sites:
                    required_fields.append(site)

            for coll_line in collection:
                keys = coll_line.keys()
                for key in keys:
                    if key not in required_fields:
                        checked = False
                        self.error = 'Wrong collection field ""{}""'.format(key)
                        break
                if not checked:
                    break

        return checked

    def _get_sites(self):
        sites = []
        sites_coll = self.get_filter_collection('filter_sites')
        if sites_coll:
            for site_line in sites_coll:
                sites.append(site_line.get('site'))

        return sites

    def _filter_collection_exists(self, collection_name):
        collection_names = self.db_connector.get_collection_names()
        return collection_name in collection_names

    def set_filter_collection(self, collection, collection_name):

        if not self._check_collection(collection_name, collection):
            return False

        self.db_connector.clear_collection(collection_name)

        for coll_line in collection:
            self.db_connector.write_line(collection_name, coll_line)

        return True

    def get_filter_collection(self, collection_name):

        if not self._check_collection(collection_name):
            return None

        result = self.db_connector.read_collection(collection_name)

        return result

    def get_filter_value(self, value, filter_name, site):

        result = value

        sites = self._get_sites()
        if site in sites:
            if self._filter_collection_exists('filter_' + filter_name + '_compliance'):
                compliance_collection = self.get_filter_collection('filter_' + filter_name + '_compliance')
                for comp_line in compliance_collection:
                    if comp_line.get('name') == value:
                        result = comp_line.get(site)
                        break

        else:
            self.error = ''
            result = None

        return result

    def get_filter_collection_names(self):
        collections = self._required_collections.copy()

        collection_names = self.db_connector.get_collection_names()
        for name in collection_names:
            name_list = name.split('_')
            if len(name_list) == 3 and name_list[0] == 'filter' and name_list[2] == 'compliance':
                collections.append(name)

        return collections

    def delete_filter_collection(self, collection_name):

        collection_names = self.get_filter_collection_names()

        if collection_name not in collection_names:
            self.error = 'Collection ""{}"" is not in existing collections'.format(collection_name)
            return False

        self.db_connector.clear_collection(collection_name)

        return True

def set_filter_collection(**kwargs):

    filter_controller = Filter(**kwargs)
    collection = kwargs.get('collection')

    if not collection:
        return None, 'Parameter ""collection"" is not find'

    collection_name = kwargs.get('collection_name')

    if not collection_name:
        return None, 'Parameter ""collection_name"" is not find'

    result = filter_controller.set_filter_collection(collection, collection_name)

    return result, filter_controller.error


def get_filter_collection(**kwargs):

    filter_controller = Filter(**kwargs)
    collection_name = kwargs.get('collection_name')

    if not collection_name:
        return None, 'Parameter ""collection_name"" is not find'

    collection = filter_controller.get_filter_collection(collection_name)

    return collection, filter_controller.error


def get_filter_collection_names(**kwargs):

    filter_controller = Filter(**kwargs)
    collection_names = filter_controller.get_filter_collection_names()

    return collection_names, filter_controller.error

def delete_filter_collection(**kwargs):

    filter_controller = Filter(**kwargs)
    collection_name = kwargs.get('collection_name')

    if not collection_name:
        return None, 'Parameter ""collection_name"" is not find'

    result = filter_controller.delete_filter_collection(collection_name)

    return result, filter_controller.error