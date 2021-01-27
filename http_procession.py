import machine_learning as ml
import cv_parsing
import xml.etree.ElementTree as ET
import json
import mongo_connection


class HTTPProcessor:

    def __init__(self, environ, start_response):
        self.environ = environ
        self.start_response = start_response
        self.parameters = {}
        self.parameters_is_set = False
        self.output = {}
        self.output_values = {}
        self.db_connector = None
        self.status = ''
        self.error = ''

    def set_parameters(self):

        if self.environ['REQUEST_METHOD'] == 'POST':

            content_length = int(self.environ.get('CONTENT_LENGTH')) if self.environ.get('CONTENT_LENGTH') else 0

            par_string = ''

            if content_length:
                par_string = self.environ['wsgi.input'].read(content_length)
            else:
                par_list = self.environ.get('wsgi.input')
                if par_list:
                    for par_element in par_list:
                        par_string = par_element

            if par_string:
                self.parameters = self.parameters_from_json(par_string)
                self.parameters_is_set = True

    def process(self):
        if not self.parameters_is_set:
            self.set_parameters()

        if not self.parameters_is_set:
            self.status = 'error'
            self.error = 'Error of setting parameters'
        else:
            if not self.parameters.get('request_type'):
                self.status = 'error'
                self.error = 'Parameter ''request_type'' have not found'
            else:
                if self.parameters['request_type'] == 'test':
                    for key in self.parameters:
                        self._add_parameter_to_output_(key, self.parameters[key] + ' test!')
                        self.status = 'OK'
                elif self.parameters['request_type'] == 'get_fitting_cvs':

                    ids, error = ml.find_fitting_ids(**self.parameters)

                    if error:
                        self.status = 'error'
                        self.error = error
                    else:
                        self._add_parameter_to_output_('fitting_cvs', ids)
                        self.status = 'OK'
                elif self.parameters['request_type'] == 'get_all_cvs':

                    ids, error = ml.get_all_ids(**self.parameters)

                    if error:
                        self.status = 'error'
                        self.error = error
                    else:
                        self._add_parameter_to_output_('all_cvs', ids)
                        self.status = 'OK'
                elif self.parameters['request_type'] == 'set_cv_vacancy_labels':

                    is_set, error = ml.set_cv_vacancy_labels(**self.parameters)

                    if not is_set:
                        self.status = 'error'
                        self.error = error
                    else:
                        self.status = 'OK'
                elif self.parameters['request_type'] == 'set_vacancies':
                    is_set, error = ml.set_vacancies(**self.parameters)  # , mongo_connection_string)

                    if not is_set:
                        self.status = 'error'
                        self.error = error
                    else:
                        self.status = 'OK'
                elif self.parameters['request_type'] == 'refill_cv_collection':

                    job_id, status, error = cv_parsing.refill_cv_collection(**self.parameters)

                    self.status = status
                    self.error = error
                    self._add_parameter_to_output_('job_id', job_id)

                elif self.parameters['request_type'] == 'check_job_status':

                    self.db_connector = mongo_connection.MongoDBConnector()
                    if self.parameters.get('job'):
                        id_filter = {'job': self.parameters.get('job')}
                        if self.parameters.get('filter'):
                            id_filter.update(self.parameters.get('filter'))

                        limit = self.parameters.get('limit')
                        job_lines = self.db_connector.read_jobs(id_filter, limit)

                        self._add_parameter_to_output_('jobs', job_lines)
                        self.status = 'OK'

                else:
                    self.status = 'error'
                    self.error = 'Unknown value of request type ''{}'''.format(self.parameters['request_type'])

        self._add_parameter_to_output_('status', self.status)
        self._add_parameter_to_output_('error', self.error)

        output_str = json.dumps(self.output, ensure_ascii=False).encode()

        output_len = len(output_str)

        self.start_response('200 OK', [('Content-type', 'text/html'), ('Content-Length', str(output_len))])

        return [output_str]

    def _add_parameter_to_output_(self, key, value):
        self.output[key] = value

    @staticmethod
    def parameters_from_xml(xml_string):
        parameters_dict = dict()
        root = ET.fromstring(xml_string)
        if root.tag.count('Structure') > 0:
            for child in root:
                if child.tag.count('Property') > 0:
                    for sub_child in child:
                        if sub_child.tag.count('Value') > 0:
                            parameters_dict[child.attrib['name']] = sub_child.text
        return parameters_dict

    @staticmethod
    def parameters_from_json(xml_string):

        return json.loads(xml_string)


def process(environ, start_response):

    processor = HTTPProcessor(environ, start_response)
    output = processor.process()

    return output