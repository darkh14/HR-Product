import machine_learning as ml
import xml.etree.ElementTree as ET
import json

class НTTPProcessor:

    def __init__(self, environ, start_response):
        self.environ = environ
        self.start_response = start_response
        self.parameters = {}
        self.parameters_is_set = False
        self.output = []
        self.output_values = {}
        self.MongoConnector = None
        self.status = ''
        self.error = ''

    def set_parameters(self):

        if self.environ['REQUEST_METHOD'] == 'POST':
            # for line in self.environ['wsgi.input']:
            #     par_string = line.decode()
            #    print(par_string)

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

        if self.environ['REQUEST_METHOD'] == 'GET':
            pass
    #    if environ['QUERY_STRING'] != '':
    #        output.append('<h1>Get data:</h1>')
    #        for ch in d:
    #            output.append(' = '.join(ch))
    #            output.append('<br>')

    def process(self):
        if not self.parameters_is_set:
            self.set_parameters()

        if not self.parameters_is_set:
            self.status = 'error'
            self.error = 'Error of setting parameters'
        else:
            if not self.parameters.get('RequestType'):
                self.status = 'error'
                self.error = 'Parameter ''RequestType'' have not found'
            else:
                if self.parameters['RequestType'] == 'test':
                    for key in self.parameters:
                        self._add_parameter_to_output_(key, self.parameters[key] + ' test!')
                        self.status = 'OK'
                elif self.parameters['RequestType'] == 'get_fitting_cvs':
                    if not self.parameters.get('MongoConnectionString'):
                        self.status = 'error'
                        self.error = 'Parameter ''MongoConnectionString'' have not found'
                    else:
                        mongo_connection_string = self.parameters.get('MongoConnectionString')

                        ids, error = ml.find_fitting_ids(self.parameters, mongo_connection_string)

                        if error:
                            self.status = 'error'
                            self.error = error
                        else:
                            # for cv_id in ids:
                            #    self._add_parameter_to_output_('id_' + str(cv_id['_id']), cv_id)
                            self._add_parameter_to_output_('fitting_cvs', ids)
                            self.status = 'OK'
                        # else:
                        #     self.status = 'OK'
                        #     self.error = ''
                else:
                    self.status = 'error'
                    self.error = 'Unknown value of request type ''{}'''.format(self.parameters['RequestType'])

        self._add_parameter_to_output_('status', self.status)
        self._add_parameter_to_output_('error', self.error)

        output_str = json.dumps(self.output, ensure_ascii=False).encode()

        output_len = len(output_str)

        self.start_response('200 OK', [('Content-type', 'text/html'), ('Content-Length', str(output_len))])

        return [output_str]

    def _add_parameter_to_output_(self, key, value):

        cur_dict = {key: value}
        self.output.append(cur_dict)

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
        parameters_dict = dict()
        j_data = json.loads(xml_string)
        root = j_data.get('#value')
        if root:
            for element in root:
                name_dict = element.get('name')
                value_dict = element.get('Value')
                if name_dict and name_dict.get('#value') and value_dict:
                    parameters_dict[name_dict.get('#value')] = value_dict.get('#value')
        return parameters_dict

def process(environ, start_response):

    processor = НTTPProcessor(environ, start_response)
    output = processor.process()

    return output
