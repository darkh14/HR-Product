from mongo_connection import MongoDBConnector
import uuid
import subprocess
import traceback
import sys
from base_parser import BaseParser


class ParsingTool:

    def __init__(self, **kwargs):

        job = kwargs.get('job')
        if job is not None:
            self.job = job
        else:
            self.job = True

        self.new_job_id = kwargs.get('new_job_id') or ''
        self.status = ''
        self.error = ''

        self.site_parsers = []
        self._set_parsers()
        self.site_list = kwargs.get('site_list') or []

        self.kwargs = kwargs
        self.db_connector = kwargs.get('db_connector') or MongoDBConnector()
        self.sub_process = bool(kwargs.get('sub_process'))

    def _set_parsers(self):

        self.site_parsers = []
        for SubClass in BaseParser.__subclasses__():
            if SubClass.enable:
                self.site_parsers.append(SubClass)

    def parse(self, **kwargs):

        parameters = kwargs.copy()
        parameter_keys = parameters.keys()
        if 'job' in parameter_keys:
            is_job = bool(parameters.get('job'))
        else:
            is_job = self.job

        if is_job:
            result = self.parse_with_job(parameters)
        else:
            result = self.parse_directly(parameters)

        if result != -1:
            self.status = 'OK'
        else:
            self.status = 'error'
            self.error = 'Site parser {} parsing error'

        return result

    def parse_with_job(self, parameters):

        job_name = 'refill_cv_collection'
        old_job_line = self.db_connector.read_job({'job': job_name, 'status': 'started'})

        parsing_par = parameters.copy()

        new_job_id = str(uuid.uuid4())

        parsing_par['sub_process'] = True
        parsing_par['new_job_id'] = new_job_id

        if not old_job_line:
            new_line = {'job_id': new_job_id, 'job': job_name, 'status': 'created', 'parameters': parsing_par,
                        'error': ''}

            self.db_connector.write_job(new_line, ['job_id', 'job'])

            if sys.platform == "linux" or sys.platform == "linux2":
                python_command = 'python3'
            else:
                python_command = 'python'

            p = subprocess.Popen([python_command, 'cv_parsing.py', '-job', 'refill_cv_collection', new_job_id])

            self.status = 'OK'

        else:
            self.status = 'error'
            self.error = 'The job {} is already started and not finished'.format(job_name)

        return new_job_id

    def parse_directly(self, parameters):

        sites = parameters.get('sites')

        count = parameters.get('count') or 0
        limit = parameters.get('limit') or 0

        for Parser in self.site_parsers:
            if sites and Parser.name not in sites:
                continue

            if limit and count >= limit:
                break

            site_parameters = parameters.copy()
            site_parameters['parser'] = Parser(**parameters)
            site_parameters['count'] = count

            count = self._parse_one_site(site_parameters)

        return count

    def _parse_one_site(self, parameters):

        count = parameters.get('count') or 0
        limit = parameters.get('limit') or 0

        vacancies = parameters.get('vacancies')

        if vacancies:
            for vacancy in vacancies:

                if limit and count >= limit:
                    break
                vacancy_parameters = parameters.copy()
                vacancy_parameters.pop('vacancies')
                vacancy_parameters['filter'] = vacancy.get('filter')
                vacancy_parameters['vacancy_id'] = vacancy.get('vacancy_id')
                vacancy_parameters['profile_id'] = vacancy.get('profile_id')
                vacancy_parameters['db'] = vacancy.get('db')
                vacancy_parameters['count'] = count

                count = self._parse_one_vacancy(vacancy_parameters)
        else:
            count = self._parse_one_vacancy(parameters)

        return count

    def _parse_one_vacancy(self, parameters):

        parsing_filter = parameters.get('filter')

        count = parameters.get('count') or 0
        limit = parameters.get('limit') or 0

        if parsing_filter and parsing_filter.get('texts'):

            for text in parsing_filter['texts']:

                if limit and count >= limit:
                    break

                current_filter = parsing_filter.copy()
                current_filter.pop('texts')
                current_filter['text'] = text

                current_parameters = parameters.copy()
                current_parameters['filter'] = current_filter
                current_parameters['count'] = count

                count = self._parse_one_text(current_parameters)
        else:
            count = self._parse_one_text(parameters)

        return count

    def _parse_one_text(self, parameters):

        site_parser = parameters.get('parser')

        if not site_parser:
            return False

        return site_parser.parse(parameters)


def refill_cv_collection(**kwargs):

    parsing_tool = ParsingTool(**kwargs)
    result = parsing_tool.parse(**kwargs)

    return result, parsing_tool.status, parsing_tool.error


def get_site_table_settings_from_parsers(**kwargs):

    site_list = []

    for SubClass in BaseParser.__subclasses__():
        if SubClass.enable:
            site_list.append({'site': SubClass.name, 'url': SubClass.url})

    return site_list, ''


if __name__ == '__main__':

    if len(sys.argv) == 4:
        if sys.argv[1] == '-job':

            job_id = str(uuid.UUID(sys.argv[3]))

            db_connector = MongoDBConnector()

            job_line = db_connector.read_job({'job': sys.argv[2], 'job_id': job_id})

            error_text = ''

            if job_line and job_line['status'] == 'created':

                job_line['status'] = 'started'
                db_connector.write_job(job_line, ['job_id', 'job'])

                parsing_parameters = job_line['parameters'].copy()

                error = False
                if parsing_parameters:
                    parsing_parameters['db_connector'] = db_connector
                else:
                    parsing_parameters = {'db_connector': db_connector}
                try:
                    parsing_parameters['job'] = False
                    parsing_parameters['sub_process'] = True

                    parser = ParsingTool(**parsing_parameters)
                    parser.parse(**parsing_parameters)
                except Exception as exc:
                    error = True
                    error_text = str(traceback.format_exc()) + '. ' + str(exc)

                job_line = db_connector.read_job({'job': sys.argv[2], 'job_id': job_id})

                job_line['status'] = 'error' if error else 'finished'
                job_line['error'] = error_text
                db_connector.write_job(job_line, ['job_id', 'job'])


