
class SettingsController:
    def __init__(self, **kwargs):
        self.parameter_names = ['mongo_uri']
        for par in self.parameter_names:
            setattr(self, par, '')
        self.mongo_uri = 'mongodb://Admin:123@34.74.148.6:27017/Admin?authSource=admin'

        self.read_parameters_from_file()

    def read_parameters_from_file(self):
        try:
            with open('cfg.ini', 'r') as f:
                for line in f:
                    self._read_parameter_from_file_line(line)
        except FileNotFoundError:
            self.write_parameters_to_file()

    def write_parameters_to_file(self):
        with open('cfg.ini', 'w') as f:
            for parameter_name in self.parameter_names:
                self._write_line_to_file(f, parameter_name)

    def _write_line_to_file(self, file, parameter_name):
        line = parameter_name + ' = ' + str(getattr(self, parameter_name)) + '\n'
        file.write(line)

    def _read_parameter_from_file_line(self, line):
        eq_ind = line.find('=')
        if eq_ind != -1:
            parameter_name = line[0: eq_ind].strip()
            parameter_value = line[eq_ind + 1: -1].strip()

            setattr(self, parameter_name, parameter_value)

    def get_parameter(self, parameter_name):
        return getattr(self, parameter_name)

    def set_parameter(self, parameter_name, parameter_value, write_to_file=True):
        setattr(self, parameter_name, parameter_value)
        if write_to_file:
            self.write_parameters_to_file()

    def set_parameters(self, parameters: dict, write_to_file=True):
        for parameter_name, parameter_value in parameters.items():
            setattr(self, parameter_name, parameter_value)
        if write_to_file:
            self.write_parameters_to_file()


if __name__ == '__main__':
    pass
    # settings = SettingsController()
    # settings.set_parameter('parameter_1', 123)
    # settings.get_parameter('parameter_1')
