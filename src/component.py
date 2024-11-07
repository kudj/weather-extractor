"""
Template Component main class.

"""
import csv
import json
import logging
from dataclasses import dataclass
from datetime import datetime

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from keboola.component.dao import TableDefinition
from keboola.csvwriter import ElasticDictWriter

from configuration import Configuration
from weather.client import WeatherClient

@dataclass
class WriterCacheRecord:
    writer: ElasticDictWriter
    table_definition: TableDefinition

class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()
        self._writer_cache: dict[str, WriterCacheRecord] = dict()

    def run(self):

        params = Configuration(**self.configuration.parameters)

        client = WeatherClient(api_key=params.api_token)

        input_tables = self.get_input_tables_definitions()

        logging.debug("asdf")

        if len(input_tables) != 1:
            raise UserException("Exactly one input table is required")

        with open(input_tables[0].full_path, 'r') as in_table:
            reader = csv.DictReader(in_table)
            for row in reader:
                response = client.get_weather_forecast(location=row[params.location_column], units=params.units.value)

                self.write_to_csv(response, "out_table")

                # out_file = self.create_out_file_definition(f'forecast{row[params.location_column]}.json', tags=['forecast'])
                # with open(out_file.full_path, 'w') as json_file:
                #     json.dump(response, json_file, indent=4)

        logging.info("Extraction finished")

        for table, cache_record in self._writer_cache.items():
            cache_record.writer.writeheader()
            cache_record.writer.close()
            self.write_manifest(cache_record.table_definition)

        self.write_state_file({"last_run": datetime.now().isoformat()})

    def write_to_csv(self, data: list[dict],
                     table_name: str,
                     ) -> None:

        if not self._writer_cache.get(table_name):
            table_def = self.create_out_table_definition(f'{table_name}.csv', incremental=True,
                                                         primary_key=['location', 'time'])

            writer = ElasticDictWriter(table_def.full_path, ['location', 'time'])

            self._writer_cache[table_name] = WriterCacheRecord(writer, table_def)

        writer = self._writer_cache[table_name].writer
        for record in data:
            writer.writerow(record)

"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
