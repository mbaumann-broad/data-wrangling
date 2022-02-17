"""
Convert a list of AnVIL CRAM and associated CRAI files to a table of the format:
sample_id, cram, crai

Example Input:
gs://fc-secure-ffc72fe8-aa14-4a33-81a8-6bd0f269f834/102244/102244.hgv.cram
gs://fc-secure-ffc72fe8-aa14-4a33-81a8-6bd0f269f834/102244/102244.hgv.cram.crai
gs://fc-secure-ffc72fe8-aa14-4a33-81a8-6bd0f269f834/102246-A/102246-A.hgv.cram
gs://fc-secure-ffc72fe8-aa14-4a33-81a8-6bd0f269f834/102246-A/102246-A.hgv.cram.crai
...

"""
import csv
import json
from collections import defaultdict


class Transformer:
    valid_file_extensions = ['cram', 'crai']

    @staticmethod
    def _get_filename(file_uri: str) -> str:
        # Assume filename is the last element in a '/' separated path
        return file_uri.split('/')[-1]

    @staticmethod
    def _get_sample_id_from_filename(filename: str) -> str:
        # Assume sample id is the first element in a '.' separated filename
        return filename.split('.')[0]

    @staticmethod
    def _get_file_extension(path: str) -> str:
        # Assume extension is the element after the last '.' in the given string
        return path.split('.')[-1]

    @staticmethod
    def read_input_file(file_uri: str) -> list:
        with open(file_uri, 'r') as fh:
            file_uri_list = list(fh)
            trimmed_list = [line.strip() for line in file_uri_list]
            return trimmed_list

    def validate_input(self, file_uri_list: list) -> None:
        pass

    def _handle_duplicate(self, value1: str, value2: str, duplicate_fh):
        if value1 == value2:
            duplicate_fh.write(f"Duplicate detected with same value: {value1}\n")
        else:
            duplicate_fh.write(f"Duplicates detected with different values: {value1}, {value2}\n")

    def transform_to_table_map(self, file_uri_list: list, \
                               ignored_files_filename: str, \
                               duplicate_input_filename: str) -> dict:
        result = defaultdict(dict)
        with open(ignored_files_filename, 'w') as ignored_fh:
            with open(duplicate_input_filename, 'w') as duplicate_fh:
                for file_uri in file_uri_list:
                    filename = self._get_filename(file_uri)
                    extension = self._get_file_extension(filename)
                    sample_id = self._get_sample_id_from_filename(filename)
                    if extension in self.valid_file_extensions:
                        sample_dict = result[sample_id]
                        if extension in sample_dict:
                            self._handle_duplicate(sample_dict[extension], file_uri, duplicate_fh)
                        sample_dict[extension] = file_uri
                        sample_dict['sample_id'] = sample_id
                    else:
                        ignored_fh.write(f'Ignoring: {file_uri}\n')
        return result

    @staticmethod
    def output_to_tsv(table_map: dict, output_filename: str) -> None:
        with open(output_filename, 'w', newline='') as csvfile:
            fieldnames = ['sample_id', 'cram', 'crai']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, restval='NA', dialect=csv.excel_tab)
            writer.writeheader()
            sample_ids = sorted(table_map.keys())
            for sample_id in sample_ids:
                writer.writerow(table_map[sample_id])

    def _get_file_line_count(self, filename: str) -> int:
        with open(filename, "r") as fh:
            line_list = list(fh)
            return(len(line_list))

    def verify_output(self, file_uri_list: list, output_filename: str, \
                      ignored_input_filename: str, duplicate_input_filename: str) -> None:

        # Simple validation check of each row
        row_count = 0
        na_count = 0
        with open(output_filename, newline='') as fh:
            reader = csv.DictReader(fh, dialect=csv.excel_tab)
            for row in reader:
                row_count += 1
                cram = row['cram']
                crai = row['crai']
                if not (len(cram) > 0 and cram in crai):
                    print(f"Invalid row: {json.dumps(row)}")
                if cram == "NA":
                    na_count += 1
                if crai == "NA":
                    na_count += 1

        # Ensure all input values are accounted for
        # input file line count == ((output file row count * 2) - "NA" values) + invalid inpput line count
        input_line_count = len(file_uri_list)
        output_row_count = row_count
        ignored_line_count = self._get_file_line_count(duplicate_input_filename)
        duplicate_count = self._get_file_line_count(ignored_input_filename)
        print("(input_line_count == ((output_row_count * 2) - na_count) + ignored_line_count + duplicate_count))")
        print(f"({input_line_count} == (({output_row_count} * 2) - {na_count}) + {ignored_line_count} + {duplicate_count}))")
        print(f"({input_line_count} == ({(output_row_count * 2) - na_count}) + {ignored_line_count + duplicate_count}))")
        print(f"({input_line_count} == ({((output_row_count * 2) - na_count) + ignored_line_count + duplicate_count})")
        assert(input_line_count == ((output_row_count * 2) - na_count) + ignored_line_count + duplicate_count)


    def convert_file_list_to_table(self, input_filename: str, \
                                   output_filename: str, \
                                   ignored_input_filename: str, \
                                   duplicate_input_filename: str) -> None:
        file_uri_list = self.read_input_file(input_filename)
        self.validate_input(file_uri_list)
        table_map = self.transform_to_table_map(file_uri_list, ignored_input_filename, duplicate_input_filename)
        self.output_to_tsv(table_map, output_filename)
        self.verify_output(file_uri_list, output_filename, ignored_input_filename, duplicate_input_filename)

# TODO - Add Arg processing
input_filename = 'ccdg_wgs_crai.txt'
output_filename = 'ccdg_wgs_crai.tsv'
duplicate_input_filename = 'ccdg_wgs_crai_duplicates.txt'
ignored_input_filename = 'ccdg_wgs_crai_ignored.txt'

transformer = Transformer()
transformer.convert_file_list_to_table(input_filename, output_filename, ignored_input_filename, duplicate_input_filename)
print('Done')
print(f'Output file: {output_filename}')
print(f'Duplicate input file: {duplicate_input_filename}')
print(f'Ignored input file: {ignored_input_filename}')
