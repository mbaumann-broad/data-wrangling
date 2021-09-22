# Expand a Kids First file manifest to include fields from the DRS metadata to create
# an output file more suitable for test data purposes.

import csv
import json

import requests as requests


class ManifestExpander:
    def __init__(self):
        pass

    @staticmethod
    def get_drs_uri(did: str) -> str:
        return f"drs://dg.F82A1A:{did}"

    @staticmethod
    def get_drs_url(did: str) -> str:
        return f"https://data.kidsfirstdrc.org/ga4gh/drs/v1/objects/{did}"

    @staticmethod
    def get_md5_checksum(drs_metadata: dict) -> str:
        for checksum in drs_metadata['checksums']:
            if checksum['type'] == "md5":
                return checksum['checksum']
        raise Exception(f"md5 checksum not found in DRS metadata: {json.dumps(drs_metadata)}")

    def get_access_types(self, drs_metadata: dict) -> list:
        result: list = []
        for access_method in drs_metadata['access_methods']:
            result.append(access_method['type'])
        return result

    def get_entity_id_column_name(self, terra_data_table_name: str) -> str:
        return f"entity:{terra_data_table_name}_id"

    def expand_manifest_rows(self, rows: list) -> list:
        result_rows: list = []
        count = 1
        for row in rows:
            original_row = row.copy()
            try:
                did = row['Latest DID']
                drs_url = self.get_drs_url(did)
                resp = requests.get(drs_url)
                if resp.status_code != 200:
                    raise Exception(f"DRS metadata request failed: {resp.status_code} {resp.text}")
                drs_metadata = resp.json()
                # print(json.dumps(drs_metadata, indent=4))
                row['ga4gh_drs_url'] = drs_url
                row['ga4gh_drs_uri'] = self.get_drs_uri(did)
                row['md5'] = self.get_md5_checksum(drs_metadata)
                row['file_size'] = drs_metadata['size']
                row['acces_types'] = self.get_access_types(drs_metadata)
            except Exception as ex:
                print(f"Failed to process row: {json.dumps(original_row)}\n\t{ex}")
            else:
                result_rows.append(row)
            count += 1
            # if count == 100:
            #     break
        return result_rows

    def expand_for_terra_data_table_import(self, rows: list, terra_data_table_name: str) -> list:
        entity_id_column_name = self.get_entity_id_column_name(terra_data_table_name)
        row_number_width = len(str(len(rows)))
        count = 1
        for row in rows:
            row[entity_id_column_name] = str(count).zfill(row_number_width)
            count += 1
        return rows

    def read_manifest(self, input_manifest_filename: str) -> list:
        rows: list = []
        with open(input_manifest_filename, "r") as fh:
            tsv_reader = csv.DictReader(fh, delimiter="\t")
            for row in tsv_reader:
                rows.append(row)
        return rows

    def write_manifest(self, rows: list, terra_data_table_name: str, output_filename: str) -> None:
        entity_id_column_name = self.get_entity_id_column_name(terra_data_table_name)
        field_names = sorted(rows[0])
        field_names.remove(entity_id_column_name)
        field_names.insert(0, entity_id_column_name)
        with open(output_filename, "w") as fh:
            writer = csv.DictWriter(fh, fieldnames=field_names, delimiter="\t")
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    def expand_manifest(self, input_manifest_filename: str, output_manifest_filename: str,
                        terra_data_table_name: str) -> None:
        rows: list = self.read_manifest(input_manifest_filename)
        rows = self.expand_manifest_rows(rows)
        rows = self.expand_for_terra_data_table_import(rows, terra_data_table_name)
        self.write_manifest(rows, terra_data_table_name, output_manifest_filename)


#
# Main
#

input_filename = "/Users/mbaumann/Projects/Interop/DataAccess/KidsFirst/Production/kidsfirst-participant-family-manifest_2021-09-17_production_open_access_original.tsv"
output_filename = "/Users/mbaumann/Projects/Interop/DataAccess/KidsFirst/Production/kids_first_prod_public_drs_uris_20210917.tsv"
terra_data_table_name = "kids_first_prod_public_drs_uris"
manifest_exapnder = ManifestExpander()
manifest_exapnder.expand_manifest(input_filename, output_filename, terra_data_table_name)
