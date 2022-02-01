"""
Convert a list of AnVIL CRAM and associated CRAI files to a table of the format:
sample_id, cram, crai

Example Input:
gs://fc-secure-ffc72fe8-aa14-4a33-81a8-6bd0f269f834/102244/102244.hgv.cram
gs://fc-secure-ffc72fe8-aa14-4a33-81a8-6bd0f269f834/102244/102244.hgv.cram.crai
gs://fc-secure-ffc72fe8-aa14-4a33-81a8-6bd0f269f834/102246-A/102246-A.hgv.cram
gs://fc-secure-ffc72fe8-aa14-4a33-81a8-6bd0f269f834/102246-A/102246-A.hgv.cram.crai
...

Usage:
    > python3 TODO
"""

import argparse
from firecloud import api as fapi
import pandas as pd

class TransformFileList:

    def read_input_file(self, filename: str) -> list:
        pass

    def validate_input(self, filelist: list) -> None:
        pass

    def transform_to_table(self, filelist: list) -> pd.DataFrame:
        pass

    def output_to_tsv(self, table: pd.DataFrame, filename: str) -> None:
        pass


# Main

# TODO - Add Arg processing
input_filename =


transform

verify_output