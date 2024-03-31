from ydata_profiling import ProfileReport
import pandas as pd
import os

def generate_report_if_not_exists(data: pd.DataFrame, report_file: str) -> None:
    if not os.path.exists(report_file):
        generate_profile_report(data, report_file)

def generate_profile_report(data: pd.DataFrame, output_file: str) -> None:
    profile = ProfileReport(data)
    profile.to_file(output_file)