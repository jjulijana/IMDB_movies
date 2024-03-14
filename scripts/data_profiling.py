from ydata_profiling import ProfileReport
import os

def generate_report_if_not_exists(data, report_file):
    if not os.path.exists(report_file):
        generate_profile_report(data, report_file)

def generate_profile_report(data, output_file):
    profile = ProfileReport(data)
    profile.to_file(output_file)