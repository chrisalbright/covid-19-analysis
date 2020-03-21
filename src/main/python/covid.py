#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import pandas as pd, os


daily_reports = "../../../COVID-19/csse_covid_19_data/csse_covid_19_daily_reports"
daily_reports = "../../../COVID-19/csse_covid_19_data/csse_covid_19_time_series"

os.chdir(daily_reports)


files = os.listdir(daily_reports)

print(files)

df = pd.read_csv('03-06-2020.csv', na_values = "")

df.head()