#!/usr/bin/python3

import pycountry
import pycountry_convert as pc
import plotly.express as px
import pandas as pd

continents = {'EU': 'Europe', 'NA': 'North America', 'SA': 'South America', 'AS': 'Asia', 'OC': 'Oceania', 'AF': 'Africa'}

cumulative_runs = pd.read_csv("input_data/cumulative_read_run.tsv", sep="\t")
cumulative_sequences = pd.read_csv("input_data/cumulative_sequence.tsv", sep="\t")

countries_info = {}
country_errors = {}
for country in pycountry.countries:
    try:
        continent_name = continents.get(pc.country_alpha2_to_continent_code(country.alpha_2), 'Unknown Code')
    except KeyError as err:
        country_errors[country.name] = err
    countries_info[country.name] = [country.alpha_3, country.numeric, continent_name]

cumulative_runs_countries = cumulative_runs.country.unique()
country_rundown = {}
row_num = 0
for country in cumulative_runs_countries:
    country_info = countries_info.get(country, "Unknown Country")
    country_alpha3 = country_info[0]
    country_isonum = country_info[1]
    country_continent = country_info[2]

    country_data = cumulative_runs[cumulative_runs["country"] == country]
    total_submissions = country_data["submissions"].sum()

    country_rundown[row_num] = [country, country_continent, country_alpha3, country_isonum, total_submissions]
    row_num += 1
# df = px.data.gapminder().query("year==2007")

country_rundown_info = pd.DataFrame.from_dict(country_rundown, orient='index', columns=['country', 'continent', 'iso_alpha', 'iso_num', 'submissions'])

fig = px.choropleth(country_rundown_info, locations="iso_alpha", color="submissions", hover_name="country", color_continuous_scale=px.colors.sequential.Redor)
fig.show()