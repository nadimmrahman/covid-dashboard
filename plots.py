#!/usr/bin/python3

import json, pycountry
import pycountry_convert as pc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np

# TO DO:
#   --> Move all data retrieval/storage code to covid_data.py to create files stored in input_data
#   --> Look into making a higher resolution map - some small countries missed, lines do not look clean.
#   --> Multi-line graph - separate by continents or only show top 10/15
#   --> Multi-line graph - make a similar cumulative count graph

# ====================== #
# Create Submissions Map #
# ====================== #
continents = {'EU': 'Europe', 'NA': 'North America', 'SA': 'South America', 'AS': 'Asia', 'OC': 'Oceania', 'AF': 'Africa'}

cumulative_runs = pd.read_csv("input_data/cumulative_read_run.tsv", sep="\t")
cumulative_sequences = pd.read_csv("input_data/cumulative_sequence.tsv", sep="\t")

# Had to hard code some changes in names of country in order to meet ISO standard which pycountry uses
cumulative_runs['country'] = np.where(cumulative_runs['country'] == "USA", "United States", cumulative_runs['country'])
cumulative_runs['country'] = np.where(cumulative_runs['country'] == "State of Palestine", "Palestine, State of", cumulative_runs['country'])
cumulative_runs['country'] = np.where(cumulative_runs['country'] == "South Korea", "Korea, Republic of", cumulative_runs['country'])
# Malta, Singapore don't show on the map

# From pycountry obtain the country mapping information --> TO BE MOVED INTO COVID_DATA AS THIS IS DATA PREPARATION
countries_info = {}
country_errors = {}
for country in pycountry.countries:
    try:
        continent_name = continents.get(pc.country_alpha2_to_continent_code(country.alpha_2), 'Unknown Code')
    except KeyError as err:
        country_errors[country.name] = err
    countries_info[country.name] = [country.alpha_3, country.numeric, continent_name]

# with open('input_data/country_mapping.txt', 'w') as country_file:
#     json.dump(countries_info, country_file, indent=4)

# Create an appropriate dataframe for the map to read, including ISO standard terms
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


# =========================================== #
# Create a Collection Date Country Line Graph #
# =========================================== #
# Shape the data in the format which is accepted by the multi-line scatter plot
submissions_total = pd.DataFrame(columns=['collection_date'])
for country in cumulative_runs_countries:
    submission_country = cumulative_runs[cumulative_runs["country"] == country]
    del submission_country['country']
    submission_country = submission_country.rename(columns={'submissions': country})
    submissions_total = pd.merge(submissions_total, submission_country, on="collection_date", how="outer")

submissions_total = submissions_total[~(submissions_total['collection_date'] < '2020-01-02')]
submissions_total = submissions_total.set_index('collection_date')

fig = go.Figure()

for column in submissions_total.columns.to_list():
    fig.add_trace(
        go.Scatter(
            x=submissions_total.index,
            y=submissions_total[column],
            name=column
        )
    )

button_all = dict(label = 'All',
                  method = 'update',
                  args = [{'visible': submissions_total.columns.isin(submissions_total.columns),
                           'title': 'All',
                           'showlegend': True}])

def country_dropdown(country):
    """

    :param country:
    :return:
    """
    return dict(label=country,
                method='update',
                args=[{'visible': submissions_total.columns.isin([country]),
                       'title': country,
                       'showlegend': True}])

addAll = True

fig.update_layout(
    updatemenus=[go.layout.Updatemenu(
        active=0,
        buttons=([button_all] * addAll) + list(submissions_total.columns.map(lambda country: country_dropdown(country)))
    )
    ],
    yaxis_type="log"
)
title = "Submissions per country on a daily basis"
# Update remaining layout properties
fig.update_layout(
    title_text=title,
    height=800

)

fig.show()