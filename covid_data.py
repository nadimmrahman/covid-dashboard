#!/usr/bin/python3

import argparse, io, os, requests
import pandas as pd

# ======================================== #
# covid_data.py                            #
# Parent script to pull COVID-19 data from #
# the European Nucleotide Archive (ENA)    #
# ======================================== #

__author__ = "Nadim Rahman"

# base_dir = /Users/nadimrahman/Documents/workspace/development/ena_data_pull
ENA_PORTAL_API_URL = 'https://www.ebi.ac.uk/ena/portal/api/search'
searches = {'sequence': {'fields': 'study_accession, sample_accession, base_count, collection_date, country, description, host, isolate, strain', 'query': 'tax_tree(2697049)','result': 'sequence', 'dataPortal': 'ena'},
            'read_run': {'fields': 'study_accession, sample_accession, experiment_accession, instrument_platform, instrument_model, library_name, nominal_length, library_layout, library_strategy, library_source, library_selection, base_count, center_name, experiment_title, fastq_ftp, collection_date, country, description, isolate, strain', 'query': 'tax_tree(2697049)', 'result': 'read_run', 'dataPortal': 'ena'}}
headers = {'accept': '*/*'}


def get_args():
    """
    Get script arguments
    :return: Dictionary of script arguments.
    """
    parser = argparse.ArgumentParser(prog='covid_data.py', formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog="""
        + ========================================= +
        | COVID-19 ENA Data Scraper:                |
        | Python script to scrape data sets held at |
        | European Nucleotide Archive (ENA).        |
        + ========================================= +
        """)
    parser.add_argument("-d", "--directory", help="Full path to the working directory.", type=str, required=True)
    args = parser.parse_args()
    return args


class RetrieveENAMetadata:
    # Retrieve and obtain ENA metadata
    def __init__(self, search_query, test=False):
        self.search_query = search_query
        self.test = test

    def req(self, params):
        """
        Using requests, retrieve metadata from API search
        :param params: A dictionary of parameters to include in the requests
        :return: Requests object
        """
        try:
            r = requests.get(ENA_PORTAL_API_URL, headers=headers, params=params)
            r.raise_for_status()
        except HTTPError as http_err:
            print('[ERROR] HTTP error occurred during metadata retrieval: {}'.format(http_err))
        except Exception as err:
            print('[ERROR] Error in metadata retrieval: {}'.format(err))
        else:
            print('[INFO] Retrieved metadata successfully for {} search.'.format(params.get('result')))
            content = r.content.decode('UTF-8')
            df = pd.read_csv(io.StringIO(content), sep="\t")
        return df

    @staticmethod
    def save_result(df, filename):
        """
        Save a search result to file
        :param df: Dataframe of search results to save
        :param filename: Filename to save data frame with
        :return: Dataframe save command
        """
        filename = os.path.join('input_data', filename+'.tsv')
        return df.to_csv(filename, sep="\t", index=False)

    def retrieve_metadata(self):
        """
        Orchestrate the retrieval of metadata
        :return: Metadata content from ENA
        """
        if self.test:
            final_results = pd.read_csv(os.path.join('input_data', self.search_query.get('result')+".tsv"), sep="\t")
            return final_results

        offset = 0
        final_results = pd.DataFrame()
        while True:
            self.search_query["offset"] = offset        # Add an offset parameter

            results = self.req(self.search_query)
            final_results = pd.concat([final_results, results], axis=0, ignore_index=True)      # Handles cases if there are multiple requests for a particular result required

            ratio = len(results) / 100000       # Maximum number of search results obtained are 100,000
            if ratio < 1:
                break       # No need to carry out a further search as there were less than 100,000 results
            else:
                offset += len(results)      # Carry out another search to obtain all results
        self.save_result(final_results, result)
        return final_results


class readENAMetadata:
    # Retrieve and save data for plots
    def __init__(self, data_type):
        self.data_type = data_type

    def read_data(self):
        """
        Read data into memory
        :return: Data frame
        """
        df = pd.read_csv(os.path.join('input_data', self.data_type+".tsv", sep="\t")
        return df


class reshapeData:
    # Reshape the metadata obtained to create R plots
    def __init__(self, df, result):
        self.df = df
        self.result = result

    def separate_column(self, old_col, new_col, delimiter):
        """
        Separate a column in the data frame
        :param old_col: Column containing data to be split
        :param new_col: Column to be created from splitting old_col
        :return: Data frame with split columns
        """
        self.df[[old_col, new_col]] = self.df[old_col].str.split(delimiter, expand=True)

    def subset_data_frame(self, column, value):
        """
        Subset a data frame
        :return: Subset data frame
        """
        return self.df[column == value]

    def column_value_count(self, df, column):
        """
        Obtain counts for specific column in a dataframe
        :param df: Data frame to obtain counts for
        :param column: Column to count unique values
        :return:
        """
        return df[column].value_counts()

    def create_cumulative_by_country(self):
        """
        Create data frame for submission counts by country
        :return: Cumulative country data by time data frame
        """
        self.separate_column('country', 'country_locality', ':')        # Separate out the country column to country and country_locality
        countries = self.df.country.unique()        # List of unique countries that data have been submitted from

        country_cumulative_df = pd.DataFrame()
        for country in countries:
            country_subset = self.subset_data_frame(self.df.country, country)       # Subset the entire data frame to show data from the specific country
            country_submissions = self.column_value_count(country_subset, 'collection_date').to_frame().reset_index()       # Obtain counts of the subset data accounting for number of submissions for the country at specific dates
            country_submissions.columns = ['collection_date', 'submissions']        # Rename columns
            country_submissions['country'] = country        # Add a column with the country name
            country_cumulative_df = pd.concat([country_cumulative_df, country_submissions], axis=0, ignore_index=True)      # Add to the large data frame
        country_cumulative_df = country_cumulative_df.sort_values(by="collection_date", ascending=False).reset_index(drop=True)      # Sort the data frame by collection date and reset the index (drop=True needed to remove the existing indexes)
        RetrieveENAMetadata.save_result(country_cumulative_df, 'cumulative_'+self.result)
        return country_cumulative_df



if __name__ == '__main__':
    args = get_args()
    print("[INFO] Directory specified: {}".format(args.directory))

    results = list(searches.keys())
    for result in results:
        result_parameters = searches.get(result)  # Obtain the specific result parameters for the search

        ### Retrieve Metadata ###
        metadata_obj = RetrieveENAMetadata(result_parameters, test=False)
        result_metadata = metadata_obj.retrieve_metadata()

        ### Reshape the Metadata ###
        reshape_obj = reshapeData(result_metadata, result)
        reshaped_metadata = reshape_obj.create_cumulative_by_country()

    # ReadENAData is for when data is to be read into memory to produce plots
