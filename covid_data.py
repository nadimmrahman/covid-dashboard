#!/usr/bin/python3

import argparse, io, requests
import pandas as pd

# ======================================== #
# covid_data.py                            #
# Parent script to pull COVID-19 data from #
# the European Nucleotide Archive (ENA)    #
# ======================================== #

__author__ = "Nadim Rahman"

# base_dir = /Users/nadimrahman/Documents/workspace/development/ena_data_pull
ENA_PORTAL_API_URL = 'https://www.ebi.ac.uk/ena/portal/api/search'
searches = {'sequence': {'fields': 'study_accession, sample_accession, base_count, collection_date, country, description, host, isolate, location, strain', 'query': 'tax_tree(2697049)','result': 'sequence', 'dataPortal': 'ena'},
            'read_run': {'fields': 'study_accession, sample_accession, experiment_accession, instrument_platform, instrument_model, library_name, nominal_length, library_layout, library_strategy, library_source, library_selection, base_count, center_name, experiment_title, fastq_ftp, collection_date, country, description, isolate, location, strain', 'query': 'tax_tree(2697049)', 'result': 'read_run', 'dataPortal': 'ena'}}
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
    def __init__(self, search_query):
        self.search_query = search_query

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

    def save_result(self, df, filename):
        """
        Save a search result to file
        :param df: Dataframe of search results to save
        :param filename: Filename to save data frame with
        :return: Dataframe save command
        """
        filename = filename+'.tsv'
        return df.to_csv(filename, sep="\t", index=False)

    def retrieve_metadata(self):
        """
        Orchestrate the retrieval of metadata
        :return: Metadata content from ENA
        """
        results = list(self.search_query.keys())
        for result in results:
            offset = 0
            final_results = pd.DataFrame()
            while True:
                result_parameters = self.search_query.get(result)       # Obtain the specific result parameters for the search
                result_parameters["offset"] = offset        # Add an offset parameter

                results = self.req(result_parameters)
                final_results = pd.concat([final_results, results], axis=0, ignore_index=True)      # Handles cases if there are multiple requests for a particular result required

                ratio = len(results) / 100000       # Maximum number of search results obtained are 100,000
                if ratio < 1:
                    break       # No need to carry out a further search as there were less than 100,000 results
                else:
                    offset += len(results)      # Carry out another search to obtain all results
            self.save_result(final_results, result)



if __name__ == '__main__':
    args = get_args()
    print("[INFO] Directory specified: {}".format(args.directory))

    # Retrieve metadata from ENA
    metadata_obj = RetrieveENAMetadata(searches)
    metadata_obj.retrieve_metadata()