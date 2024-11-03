import argparse
import sys

def parse_arguments(*args):
    """Parse command-line arguments for the pipeline."""
    parser = argparse.ArgumentParser(description='Pipeline Data for NeoWs NASA.')
    
    parser.add_argument(
        '-c', '--create',
        action='store_true',
        help="Flag to create the database table."
    )
    
    parser.add_argument(
        '-r', '--read_file',
        type=str,
        help="Specify the file path to read asteroid data from."
    )
    
    parser.add_argument(
        '-e', '--extract',
        type=str,
        help='Specify the start and end extraction dates (\'Splitted with blank space\')'
    )
    
    parser.add_argument(
        '-pipe', '--pipeline',
        type=str,
        help='Specify the start and end extraction dates (\'Splitted with blank space\')'
    )
    
    return parser.parse_args()