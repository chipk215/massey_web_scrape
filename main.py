import re
import requests
import pandas as pd
import os
from bs4 import BeautifulSoup
from io import StringIO
from pathlib import Path

# Dictionary of Web URLs by season
url_dict = {
    2018: "https://www.masseyratings.com/cb/arch/compare2018-18.htm",
    2017: "https://www.masseyratings.com/cb/arch/compare2017-18.htm",
    2016: "https://www.masseyratings.com/cb/arch/compare2016-18.htm",
    2015: "https://www.masseyratings.com/cb/arch/compare2015-18.htm",
    2014: "https://www.masseyratings.com/cb/arch/compare2014-19.htm",
    2013: "https://www.masseyratings.com/cb/arch/compare2013-19.htm",
    2012: "https://www.masseyratings.com/cb/arch/compare2012-18.htm",
    2011: "https://www.masseyratings.com/cb/arch/compare2011-18.htm",
    2010: "https://www.masseyratings.com/cb/arch/compare2010-18.htm",
    2009: "https://www.masseyratings.com/cb/arch/compare2009-18.htm",
    2008: "https://www.masseyratings.com/cb/arch/compare2008-18.htm",
    2007: "https://www.masseyratings.com/cb/arch/compare2007-18.htm",
    2006: "https://www.masseyratings.com/cb/arch/compare2006-18.htm",
    2005: "https://www.masseyratings.com/cb/arch/compare2005-18.htm",
    2004: "https://www.masseyratings.com/cb/arch/compare2004-17.htm",
    2003: "https://www.masseyratings.com/cb/arch/compare2003-14.htm"
}

# Text strings used to find the header column for the rankings data in the pre-formatted text
start_text_dict = {
    2018: 'WLK BWE',
    2017: 'BWE WLK',
    2016: 'BWE FAS',
    2015: 'STF DII',
    2014: 'STH STF',
    2013: 'KPK STH',
    2012: 'KPK BOB',
    2011: 'KPK SAG',
    2010: 'MAS SAG',
    2009: 'BOB GRN',
    2008: 'GRN BOB',
    2007: 'ROH SEL',
    2006: 'BOB GRN',
    2005: 'BOB ROH',
    2004: 'WLK MAS',
    2003: 'SAG BOB'
}

# Lists of unwanted fields, by season, to drop before saving data as a CSV file
drop_column_dict = {
    2018: ['Record', 'Rank_1', 'Team_1', 'Rank_2', 'Team_2', 'Rank_3', 'Team_3', 'BNT',
           'USA', 'AP', 'DES', 'Mean', 'Median', 'St.Dev'],

    2017: ['Record', 'Rank_1', 'Team_1', 'Rank_2', 'Team_2', 'Rank_3', 'Team_3', 'Rank_4', 'Team_4',
           'USA', 'AP', 'DES', 'Mean', 'Median', 'St.Dev'],

    2016: ['Record', 'Rank_1', 'Team_1', 'Rank_2', 'Team_2', 'Rank_3', 'Team_3', 'D1A',
           'USA', 'AP', 'DES', 'Mean', 'Median', 'St.Dev'],

    2015: ['Record', 'Rank_1', 'Team_1', 'Rank_2', 'Team_2', 'Rank_3', 'Team_3', 'D1A',
           'USA', 'AP', 'DES', 'Mean', 'Median', 'St.Dev'],

    2014: ['Record', 'Rank_1', 'Team_1', 'Rank_2', 'Team_2', 'Rank_3', 'Team_3', 'D1A',
           'USA', 'AP', 'DES', 'Mean', 'Median', 'St.Dev'],

    2013: ['Record', 'Rank_1', 'Team_1', 'Rank_2', 'Team_2', 'Rank_3', 'Team_3', 'D1A',
           'USA', 'AP', 'DES', 'Mean', 'Median', 'St.Dev'],

    2012: ['Record', 'Rank_1', 'Team_1', 'Rank_2', 'Team_2', 'Rank_3', 'Team_3',
           'USA', 'AP', 'DES', 'Mean', 'Median', 'St.Dev'],

    2011: ['Record', 'Rank_1', 'Team_1', 'Rank_2', 'Team_2',
           'USA', 'AP', 'DES', 'Mean', 'Median', 'St.Dev'],

    2010: ['Record', 'Rank_1', 'Team_1', 'Rank_2', 'Team_2', 'TRX',
           'USA', 'AP', 'DES', 'Mean', 'Median', 'St.Dev'],

    2009: ['Record', 'Rank_1', 'Team_1', 'Rank_2', 'Team_2', 'TRX',
           'USA', 'AP', 'DES', 'Mean', 'Median', 'St.Dev'],

    2008: ['Record', 'Rank_1', 'Team_1', 'Rank_2', 'Team_2', 'LYN', 'USA',
           'AP', 'DES', 'Mean', 'Median', 'St.Dev'],

    2007: ['Record', 'Rank_1', 'Team_1', 'Rank_2', 'Team_2', 'LYN', 'USA', 'AP', 'Mean', 'Median', 'St.Dev'],

    2006: ['Record', 'Rank_1', 'Team_1', 'TRX', 'LYN', 'USA', 'AP', 'Mean', 'Median', 'St.Dev'],

    2005: ['Record', 'Rank_1', 'Team_1', 'LYN', 'DES', 'USA', 'AP', 'Mean', 'Median', 'St.Dev'],

    2004: ['Record', 'Rank_1', 'Team_1', 'LYN', 'DES', 'USA', 'AP', 'Mean', 'Median', 'St.Dev'],

    2003: ['Record', 'Rank_1', 'Team_1', 'USA', 'AP', 'Mean', 'Median', 'St.Dev']
}


# Determine the field widths by matching tokens that start with one or more non white space
# characters and end with 0 or more white spaces.
def get_field_widths(line):
    match_expression = r'\S+\s*'
    tokens = re.findall(match_expression, line)
    widths = []
    for token in tokens:
        # print(token)
        widths.append(len(token))
    return widths


# Format the header column to have column delimited fields
def make_csv_line(line):
    # replace existing commas with an empty character and trim white space from line
    no_comma = line.lstrip(' ').replace(',', '')
    words = no_comma.split()
    stripped_words = []
    for word in words:
        stripped_words.append(word.strip())

    # add a comma to delimit word tokens
    return ','.join(stripped_words)


# The tabular data used duplicate column names for Rank and Team that need to be
# mangled in order to have unique field names
def rename_duplicate_column_names(header_row):
    column_names = header_row.split(',')
    rank_indices = [i for i, column in enumerate(column_names) if column == 'Rank']
    rank_indices = rank_indices[1:]

    for i in range(1, len(rank_indices) + 1):
        str_index = str(i)
        # Modify duplicate field names to have a suffix with a count indicating the occurrence
        #  E.g. Rank_1, Rank_2, etc.
        column_names[rank_indices[i - 1]] = 'Rank_' + str_index
        column_names[rank_indices[i - 1] + 1] = 'Team_' + str_index

    return column_names


def compute_win_percentage(team_record):
    components = team_record.split('-')
    return float(int(components[0]))/(int(components[0]) + int(components[1]))


def run_main():
    # demo code for a single season
    season = 2018
    source = requests.get(url_dict[season]).text
    soup = BeautifulSoup(source, 'lxml')
    pre = soup.find('pre')

    start_section_text = start_text_dict[season]
    end_section_text = '--------------------'

    # remove any embedded tags
    page_text = pre.text.strip()

    # determine the starting and ending positions of the tabular data
    start_position = page_text.find(start_section_text)
    end_position = page_text.find(end_section_text)

    # create a list of rows for all of the table data
    table_text = page_text[start_position:end_position]
    lines = table_text.splitlines()

    # Process the rows by removing blank lines and formatting the header row
    first_row_copied = False
    duke_found = False
    # list for holding the fixed field widths for the table data
    widths = []
    result = []
    for line in lines:
        # handle blank lines
        if not line:
            # blank line do not copy into result
            continue
        elif line.lstrip(' ').startswith(start_section_text):
            # the first row of the tabular data (repeats throughout the data for readability)
            if not first_row_copied:
                # Process the first occurrence of the header row

                # Add commas to delimit the column names
                result.append(make_csv_line(line))
                first_row_copied = True
            else:
                # ignore subsequent copies of the header row
                continue
        else:
            # Use Duke, a single work Team to compute fixed field widths
            if not duke_found:
                if line.find('Duke'):
                    # Compute the fixed field widths
                    widths = get_field_widths(line)
                    duke_found = True

            # not a blank line or a column header
            result.append(line[1:])

    # Rename all the occurrences of Rank and Team columns except for the first pair
    column_names = rename_duplicate_column_names(result[0])

    # Append a newline character to each row of data
    string_buffer = ''
    for line in result[1:]:
        string_buffer = string_buffer + line + "\n"

    df = pd.read_fwf(StringIO(string_buffer), widths=widths, names=column_names,
                     usecols=column_names[:len(widths)])

    # Add the season to the data frame along with the win percentage of the team
    df['season'] = season
    df['win_pct'] = df.apply(lambda row: compute_win_percentage(row.Record), axis=1)

    # drop unwanted columns
    df.drop(columns=drop_column_dict[season], inplace=True)

    f_name = 'data/rankings_' + str(season) + '.csv'
    csv_file = Path(f_name)
    if csv_file.is_file():
        os.remove(csv_file)

    df.to_csv(csv_file, sep=',', index=False)

    return


if __name__ == "__main__":
    run_main()
