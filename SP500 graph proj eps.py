'''
First, run: SP500 index and earnings analysis.py
to create the input dir for this program

This program graphs the data for the S&P 500

file structure
|
|__ sp_data_dir/
|   |__ <sp-500 .xlsx data docs>
|
|__ Python_code_dir/
    |__ SP500 index and earnings analysis.py
    |__ <this program>
    |__ output_dir/
        |__<sp500_pe_data.pickle>
        |__ <this prog graphs>


sp500_pe_dict --
        {'history': 
                    {'date': date as string
                     'src_file' : name of data file
                     'actuals': dataframe, with op_margins, qtrly_data
                    }
        'projections':
                    {name_date: key is date of projection as dt
                                {'estimates': dataframe
                                 'price': float actual price on the date
                                }
                    }
        'input_xlsx_set': { } #set of files already read from data_dir
        }


data_df from sp500_pe_dict

    hist_cols_out = 'datetime', 'year_qtr', 'year', 
                    'month', '12m_op_eps', '12m_rep_eps',
                    'op_margin', 'real_int_rate', 'op_p/e',
                    'rep_p/e'
    data_cols_rename = {'op_margin': 'margin',
                        'real_int_rate': 'real_rate'}
    
    data_cols  = 'datetime', 'year_qtr', 'year', 
                 'month', '12m_op_eps', '12m_rep_eps',
                 'margin', 'real_rate', 'op_e/p', 'rep_e/p',
                 'op_p/e', 'rep_p/e'
           
    using: SP500 index and earnings analysis.py
    ['history']['actual'] DF
    ['projections'] Dict
    ['projections'][<date>]['estimates'] DF
    
'''

import os
import sys
import pickle
from datetime import datetime
from copy import deepcopy
from pprint import pprint

import pandas as pd
import numpy as np
np.random.seed(444)
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches


#=================  Global Parameters  ================================
# set paramaters
data_dir = "/Users/.../sp_data_dir"
code_dir = "/Users/.../Python_code_dir"

page0_suptitle = " \nPrice-Earnings Ratios for the S&P 500"
proj_eps_suptitle = " \nCalendar-Year Earnings per Share for the S&P 500"
page2_suptitle = " \nEarnings Margin and Equity Premium for the S&P 500"
page3_suptitle = \
    " \nEarnings Yield on S&P 500, 10-Year TIPS Rate, and Equity Premium"

proj_source = \
    'https://www.spglobal.com/spdji/en/search/?query=index+earnings&activeTab=all'
    
rr_source = '10-year TIPS: latest rate for each quarter from' + \
    '\nBoard of Governors of the Federal Reserve System (US), ' + \
    '\nMarket Yield on U.S. Treasury Securities at 10-Year Constant Maturity, ' + \
    '\nQuoted on an Investment Basis, Inflation-Indexed [DFII10], FRED, Federal Reserve Bank of St. Louis.'

 # set working directory to the code directory
os.chdir(code_dir)
output_dir = f"{code_dir}/output_dir"
input_file = 'sp500_pe_data.pickle'
output_file = 'TBD'

q_to_m_dict = {'Q1': (1, 2, 3),
               'Q2': (4, 5, 6),
               'Q3': (7, 8, 9),
               'Q4': (10, 11, 12)}

m_to_q_dict = dict(zip(q_to_m_dict.values(),
                       q_to_m_dict.keys()))

hist_cols_out = ['datetime', 'year_qtr', 'year', 
                 'month', '12m_op_eps', '12m_rep_eps',
                 'op_margin', 'real_int_rate', 'op_p/e',
                 'rep_p/e']

data_cols_rename  = {'op_margin': 'margin',
                    'real_int_rate': 'real_rate'}

data_cols  = ['datetime', 'year_qtr', 'year', 
              'month', '12m_op_eps', '12m_rep_eps',
              'margin', 'real_rate', 'op_e/p', 'rep_e/p',
              'op_p/e', 'rep_p/e']

#======================================================================


# ================  MAIN =============================================+
# +++++++++++++  read the data 
def main():

    with open(f"{output_dir}/{input_file}", "rb") as file:
        sp500_pe_data = pickle.load(file)
        
# create dict for projections dict, dt of projections are keys
    proj_dict = sp500_pe_data['projections']
    
# create time-line for time series from dates of projections (keys)
    kf = pd.DataFrame({'datetime': proj_dict.keys()})
    
    # create year_qtr from datetime, for classification
    y_m_lst = [(date.strftime("%Y"), date.strftime("%m"))
               for date in kf['datetime']]
    years_qtrs_lst = [f'{item[0]}-Q{(int(item[1]) - 1) // 3 + 1}'
                      for item in y_m_lst]
    kf['year_qtr']= years_qtrs_lst
    
    # collape kf to contain only the max value of dt
    # for each quarter
    hf = kf.groupby(['year_qtr'],
                     sort= False,
                     as_index= False) \
        .agg({'datetime': ['max']})
    # creates a multiIndex for hf -> simplify
    hf.columns = hf.columns.droplevel(1)

    hf.rename({'datetime': 'latest_proj_key'}, 
              axis=1,
              inplace=True)
    hf.sort_values(by= ['latest_proj_key'],
                   axis=0,
                   inplace= True,
                   ignore_index= True)
    # hf contains pairs that hold the dt key for
    # each quarter's latest projections
    # put these series in kf, which will merge into data_df below
    kf = deepcopy(hf)
    del(hf)

# create df historical data with renamed cols
    df = pd.DataFrame(sp500_pe_data['history']['actuals'] \
        [hist_cols_out])
    df['op_e/p'] = 1.0 / df['op_p/e'] * 100
    df['rep_e/p'] = 1.0 / df['rep_p/e'] * 100
    df['year'] = df['year'].astype('string')
   
    df.rename(data_cols_rename, axis=1, inplace=True)
    df = df[data_cols]

    del(sp500_pe_data)
    
# construct working history df
    # add two new series for actual cy earnings 
    # for each year for all qtrs for the year
    # (actual cy earnings appears in dec (month 12))
    row_filter = (df['month'] == 12)
    hf = df.loc[row_filter, ['year', '12m_op_eps', '12m_rep_eps']]
    hf.rename({'12m_op_eps': 'cy_op_eps',
               '12m_rep_eps': 'cy_rep_eps'}, 
               axis=1,
               inplace=True)
    df = pd.merge(df, hf,
                  how= 'left', 
                  on= ['year'])
    del(hf)
    
    # put actual cy earnings for each year in Q4, two new series
    # for convenience in plotting actual earnings in the graphs
    df.loc[row_filter,'actual_4Q_op_eps'] = \
        df.loc[row_filter, '12m_op_eps']
    df.loc[row_filter,'actual_4Q_rep_eps'] = \
        df.loc[row_filter, '12m_rep_eps']
    
# put the projection keys and data history into data_df
    data_df = pd.merge(kf, df,
                       how= 'left',
                       on=  ['year_qtr'])
    data_df['year'] = [item[:4]
                       for item in data_df['year_qtr']]
    del(df)
    del(kf)
    
    # display the latest quarter's latest_proj_key
    lngth = len(data_df) - 1
    print('\n=================================')
    print(f"latest projection: \n{data_df.at[lngth, 'latest_proj_key']}")
    print('=================================')
    # entry in first row, second col of data_df
    date_this_projn = data_df.at[lngth, 'latest_proj_key']
    
    # add qtr to data_df
    data_df['qtr'] = [np.int16(item[6:])
                      for item in data_df['year_qtr']]
    
# add "current" projections for each future year to data_df
#   enter the proj 4q eps for each future year into new cols, 
#       named for the future year
# for each proj key;
#       strip year from date for (12m) projections for future Q4s
#       enter the value of the projection for the date 
#       of the projection in data in the column named for the 
#       future (stripped) year
    
    data_df = add_eps_projctns(data_df, proj_dict)
    
    # current projection
    pf = proj_dict[date_this_projn]['estimates']
    pf = pf.loc[:, ['year_qtr', 'op_p/e', 
                    'rep_p/e', 'datetime']]
    pf.sort_values(by=['year_qtr'], inplace= True)
    pf.reset_index(drop=True, inplace=True)
    
    rate_of_growth_qtr = (1.05) ** 0.25
    pf['alt_op_p/e'] = [item * rate_of_growth_qtr ** idx
                        for idx, item in enumerate (pf['op_p/e'])]
    pf['alt_rep_p/e'] = [item * rate_of_growth_qtr ** idx
                        for idx, item in enumerate (pf['rep_p/e'])]

    del(proj_dict)
    
    # # just to be sure, sort into ascending time series by year_qtr
    data_df.sort_values(by =['year_qtr'],
                        inplace=True)
    data_df.reset_index(drop=True, inplace=True)
    
    # try to replace float64 with float32
    ch_names = list(data_df.select_dtypes(include= np.float64))
    # cannot change float64 cols that contain NaN which is float64
    data_df.loc[:, ch_names] = \
        data_df.loc[:, ch_names].astype(np.float32)
    
    del row_filter
    del ch_names
    del data_df['month']
    
    
# DISPLAY THE DATA ==================================================
    # https://matplotlib.org/stable/api/_as_gen/matplotlib.pyplot.subplot_mosaic.html
    # https://matplotlib.org/stable/api/axes_api.html
    # https://matplotlib.org/stable/api/axes_api.html#axes-position

# page zero  ======================
    # shows:  projected eps for current cy and future cy
    # the projections shown for each quarter are the latest
    # made in the quarter
    
# create graphs
    fig = plt.figure(figsize=(8.5, 11), 
                     layout="constrained")
    # one plot above the other
    ax = fig.subplot_mosaic([['operating'],
                             ['reported']])
    fig.suptitle(
        f'{proj_eps_suptitle}\n{date_this_projn}',
        fontsize=13,
        fontweight='bold')
    
    fig.supxlabel(proj_source, fontsize= 9)

# create the top and bottom graphs for op and rep eps
# label subsets of columns for op eps (top panel)
    cols_op_eps = [name
                   for name in data_df.columns
                   if name[9:] == 'op_eps']
    # create op_df to pass to plot
    slice = ['year_qtr', 'actual_4Q_op_eps',
             *cols_op_eps]
    df = deepcopy(data_df.loc[:, slice])
    # use year labels only for names of the data
    df_newNames = {name : name[:4]
                   for name in df.columns
                   if name[9:] == 'op_eps'}
    df_newNames['actual_4Q_op_eps'] = 'actual'
    df.rename(df_newNames, axis= 1,inplace= True)
    
    xlabl = '\ndate of projection\n'
    ylabl = '\nearnings per share\n'
    
    plots_page0(ax['operating'], df,
                title= ' \nProjections of Operating EPS',
                ylim= (100, None),
                xlabl= xlabl,
                ylabl= ylabl)
    
# label subsets of columns for rep eps (bottom panel)
    cols_rep_eps = [name
                    for name in data_df.columns
                    if name[9:] == 'rep_eps']
    
    # create rep_df to pass to plot
    slice = ['year_qtr', 'actual_4Q_rep_eps',
             *cols_rep_eps]
    df = deepcopy(data_df.loc[:, slice])
    df_newNames = {name : name[:4]
                   for name in df.columns
                   if name[9:] == 'rep_eps'}
    df_newNames['actual_4Q_rep_eps'] = 'actual'
    df.rename(df_newNames, axis= 1, inplace=True)
    
    plots_page0(ax['reported'], df,
                title= ' \nProjections of Reported EPS',
                ylim= (75, None),
                xlabl= xlabl,
                ylabl= ylabl)
    
# show the figure
    fig.savefig(f'{output_dir}/eps_page0.pdf')
    
    del df

# page one  ======================
    # shows:  historical data for margins and 
    # historical data with current estimate for equity premium
    
# create graphs
    fig = plt.figure(figsize=(8.5, 11), 
                     layout="constrained")
    # one plot above the other
    ax = fig.subplot_mosaic([['operating'],
                             ['reported']])
    fig.suptitle(
        f'{page0_suptitle}\n{date_this_projn}\n ',
        fontsize=13,
        fontweight='bold')
    
# create the top and bottom graphs for op and rep ratios
    hf = pd.merge(data_df.loc[:, ['year_qtr', 'op_p/e', 'rep_p/e']],
                  pf.loc[:, ['year_qtr', 'op_p/e', 'rep_p/e',
                             'alt_op_p/e', 'alt_rep_p/e']],
                  how= 'outer',
                  on= ['year_qtr'])
    denom = 'divided by projected earnings'
    legend1 = f'price constant from {date_this_projn}\n{denom}'
    legend2 = f'price increases 5% ar from {date_this_projn}\n{denom}'
    
# create working df for op ratio (top panel)
    df = deepcopy(hf.loc[:,['year_qtr', 'op_p/e_x', 
                            'op_p/e_y', 'alt_op_p/e']])
   
    df.rename({'op_p/e_x': 'historical',
               'op_p/e_y': legend1,
               'alt_op_p/e': legend2},
               axis=1,
               inplace=True)
    
    title = 'Ratio: Price to 12-month Trailing Operating Earnings'
   
    plots_page1(ax['operating'], df,
                    ylim= (None, None),
                    title= title,
                    ylabl= ' \n',
                    xlabl= ' \n')

# create working df for premia (bottom panel)
    df = deepcopy(hf.loc[:,['year_qtr','rep_p/e_x', 
                            'rep_p/e_y', 'alt_rep_p/e']])
    df.rename({'rep_p/e_x': 'historical',
               'rep_p/e_y': legend1,
               'alt_rep_p/e': legend2},
               axis=1,
               inplace=True)
    
    plots_page1(ax['reported'], df,
                    ylim= (None, None),
                    title= title,
                    ylabl= ' \n',
                    xlabl= ' \n')
    
    plt.savefig(f'{output_dir}/eps_page1.pdf', bbox_inches='tight')
    
    del(hf)
    del(df)

# page two  ======================
    # shows:  historical data for margins and 
    # historical data with current estimate for equity premium
    
# create graphs
    fig = plt.figure(figsize=(8.5, 11), 
                     layout="constrained")
    # one plot above the other
    ax = fig.subplot_mosaic([['margin'],
                             ['premium']])
    fig.suptitle(
        f'{page2_suptitle}\n{date_this_projn}\n',
        fontsize=13,
        fontweight='bold')
    
# create the top and bottom graphs for margins and premiums
# create working df for op margins (top panel)
    df = deepcopy(data_df.loc[:,['year_qtr', 'margin']])
    
    row_filter = pd.isna(df['margin']) != True
    df.loc[row_filter, 'margin'] = df.loc[row_filter, 'margin'] * 100
    
    plots_page2(ax['margin'], df,
                    ylim= (None, None),
                    title= 'Operating Earnings relative to Revenues',
                    ylabl= ' \npercent\n ',
                    xlabl= ' \n ')

# create working df for premia (bottom panel)
    df = deepcopy(data_df.loc[:,['year_qtr']])
    df['premium'] = data_df['rep_e/p'] - data_df['real_rate']
    # df['10-year TIPS rate'] = data_df['real_rate']
    
    title = 'Equity Premium: ratio of reported earnings to price, '
    title += 'less 10-year TIPS rate'

    plots_page2(ax['premium'], df,
                    ylim= (None, None),
                    title= title,
                    ylabl= ' \npercent\n ',
                    xlabl= ' \n ')
    
    plt.savefig(f'{output_dir}/eps_page2.pdf', bbox_inches='tight')
    
# page three  ======================
    # shows:  components of the equity premium
    
# create graphs
    fig = plt.figure(figsize=(8.5, 11), 
                     layout="constrained")
    # upper and lower plots
    ax = fig.subplot_mosaic([['operating'],
                             ['reported']])
    fig.suptitle(
        f'{page3_suptitle}\n{date_this_projn}\n',
        fontsize=13,
        fontweight='bold')
    
    fig.supxlabel(rr_source, fontsize= 9)
    
# create the top and bottom graphs for margins and premiums
# create working df for op margins (top panel)
    df = deepcopy(data_df.loc[:,['year_qtr']])
    df['earnings yield: earnings / price'] = data_df['op_e/p']
    df['10-year TIPS rate'] = data_df['real_rate']
    df['equity premium'] =  df['earnings yield: earnings / price'] - \
        df['10-year TIPS rate']
    
    title = 'Operating Earnings '

    plots_page3(ax['operating'], df,
                ylim= (None, 8),
                title= title,
                ylabl= ' \npercent\n ',
                xlabl= ' \n ')
    
    # create working df for rep margins (bottom panel)
    df['earnings yield: earnings / price'] = data_df['rep_e/p']
    df['equity premium'] =  df['earnings yield: earnings / price'] - \
        df['10-year TIPS rate']
    
    title = 'Reported Earnings '

    plots_page3(ax['reported'], df,
                ylim= (None, 8),
                title= title,
                ylabl= ' \npercent\n ',
                xlabl= ' \n ')
    
    plt.savefig(f'{output_dir}/eps_page3.pdf', bbox_inches='tight')
    
    sys.exit()


#=================  Helper Functions  =================================

def add_eps_projctns(df, p_dict):
    '''
    find qtrs in data_df that contain est eps in proj
    add these estimates for the current year and the next
    year to data_df
    
    data_df: 4 new [df_proj_cols]: op (curr, next), proj (curr, next)
    latest end-of-qtr proj for eps in current year and next
    appears in 12m-eps for this coming dec and the subsequent dec
    '''
    
    # put est eps into df
    # for each year_qtr, use the key to fetch the projections
    # est_eps_col_names in df
    #   ['est_curr_yr_op_eps', 'est_next_yr_op_eps',
    #    'est_curr_yr_rep_eps', 'est_next_yr_rep_eps']]
    # data from p_dict[key]['estimates'] for Dec each year
    
    for key in df['latest_proj_key']:
        
        pf = deepcopy(p_dict[key]['estimates'])
        # use only rows that contain future december dates
        #   dec 12m est are est eps for cal year
        pf_filter = ((pf['month'] == 12) & (pf['datetime'] >= key))
        # number of rows that meet the restrictions
        lngth = sum(pf_filter)
        
        if lngth > 0:
            # index of last entry is number of rows minus 1
            lngth -= 1
            pf = pf.loc[pf_filter, ['year_qtr', 
                                    '12m_op_eps', 
                                    '12m_rep_eps']]
            pf.reset_index(drop=True, inplace=True)
            # all eligible entries from pf enter only one row of df
            df_row = df['latest_proj_key'] == key
            while lngth >= 0:
                df_col_name = pf.at[lngth, 'year_qtr'][:4]
                df.loc[df_row, [f'{df_col_name}_est_op_eps']] =\
                    pf.at[lngth, '12m_op_eps']
                df.loc[df_row,[f'{df_col_name}_est_rep_eps']] =\
                    pf.at[lngth, '12m_rep_eps']
                lngth -= 1

    
    # reduce (spurious) precision
    float64_cols = list(df.select_dtypes(include='float64').columns)
    df.loc[:, float64_cols] = df.loc[:, float64_cols].astype('float32')
    
    return df
    
# =====================================================================


# ===============  PLOTTING FUNCTIONS  ================================
def plots_page0(ax, df,
                title= None, 
                ylim = (None, None), 
                xlabl= None,
                ylabl= None):
    '''
        A helper function to show many line and scatter plots
    '''
    
    # create the title and labels for the plot
    ax.set_title(title, fontweight= 'bold', loc= 'left')
    ax.set_xlabel(xlabl, fontweight= 'bold')
    ax.set_ylabel(ylabl, fontweight= 'bold')
    
    # prepare labels for the horizontal axis
    [yq, x_tick_labels] = yq_and_ticklabels(df)
    
    #color_ = ['gray', 'blue', 'red']
    #style_ = ['dashed', 'dotted', 'dotted']
    
    for name in list(df.columns)[2:]:
        # if name has only one data point
        if df[name].count() == 1:
            ax.scatter(yq, df[name], 
               marker= 'o', 
               s= 15,
               label= name)
        # if name has several data points
        else:
            ax.plot(yq, df[name], 
                # with enumerate:
                #   linestyle= style_[idx],
                #   color= color_[idx],
                label= name)
        
    ax.scatter(yq, df['actual'], 
               marker= 's', 
               s= 15,
               color= 'black',
               label= 'actual')
    
    ax.set_ylim(ylim)

    # the 1st arg gets locs for the labels
    # the 2nd arg specs the labels for these locs             
    ax.set_yticks(ax.get_yticks(), ax.get_yticklabels(), 
                  fontsize= 8)
    ax.set_xticks(ax.get_xticks(), x_tick_labels, 
                  rotation= 90, fontsize= 7)
    #ax.yaxis.set_ticks_position('both')
    
    ax0 = ax.twinx()
    ax0.set_ylim(ax.get_ylim())
    ax0.set_yticks(ax.get_yticks(), ax.get_yticklabels(),
                   fontsize= 8)
    ax0.set_ylabel(' ') #creates a space on the right side
    
    # https://matplotlib.org/stable/users/explain/axes/legend_guide.html
    ax.legend(title= 'for years:',
              title_fontsize= 9,
              fontsize= 8,
              loc= 'upper left')
    
    ax.hlines(y=200, color='lightgray',
              xmin= min(yq),
              xmax= max(yq),
              linestyle= 'dotted')
    
    return ax


def plots_page1(ax, df,
                ylim= (None, None),
                title = None,
                xlabl = None,
                ylabl = None):
    """
        Show one simple line plot
        the x axis labels are strings in the first col of df
        the data to be plotted are in the subsequent cols of df
    """
    
     # create the title and labels for the plot
    ax.set_title(title, fontweight= 'bold', loc= 'left')
    ax.set_xlabel(xlabl, fontweight= 'bold')
    ax.set_ylabel(ylabl, fontweight= 'bold')
    
    # prepare labels for the horizontal axis
    [yq, x_tick_labels] = yq_and_ticklabels(df)
    
    # fetch series name and plot it
    for name in list(df.columns)[1:]:
        if name == 'historical':
            ax.plot(yq, df[name], 
                    label= name)
        else: 
            ax.plot(yq, df[name],
                    label= name,
                    linestyle= 'dashed')
                
    # axis titles, tick labels, and legend
    ax.set_ylim(ylim)

    # the 1st arg gets locs for the labels
    # the 2nd arg specs the labels for these locs             
    ax.set_yticks(ax.get_yticks(), ax.get_yticklabels(), 
                  fontsize= 8)
    ax.set_xticks(ax.get_xticks(), x_tick_labels, 
                  rotation= 90, fontsize= 8)
    
    ax0 = ax.twinx()
    ax0.set_ylim(ax.get_ylim())
    ax0.set_yticks(ax.get_yticks(), ax.get_yticklabels(),
                   fontsize= 8)
    ax0.set_ylabel(' ') #creates a space on the right side

    ax.legend(#title= 'price to 12-month trailing earnings',
              #title_fontsize= 10,
              fontsize= 9,
              loc= 'upper right')
    
    ax.hlines(y=20, color='lightgray',
              xmin= min(yq),
              xmax= max(yq),
              linestyle= 'dotted')
    
    return ax


def plots_page2(ax, df,
                ylim= (None, None),
                title = None,
                xlabl = None,
                ylabl = None):
    """
        A helper function to show one simple line plot
    """
    
     # create the title and labels for the plot
    ax.set_title(title, fontweight= 'bold', loc= 'left')
    ax.set_xlabel(xlabl, fontweight= 'bold')
    ax.set_ylabel(ylabl, fontweight= 'bold')
    
    # prepare labels for the horizontal axis
    [yq, x_tick_labels] = yq_and_ticklabels(df)
    
    # series name and plot it
    name = list(df.columns)[-1:]
    ax.plot(yq, df[name])
                
    # axis titles, tick labels, and legend
    ax.set_ylim(ylim)

    # the 1st arg gets locs for the labels
    # the 2nd arg specs the labels for these locs             
    ax.set_yticks(ax.get_yticks(), ax.get_yticklabels(), 
                  fontsize= 8)
    ax.set_xticks(ax.get_xticks(), x_tick_labels, 
                  rotation= 90, fontsize= 8)
    
    ax0 = ax.twinx()
    ax0.set_ylim(ax.get_ylim())
    ax0.set_yticks(ax.get_yticks(), ax.get_yticklabels(),
                   fontsize= 8)
    ax0.set_ylabel(' ') #creates a space on the right side
    
    ax.hlines(y=4.0, color='lightgray',
              xmin= min(yq),
              xmax= max(yq),
              linestyle= 'dotted')
    ax.hlines(y=2.0, color='lightgray',
              xmin= min(yq),
              xmax= max(yq),
              linestyle= 'dotted')
    ax.hlines(y=10, color='lightgray',
              xmin= min(yq),
              xmax= max(yq),
              linestyle= 'dotted')
    
    return ax


def plots_page3(ax, df,
                ylim= (None, None),
                title = None,
                xlabl = None,
                ylabl = None):
    """
        A helper function to show one simple line plot
    """
    
     # create the title and labels for the plot
    ax.set_title(title, fontweight= 'bold', loc= 'left')
    ax.set_xlabel(xlabl, fontweight= 'bold')
    ax.set_ylabel(ylabl, fontweight= 'bold')
    
    # prepare labels for the horizontal axis
    [yq, x_tick_labels] = yq_and_ticklabels(df)
    
    # series name and plot it
    for name in list(df.columns)[1:]:
        if name == 'equity premium':
            ax.plot(yq, df[name],
                    label= name)
        else:
            ax.plot(yq, df[name],
                    label= name,
                    linestyle= 'dashed')
                
    # axis titles, tick labels, and legend
    ax.set_ylim(ylim)

    # the 1st arg gets locs for the labels
    # the 2nd arg specs the labels for these locs             
    ax.set_yticks(ax.get_yticks(), ax.get_yticklabels(), 
                  fontsize= 8)
    ax.set_xticks(ax.get_xticks(), x_tick_labels, 
                  rotation= 90, fontsize= 8)
    
    ax0 = ax.twinx()
    ax0.set_ylim(ax.get_ylim())
    ax0.set_yticks(ax.get_yticks(), ax.get_yticklabels(),
                   fontsize= 8)
    ax0.set_ylabel(' ') #creates a space on the right side
    
    ax.legend(#title= 'price to 12-month trailing earnings',
              #title_fontsize= 10,
              fontsize= 9,
              loc= 'upper right')
    
    ax.hlines(y=4.0, color='lightgray',
              xmin= min(yq),
              xmax= max(yq),
              linestyle= 'dotted')
    ax.hlines(y=2.0, color='lightgray',
              xmin= min(yq),
              xmax= max(yq),
              linestyle= 'dotted')
    
    return ax


def yq_and_ticklabels(df):
    yr_qtr = np.array(df['year_qtr'])
    x_tick_labels = \
        [item if item[-1:] == '1' else item[-2:]
         for item in yr_qtr]
    return [yr_qtr, x_tick_labels]

# =====================================================================


if __name__ == '__main__':
    main()

