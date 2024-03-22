# Stock PE ratios
Fetches data from two sources
- 10-yr TIPS: https://fred.stlouisfed.org/series/DFII10
- S&P: https://www.spglobal.com/spdji/en/search/?query=index+earnings&activeTab=all


### DB Program
The DB program reads the Excel workbook downloaded from the S&P link.  The user must provide the path to the downloaded .xlsx.  The DB program also reads the 10-yr TIPS data, quarterly with end of quarter observations, from Fred.  The user enters the value of current day's TIPS rate for the current quarter into the downloaded workbook, then converts the .xls to an .xlsx.  For convenience, the user should place this workbook in the directory that contains the workbook from S&P.
&nbsp;

The S&P's downloaded .xlsx should be named: sp-500-eps-est yyyy mm dd.xlsx
The .xlsx downloaded from Fred should be named: DFII10.xlsx
&nbsp;

The program reads the S&P .xlsx only once, so downloads may be archived or destroyed after they are read.  Except for the projections of future earnings, the DB contains only the historical data for the S&P 500 that it fetches from the latest download.  The program always reads the DII10.xlsx file.  The DB contains only the historical data for the TIPS taken from the latest download.
&nbsp;

The user also must provide a location for the output, a pickle file containing a python dict which in turn contains the data frames used by the Graph program.


### Graph Program
The Graph program generates three pdf files. The user should specify both the path to the pickle file that contains the DB and the path for the program's output.
