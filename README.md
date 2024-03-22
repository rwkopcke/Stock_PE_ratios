# Stock PE ratios
Fetches data from two sources
- 10-yr TIPS: https://fred.stlouisfed.org/series/DFII10
- S&P: https://www.spglobal.com/spdji/en/search/?query=index+earnings&activeTab=all


### DB Program
The DB program reads the Excel workbook downloaded from the S&P link.  The user must provide the path to the downloaded .xlsx.  The DB program also reads the 10-yr TIPS data, quarterly with end of quarter observations, from Fred.  The user enters the value of current day's TIPS rate for the current quarter into the downloaded workbook, then converts the .xls to an .xlsx.  For convenience, the user should place this workbook in the directory that contains the workbook from S&P.
-
The S&P's downloaded .xlsx should be named: sp-500-eps-est yyyy mm dd.xlsx
The .xlsx downloaded from Fred should be named: DFII10.xlsx
-
The user also must provide a location for the output, a pickle file containing a python dict which in turn contains the data frames used by the Graph program.


### Graph Program
The Graph program generates three pages of pdfs. The user should specify the path for this output.
