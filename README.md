The motivation behind the project was to process the huge amount of data being generated daily from the YouTube.
However only trending datasets is considered since it's highly significant for the company, creators and advertisers.
The processing is done on approximately 500MB of datasets across different countries.

**The code can be given all the 10 CSV files at once, however it may remove all duplicate videos, even if they're occuring in different files.**

*If you're new to MapReduce, try v1 of TopChannelCount in src folder, first. 

There are mainly six source files, and each have been done in different stages of development, some earlier source codes might not have been improvised.

•You can take each MR stage as a separate java file to make them as reusable components.

Languages/Tech used or further high-level info:
refer PDF(to be uploaded soon)

*Additionally, bash scripts can be used to compile the code, run the jobs and fetch the output from Hadoop FS faster.

Folder descriptions
___________________________________________________________________
src - containing 6 folders for each use case.
	Contains Java Source code.
___________________________________________________________________
input - contains csv and json files used as input to MR code.
	10 CSV and 10 JSON files for each region of CA,FR,GB,DE,IN,RU,KR,US,MX,JP.
	CSV file has 16 columns which has description of the video, while JSON file contains the category details for the specific region.
	These have been obtained from kaggle, and have been scraped from YouTube Trending page.
	Scraper Project: https://github.com/mitchelljy/Trending-YouTube-Scraper
___________________________________________________________________
output - contains the 6 output folders, each for one java file in src.
	subfolders: all - 10 files given as input, <region code> - region specific input only(IN - india).

______________________________________________________________________
extJARs - contains external JARs used int the project

_________________________________________________
Visualization - data visualised using PowerBI tool.

______________________________________________________________

(Internal) FolderName - Description

topStatsCategories - takes total stats by categories and sorts them according to the required stat.
topStatsChannels - takes total stats by channels and sorts them according to the required stat.
topStatsVideos - takes total stats by videos and sorts them according to the required stat.

The OP columns are in order of the csv file's columns, except the first column is the name of the folder in which file is present(e.g.: viewStats' first column is views, likeStats' is likes) 
.....................................................
TopCategory - TopCategory according to views
TopChannelCount - The most basic algo, doesnt take care of redundant views in v1, returns the occurences of a channel in the dataset.
		In v2, duplicate records have been dropped with latest instance of the video considered, and sorted according to the counts.
TopViewedChan - Channels with most views, redundant videos are dropped, with highest views considered.
_______________

Some sample use cases and their respective folder

• Print the videos/channels/categories that have the most counts for statistics specified by the
colum_stats plus their corresponding comment count, likes and dislikes -

 [Folders - topStats*]

• visualize the videos/channels/categories that have the most counts for statistics specified by
the colum_stats. 

-[Using matplotlib in Python/PowerBI visualization - (take OP from topStats*)]

•Find the top 5 most viewed channels?- 

 [op of TopViewedChan ]

• Find top 5channels with the most trending videos.

[op of TopTrendChan - (v1 -redundant and not sorted) (v2-sorted w/o redundancies)]

• Does a Relationship exist between views, likes, dislikes and comments?? Perform a correlation between them? - 

[Using matplotlib and correlations in Python (take op from topStatsVideos/viewStats and then apply correlation on each data column)]
____________________________________

****There might be bugs in the MR code. The columns presenting trouble are dropped automatically, if CSVReader is unable to process them(using Exception class -[make a user def exception for better results]).
***Could be improvised by analysing the code and implementing better preprocessing techniques.
**Don't forget to convert the DS files in UTF-8 by using "Save As" of LibreOffice Calc(preferred) or iconv command.
*Project implemented in Ubuntu 14.04 with Hadoop 2.6.0.

•String.split() can be used instead of CSVReader at many places, but CSVReader is more failsafe, in detecting stray embedded commas.
•Redundant(Duplicate) views/videos have been taken care of in this section by using an MR stage for preprocessing. This can also be said as a pre processing stage, where the unneccesary columns will be dropped off.
•Other bigdata techs like Hive can also be used for data visualization.

contributors:
JF
AP
