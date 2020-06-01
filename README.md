# python-scraper
This is a simple web scraper that uses the Python requests library to download data from Caltran PeMS (http://pems.dot.ca.gov/). PeMS has a limit on the amount of information that can be retrieved per query, so many queries are necessary to fetch enough information to facilitate analysis on longer timeframes.

There are two versions of the scraper in this repository: an asychronous one and a synchronous one. The asynchronous one is quite slow. The synchronous one works much faster, but be sure to set a reasonable number of threads so as to not overwhelm the PeMS server.
