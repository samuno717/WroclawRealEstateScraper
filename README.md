This script provides listing data from all listings that are not archived from the website https://wroclaw.nieruchomosci-online.pl/.
Later aforementioned data is put to an SQlite database for further use. 

In order for the process to be a bit more efficient, multithreading has been used. 
Nonetheless, the time to obtain all available listings in March 2025 (more than 11K) is somewhere in the range of 550 to 750 seconds. 
This depends on the server's responsiveness and possibly on the user's machine. 

Further experimentation and optimization is possible.

