## ReadMe
## Requirements
### Applications:
* Install python 3.8 or Higher
* Install python pip - https://www.liquidweb.com/kb/install-pip-windows/
* MySQL server
### Python packages:
* pip install mysql-connector-python
## Files Needed
* Download files available in this site: config.ini, schoolData.py
## Setup Configuration
* Input the correct user directory in config.ini file under [DEFAULT] -> directory
![user path](https://github.com/earlestradalopez/schoolData/blob/main/images/userdir.png)

* If you want data in the table not to be overwritten, you may place the folder name in the config.ini under [DEFAULT] -> update_date
![user path](https://github.com/earlestradalopez/schoolData/blob/main/images/updateTable.png)

* Configurations in your local DB
![user path](https://github.com/earlestradalopez/schoolData/blob/main/images/dbinfo.png)

## How to Run
`python schoolData.py`
