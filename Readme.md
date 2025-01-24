# Housing Prices Dashboard

A project built with streamlit to display interactive historical housing price information and predict future housing price information. Built in front of MongoDB for New Jersey and MS SQL Server for Massachusetts. This project connects to MongoDB and MS SQL Server locally, so it must be deployed on TFS.

## Getting Started

### Prerequisites

* pip

* python 3

### Installation

#### Clone the Repo and CD into the directory

```shell
git clone git@bitbucket.org:tlcengine/housingpricesdashboard.git
```

#### Set up the virtual environment

##### Windows

```shell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

##### Linux

```shell
sudo apt update
sudo apt-get install python3-pip
sudo -H pip install pipenv
pipenv shell
pipenv install -r requirements.txt

python -m venv .venv
.venv\Scripts\Activate.ps1
```

#### Install pip packages

```shell
pip install -r requirements.txt
```

#### Set up .env

Should contain the following:

MONGODB_USERNAME
MONGODB_PASSWORD
MONGODB_URL
MLSPIN_LOGIN_URL  
MLSPIN_USERNAME
MLSPIN_PASSWORD
CTMLS_LOGIN_URL
CTMLS_USERNAME
CTMLS_PASSWORD
BRIDGE_ACCESS_TOKEN
MLSGRID_URL
MLSGRID_TOKEN
MLSMATRIX_LOGIN_URL
MLSMATRIX_USERNAME
MLSMATRIX_PASSWORD
PARAGON_LOGIN_URL
PARAGON_USERNAME
PARAGON_PASSWORD

#### Run

##### Windows

```shell
streamlit run app.py --server.enableCORS=false  --server.enableWebsocketCompression=false --server.enableXsrfProtection=false --browser.serverAddress=“0.0.0.0”
```

##### Linux
```shell
nohup streamlit run app.py &
```

Or simply run the included script

```shell
bash ./runApp.sh
```

## Deployment

Initial testing deployment lives on TFS at E:\housingPricesDashboard

Secondary testing deployment lives on geo2 at /DataDrive/krish/krish/housingPrices

Domain: streamlit.tlcengine.com
