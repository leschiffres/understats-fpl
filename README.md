# understats-fpl

This is a repository that scraps the [https://understat.com/league/EPL](https://understat.com/league/EPL) to get information about team and player data for the english premier league.

The code is set up in a way to instantiate two airflow dags.
- The first does the scraping part 
- The second transforms the tables into tables to extract meaningful insights.

Finally the notebook `visualizations.ipynb` access those transformed tables to show ideal players to be selected in the fantasy premier league game, based on the player form, the recent team performance and the schedule coming ahead.

## Setting up the environment

To setup the dependences (including airflow), one needs to go in the terminal to the folder and then run:

```bash
python -m venv .
source bin/activate
python -m pip install --upgrade pip

export AIRFLOW_HOME=`pwd`
export AIRFLOW_VERSION=2.7.0
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install -r requirements.txt
```

The airflow instance assumes that there is a postgres databased called `fantasy`. That it will use to store raw and transformed data.

Then to initiate the airflow instance from a new terminal run:

```
source bin/activate
export AIRFLOW_HOME=`pwd`
export $(grep -v '^#' .env | xargs)
source .airflow_env
airflow standalone
```

Please note that there needs to be a `.env` file with the database credentials as in the `env.example` file.
