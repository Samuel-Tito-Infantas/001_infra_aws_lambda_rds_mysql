#!/bin/bash

main_parameter="$1"

if [ "$main_parameter" == "black" ]; then
    python -m black etl_index_igf/ lambda_function.py

elif [ "$main_parameter" == "flake8" ] || [ "$main_parameter" == "flake" ]; then
    python -m flake8 etl_index_igf/ lambda_function.py

elif [ "$main_parameter" == "install" ]; then
    pip install -r requirements.txt -t .

elif [ "$main_parameter" == "zip" ]; then
    echo "building zip envirotment for AWS Lambda"
    zip -r lambda_enviroment.zip etl_index_igf pymysql PyMySQL-1.1.0.dist-info lambda_function.py 

else
    echo "Invalid subparameter, please choose between this option ['black', 'flake8', 'zip']"
    
fi
