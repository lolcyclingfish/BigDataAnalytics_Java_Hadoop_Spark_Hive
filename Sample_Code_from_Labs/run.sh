 spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 4 \
    --executor-cores 4 \
    --packages com.databricks:spark-csv_2.10:1.5.0 \
    test.py
