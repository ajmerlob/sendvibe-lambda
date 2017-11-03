rm lambda.zip
cd lambda-env/lib/python2.7/site-packages/
zip -r9 ../../../../lambda.zip *
cd ../../../../
zip -g lambda.zip handler.py
