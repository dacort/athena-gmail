FROM public.ecr.aws/lambda/python:3.8

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY athena_federator.py gathena.py credentials.json token.pickle ./

CMD [ "gathena.lambda_handler" ]