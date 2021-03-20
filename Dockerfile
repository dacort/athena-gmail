FROM public.ecr.aws/lambda/python:3.8

ENV TARGET_BUCKET=replace_me

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY athena/ gathena.py credentials.json token.pickle ./

CMD [ "gathena.lambda_handler" ]