FROM python:3.11.9-bullseye
ADD requirements.txt /
RUN pip install -r requirements.txt
ADD main.py /
CMD [ "python", "./main.py" ]