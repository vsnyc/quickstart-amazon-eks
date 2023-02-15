FROM lambci/lambda:build-python3.8

COPY ./index.py ./

RUN zip -X -r ./lambda.zip ./

CMD mkdir -p /output/ && mv ./lambda.zip /output/
