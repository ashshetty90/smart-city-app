FROM python:3.8-slim
LABEL maintainer="Ashish Shetty ashshetty90@gmail.com>"
WORKDIR /usr/src/app
#ENV APP_DIR /main
#RUN mkdir ${APP_DIR}
#VOLUME ${APP_DIR}
#WORKDIR ${APP_DIR}
COPY requirements.txt ./
RUN pip3 install --upgrade pip && \
    pip3 install dask[dataframe] && \
    pip3 install -r requirements.txt
# copy config files into filesystem
COPY / .
CMD [ "python", "./main.py" ]