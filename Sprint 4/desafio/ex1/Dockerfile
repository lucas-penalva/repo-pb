FROM python:3

WORKDIR /app

COPY carguru.py .

RUN pip install Flask

EXPOSE 3000

CMD [ "python", "carguru.py" ]