FROM python:3

WORKDIR /flask_api

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000
CMD ["flask", "run","--host","0.0.0.0","--port","5000"]